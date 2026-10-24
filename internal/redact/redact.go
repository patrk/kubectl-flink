// Package redact masks secrets in JSON, argument lists and text, for anything that leaves the cluster: the
// report bundle and the spec diffs of history.
package redact

import (
	"bytes"
	"encoding/json"
	"regexp"
	"strings"
)

// Mask replaces a redacted value; keys and structure stay, so the bundle still shows that the value was set.
const Mask = "[REDACTED]"

// Default patterns. A key is secret when its last parts name a password, token, key or credential; references
// to secrets (a Secret's name, a key selector, a file path) are not secret and stay readable, because which
// Secret is missing is often the evidence.
var (
	// #nosec G101 -- patterns that recognise credentials, not credentials
	defaultSecretKey = `(?i)(passw(?:or)?d|pwd|(?:^|[-_.])pass$|secret|token|credential|api[-_.]?key|access[-_.]?key|private[-_.]?key|account[-_.]key|jaas[-_.]config|authorization|bearer|session[-_.]?key)`
	defaultReference = `(?i)(secretname|secretkeyref|secretref|^secrets$|kubernetes\.(?:env\.)?secrets?$|[-_.](?:path|file|location|dir|type|class|provider|enabled)$|^key$|^name$)`
	defaultText      = []string{
		// JAAS: username="u" password="p"
		`(?i)(password\s*=\s*")[^"]*(")`,
		// key=value, key: value, --key=value, -Dkey=value in logs, args and messages
		`(?i)((?:passw(?:or)?d|pwd|secret|token|credential|api[-_.]?key|access[-_.]?key|secret[-_.]?key|private[-_.]?key)[\w.-]*"?\s*[:=]\s*"?)[^\s"',;&}]+()`,
		// user:password@ in URLs
		`([a-zA-Z][a-zA-Z0-9+.-]*://[^/\s:@]+:)[^@\s/]+(@)`,
		// Authorization headers and bearer tokens
		`(?i)(\b(?:bearer|basic)\s+)[A-Za-z0-9._~+/=-]{8,}()`,
		// JSON web tokens
		`()eyJ[A-Za-z0-9_-]{8,}\.[A-Za-z0-9_-]{8,}\.[A-Za-z0-9_-]*()`,
		// AWS access key ids
		`()\b(?:AKIA|ASIA)[0-9A-Z]{16}\b()`,
	}
)

// Redactor masks secrets in JSON, argument lists and text, and counts what it masked.
type Redactor struct {
	secretKey *regexp.Regexp
	reference *regexp.Regexp
	text      []*regexp.Regexp
	count     int
}

// NewRedactor builds a redactor from the default patterns plus extra key and text patterns; a text pattern
// masks its whole match unless it has two groups, which are kept around the masked part.
func NewRedactor(extraKeys, extraText []string) (*Redactor, error) {
	keys := defaultSecretKey
	for _, k := range extraKeys {
		if _, err := regexp.Compile(k); err != nil {
			return nil, err
		}
		keys += "|" + k
	}
	r := &Redactor{secretKey: regexp.MustCompile(keys), reference: regexp.MustCompile(defaultReference)}
	for _, t := range append(slicesClone(defaultText), extraText...) {
		re, err := regexp.Compile(t)
		if err != nil {
			return nil, err
		}
		r.text = append(r.text, re)
	}
	return r, nil
}

func slicesClone(s []string) []string { return append([]string(nil), s...) }

// Count returns how many values were masked since the last call, and resets it.
func (r *Redactor) Count() int {
	n := r.count
	r.count = 0
	return n
}

// SecretKey reports whether a key, such as a Flink option, env var or JSON field, names a secret value.
func (r *Redactor) SecretKey(key string) bool {
	return r.secretKey.MatchString(key) && !r.reference.MatchString(key)
}

func (r *Redactor) mask() string {
	r.count++
	return Mask
}

// Text masks secrets in free text: lines of key: value or key=value whose key is secret, and the text patterns.
func (r *Redactor) Text(s string) string {
	lines := strings.Split(s, "\n")
	for i, line := range lines {
		lines[i] = r.line(line)
	}
	return strings.Join(lines, "\n")
}

var keyValueLine = regexp.MustCompile(`^(\s*-?\s*"?([\w.$-]+)"?\s*[:=]\s*)(\S.*)$`)

func (r *Redactor) line(line string) string {
	if m := keyValueLine.FindStringSubmatch(line); len(m) == 4 && r.SecretKey(m[2]) && m[3] != Mask {
		return m[1] + r.mask()
	}
	for _, re := range r.text {
		line = re.ReplaceAllStringFunc(line, func(match string) string {
			sub := re.FindStringSubmatch(match)
			if len(sub) == 3 {
				return sub[1] + r.mask() + sub[2]
			}
			return r.mask()
		})
	}
	return line
}

// Args masks the value after a flag that names a secret (--password x) and the text patterns in each argument.
func (r *Redactor) Args(args []string) []string {
	out := make([]string, len(args))
	prev := ""
	for i, a := range args {
		flag := strings.TrimLeft(prev, "-")
		if strings.HasPrefix(prev, "-") && !strings.Contains(flag, "=") && r.SecretKey(flag) {
			out[i] = r.mask()
		} else {
			out[i] = r.line(a)
		}
		prev = a
	}
	return out
}

// JSON masks secrets in a JSON document and returns it indented. Values under secret keys are masked; objects of
// the {name|key, value} shape, such as env vars and Flink's REST configuration, are masked by their name; arrays
// of strings are treated as argument lists; strings that hold a JSON object, such as the operator's copy of the
// last reconciled spec, are redacted inside.
func (r *Redactor) JSON(b []byte) ([]byte, error) {
	d := json.NewDecoder(bytes.NewReader(b))
	d.UseNumber()
	var v any
	if err := d.Decode(&v); err != nil {
		return nil, err
	}
	v = r.value("", v)
	var out bytes.Buffer
	enc := json.NewEncoder(&out)
	enc.SetIndent("", "  ")
	enc.SetEscapeHTML(false)
	err := enc.Encode(v)
	return out.Bytes(), err
}

func (r *Redactor) value(key string, v any) any {
	// Everything under a secret key is secret, a list of tokens or an object of credentials alike, except the
	// references inside it: a volume's secret: {secretName: x} names the Secret it mounts.
	if key != "" && r.SecretKey(key) {
		switch v.(type) {
		case map[string]any, []any:
			return r.maskAll(v)
		}
	}
	switch t := v.(type) {
	case map[string]any:
		r.object(t)
		return t
	case []any:
		strs := make([]string, 0, len(t))
		for _, e := range t {
			if s, ok := e.(string); ok {
				strs = append(strs, s)
			}
		}
		if len(strs) == len(t) && len(t) > 0 {
			red := r.Args(strs)
			for i := range t {
				t[i] = red[i]
			}
			return t
		}
		for i := range t {
			t[i] = r.value(key, t[i])
		}
		return t
	case string:
		if key != "" && r.SecretKey(key) && t != "" {
			return r.mask()
		}
		if s := strings.TrimSpace(t); strings.HasPrefix(s, "{") && json.Valid([]byte(s)) {
			if red, err := r.JSON([]byte(s)); err == nil {
				return strings.TrimSpace(string(compact(red)))
			}
		}
		return r.Text(t)
	case json.Number, bool, nil:
		if key != "" && r.SecretKey(key) && t != nil {
			return r.mask()
		}
		return t
	}
	return v
}

// maskAll masks every value below v but those under reference keys.
func (r *Redactor) maskAll(v any) any {
	switch t := v.(type) {
	case map[string]any:
		for k, child := range t {
			if !r.reference.MatchString(k) {
				t[k] = r.maskAll(child)
			}
		}
		return t
	case []any:
		for i := range t {
			t[i] = r.maskAll(t[i])
		}
		return t
	case nil:
		return nil
	}
	return r.mask()
}

func compact(b []byte) []byte {
	var out bytes.Buffer
	if json.Compact(&out, b) != nil {
		return b
	}
	return out.Bytes()
}

func (r *Redactor) object(m map[string]any) {
	// {name: X, value: Y} and {key: X, value: Y}: the name says whether the value is secret.
	for _, nameKey := range []string{"name", "key"} {
		if name, ok := m[nameKey].(string); ok {
			if val, ok := m["value"].(string); ok && val != "" && r.SecretKey(name) {
				m["value"] = r.mask()
			}
		}
	}
	for k, v := range m {
		if r.reference.MatchString(k) {
			continue // a reference to a secret, such as secretKeyRef{name, key}: names, not values
		}
		if k == "value" {
			if s, ok := v.(string); ok && s == Mask {
				continue
			}
		}
		m[k] = r.value(k, v)
	}
}
