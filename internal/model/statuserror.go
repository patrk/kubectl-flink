package model

import (
	"encoding/json"
	"strings"
)

// StatusError is a parsed status.error. The operator writes JSON of FlinkResourceException, but also plain
// strings such as "Job Not Found", and e.toString() when serialisation fails.
type StatusError struct {
	Type     string  `json:"type,omitempty"`
	Message  string  `json:"message,omitempty"`
	Reason   string  `json:"reason,omitempty"`
	HTTPCode int     `json:"httpResponseCode,omitempty"`
	Causes   []Cause `json:"causes,omitempty"`
	// Raw is the original text when it was not JSON.
	Raw string `json:"raw,omitempty"`
}

// Cause is one entry of the exception's cause chain.
type Cause struct {
	Type    string `json:"type,omitempty"`
	Message string `json:"message,omitempty"`
}

// ParseStatusError parses status.error; it returns nil for an empty string.
func ParseStatusError(s string) *StatusError {
	s = strings.TrimSpace(s)
	if s == "" {
		return nil
	}
	var raw struct {
		Type               string `json:"type"`
		Message            string `json:"message"`
		AdditionalMetadata struct {
			Reason           string `json:"reason"`
			HTTPResponseCode int    `json:"httpResponseCode"`
		} `json:"additionalMetadata"`
		ThrowableList []Cause `json:"throwableList"`
	}
	if !strings.HasPrefix(s, "{") || json.Unmarshal([]byte(s), &raw) != nil || (raw.Type == "" && raw.Message == "") {
		return &StatusError{Message: s, Raw: s}
	}
	return &StatusError{
		Type:     raw.Type,
		Message:  raw.Message,
		Reason:   raw.AdditionalMetadata.Reason,
		HTTPCode: raw.AdditionalMetadata.HTTPResponseCode,
		Causes:   raw.ThrowableList,
	}
}

// Root is the innermost cause, which usually names the real problem.
func (e *StatusError) Root() Cause {
	if n := len(e.Causes); n > 0 {
		return e.Causes[n-1]
	}
	return Cause{Type: e.Type, Message: e.Message}
}

// Summary is a one-line description for tables: the reason if the operator classified the error, then the
// root cause's message, shortened.
func (e *StatusError) Summary(limit int) string {
	root := e.Root()
	msg := firstLine(root.Message)
	if msg == "" {
		msg = shortType(root.Type)
	}
	if e.Reason != "" && !strings.Contains(msg, e.Reason) {
		msg = e.Reason + ": " + msg
	}
	return truncate(msg, limit)
}

func shortType(t string) string {
	if i := strings.LastIndexByte(t, '.'); i >= 0 {
		return t[i+1:]
	}
	return t
}

func firstLine(s string) string {
	s = strings.TrimSpace(s)
	if i := strings.IndexByte(s, '\n'); i >= 0 {
		s = s[:i]
	}
	return strings.TrimSpace(s)
}

func truncate(s string, limit int) string {
	r := []rune(s)
	if limit <= 0 || len(r) <= limit {
		return s
	}
	if limit == 1 {
		return "…"
	}
	return string(r[:limit-1]) + "…"
}
