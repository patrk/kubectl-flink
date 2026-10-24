package redact

import (
	"strings"
	"testing"
)

func redactor(t *testing.T) *Redactor {
	t.Helper()
	r, err := NewRedactor(nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	return r
}

func TestSecretKey(t *testing.T) {
	r := redactor(t)
	for key, want := range map[string]bool{
		"s3.secret-key": true, "s3.access-key": true, "fs.azure.account.key.acct.blob.core.windows.net": true,
		"security.ssl.keystore-password": true, "properties.sasl.jaas.config": true, "AWS_SECRET_ACCESS_KEY": true,
		"DB_PASSWORD": true, "password": true, "api.token": true, "API_KEY": true, "github-token": true,
		// References and paths: names, not values.
		"secretName": false, "secretKeyRef": false, "kubernetes.secrets": false, "kubernetes.env.secretKeyRef": false,
		"security.ssl.keystore": false, "token.path": false, "password-file": false, "key": false, "name": false,
		"taskmanager.numberOfTaskSlots": false, "high-availability.storageDir": false, "s3.endpoint": false,
		"pass": true, "db.pass": true, "bypass": false, "passive": false,
	} {
		if got := r.SecretKey(key); got != want {
			t.Errorf("%s: %v, want %v", key, got, want)
		}
	}
}

func TestText(t *testing.T) {
	r := redactor(t)
	for in, want := range map[string]string{
		`password: hunter2`:      `password: [REDACTED]`,
		`s3.secret-key: AbC/123`: `s3.secret-key: [REDACTED]`,
		`fs.azure.account.key.acct.blob.core.windows.net: xyz==`:                                             `fs.azure.account.key.acct.blob.core.windows.net: [REDACTED]`,
		`s3.access-key=AKIAABCDEFGHIJKLMNOP`:                                                                 `s3.access-key=[REDACTED]`,
		`org.apache.kafka.common.security.plain.PlainLoginModule required username="app" password="s3cr3t";`: `org.apache.kafka.common.security.plain.PlainLoginModule required username="app" password="[REDACTED]";`,
		`connecting to jdbc:postgresql://db:5432/x?user=app&password=pw123&ssl=true`:                         `connecting to jdbc:postgresql://db:5432/x?user=app&password=[REDACTED]&ssl=true`,
		`fetch https://user:pa55@repo.example.com/x.jar failed`:                                              `fetch https://user:[REDACTED]@repo.example.com/x.jar failed`,
		`Authorization: Bearer abcdefghijklmnop.qrs`:                                                         `Authorization: [REDACTED]`,
		`header Bearer abcdefghijklmnop.qrs sent`:                                                            `header Bearer [REDACTED] sent`,
		`token eyJhbGciOiJIUzI1NiJ9.eyJzdWIiOiIxMjM0In0.abc expired`:                                         `token [REDACTED] expired`,
		`key id AKIAABCDEFGHIJKLMNOP denied`:                                                                 `key id [REDACTED] denied`,
		`2026-10-24 INFO request {"password":"hunter2","user":"x"}`:                                          `2026-10-24 INFO request {"password":"[REDACTED]","user":"x"}`,
		// Left alone.
		`taskmanager.numberOfTaskSlots: 1`:                                                `taskmanager.numberOfTaskSlots: 1`,
		`MountVolume.SetUp failed for volume "creds" : secret "does-not-exist" not found`: `MountVolume.SetUp failed for volume "creds" : secret "does-not-exist" not found`,
		`2026-10-24 18:29:22,915 ERROR Could not create application program.`:             `2026-10-24 18:29:22,915 ERROR Could not create application program.`,
		`http://repo.example.com:8081/x`:                                                  `http://repo.example.com:8081/x`,
	} {
		if got := r.Text(in); got != want {
			t.Errorf("%s\n got %s\nwant %s", in, got, want)
		}
	}
}

func TestArgs(t *testing.T) {
	r := redactor(t)
	got := r.Args([]string{"--hostname", "db", "--password", "pw1", "--db.token=tok2", "-Ds3.secret-key=sk3", "--verbose"})
	want := "--hostname db --password [REDACTED] --db.token=[REDACTED] -Ds3.secret-key=[REDACTED] --verbose"
	if strings.Join(got, " ") != want || r.Count() != 3 {
		t.Errorf("%q", got)
	}
}

func TestJSON(t *testing.T) {
	r := redactor(t)
	in := `{
	  "spec": {
	    "flinkConfiguration": {"s3.secret-key": "sk1", "taskmanager.numberOfTaskSlots": "2", "security": {"ssl": {"keystore-password": "kp2", "keystore": "/etc/ks.jks"}}},
	    "job": {"args": ["--password", "pw3"]},
	    "podTemplate": {"spec": {"containers": [{"name": "flink-main-container",
	      "env": [{"name": "DB_PASSWORD", "value": "pw4"}, {"name": "MODE", "value": "batch"},
	              {"name": "API_KEY", "valueFrom": {"secretKeyRef": {"name": "creds", "key": "api-key"}}}],
	      "args": ["--token=tk5"]}],
	      "volumes": [{"name": "creds", "secret": {"secretName": "creds"}}]}}
	  },
	  "status": {"reconciliationStatus": {"lastReconciledSpec": "{\"spec\":{\"flinkConfiguration\":{\"s3.secret-key\":\"sk6\"}}}"},
	             "error": "connect jdbc:mysql://db/x?password=pw7 failed"},
	  "rest": [{"key": "s3.access-key", "value": "ak8"}, {"key": "parallelism.default", "value": "1"}],
	  "count": 3, "token": 12345,
	  "tokens": ["tk9"], "credentials": {"user": "u", "pass": "pw10"}, "db": {"pass": "pw11"}
	}`
	out, err := r.JSON([]byte(in))
	if err != nil {
		t.Fatal(err)
	}
	s := string(out)
	for _, secret := range []string{"sk1", "kp2", "pw3", "pw4", "tk5", "sk6", "pw7", "ak8", "12345", "tk9", "pw10", "pw11"} {
		if strings.Contains(s, secret) {
			t.Errorf("%s leaked:\n%s", secret, s)
		}
	}
	for _, kept := range []string{`"secret": {`, `"taskmanager.numberOfTaskSlots": "2"`, `"/etc/ks.jks"`, `"value": "batch"`, `"secretName": "creds"`,
		`"key": "api-key"`, `"value": "1"`, `"count": 3`, `"s3.secret-key": "[REDACTED]"`} {
		if !strings.Contains(s, kept) {
			t.Errorf("%s lost:\n%s", kept, s)
		}
	}
	// tk9, and user and pass of the credentials object.
	if n := r.Count(); n != 13 {
		t.Errorf("counted %d, want 13", n)
	}
}

func TestExtraPatterns(t *testing.T) {
	r, err := NewRedactor([]string{`(?i)tenant[-_.]?id`}, []string{`(acct-)\d+()`})
	if err != nil {
		t.Fatal(err)
	}
	if got := r.Text("tenant.id: t-42\nbilled to acct-123456"); got != "tenant.id: [REDACTED]\nbilled to acct-[REDACTED]" {
		t.Errorf("%q", got)
	}
	if _, err := NewRedactor([]string{"("}, nil); err == nil {
		t.Error("want error for a bad pattern")
	}
}
