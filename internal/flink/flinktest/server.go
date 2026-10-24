// Package flinktest serves captured JobManager REST fixtures for tests.
package flinktest

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// Server serves dir/rest/*.json, as hack/capture-fixtures.sh saved them, by REST path, and returns the
// server and the captured job's ID.
func Server(t testing.TB, dir string) (srv *httptest.Server, jid string) {
	t.Helper()
	read := func(name string) []byte {
		b, err := os.ReadFile(filepath.Join(dir, "rest", name+".json")) // #nosec G304 -- test fixtures
		if err != nil {
			t.Fatal(err)
		}
		return b
	}
	var overview struct {
		Jobs []struct {
			JID string `json:"jid"`
		} `json:"jobs"`
	}
	if err := json.Unmarshal(read("jobs-overview"), &overview); err != nil || len(overview.Jobs) == 0 {
		t.Fatalf("%s: no job in jobs-overview: %v", dir, err)
	}
	jid = overview.Jobs[0].JID
	routes := map[string]string{
		"/config": "config", "/jobs/overview": "jobs-overview", "/jobs/" + jid: "job",
		"/jobs/" + jid + "/exceptions": "exceptions", "/jobs/" + jid + "/checkpoints": "checkpoints",
		"/jobs/" + jid + "/checkpoints/config": "checkpoints-config", "/jobs/" + jid + "/config": "job-config",
		"/jobmanager/config": "jobmanager-config", "/taskmanagers": "taskmanagers",
	}
	// Vertex metrics: the names list without get, the captured aggregates with it.
	files, _ := filepath.Glob(filepath.Join(dir, "rest", "metric-names-*.json"))
	for _, f := range files {
		vid := strings.TrimSuffix(strings.TrimPrefix(filepath.Base(f), "metric-names-"), ".json")
		routes["/jobs/"+jid+"/vertices/"+vid+"/subtasks/metrics"] = "metric-names-" + vid
		routes["/jobs/"+jid+"/vertices/"+vid+"/subtasks/metrics?get"] = "metrics-" + vid
	}
	srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		key := r.URL.Path
		if r.URL.Query().Has("get") {
			key += "?get"
		}
		name, ok := routes[key]
		if !ok {
			w.WriteHeader(http.StatusNotFound)
			_ = json.NewEncoder(w).Encode(map[string][]string{"errors": {"Not found: " + r.URL.Path}})
			return
		}
		_, _ = w.Write(read(name))
	}))
	t.Cleanup(srv.Close)
	return srv, jid
}
