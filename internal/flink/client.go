// Package flink reads the JobManager REST API of Flink 1.20 and 2.x, through port-forwards the pool manages.
package flink

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"
)

// Timeout bounds one request, so that a request through a dead forward fails instead of hanging.
const Timeout = 10 * time.Second

// Client calls one JobManager.
type Client struct {
	base string
	hc   *http.Client
}

// New returns a client for a JobManager REST base URL.
func New(base string) *Client {
	return &Client{
		base: strings.TrimSuffix(base, "/"),
		hc: &http.Client{
			Timeout: Timeout,
			// A redirect would point at another JobManager's pod address, which is unreachable from here.
			CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse },
		},
	}
}

// HTTPError is a non-2xx answer, with Flink's error messages.
type HTTPError struct {
	Path   string
	Status int
	Errors []string
}

func (e *HTTPError) Error() string {
	msg := http.StatusText(e.Status)
	if len(e.Errors) > 0 {
		msg = FirstLine(e.Errors[0])
	}
	return fmt.Sprintf("JobManager %s: %d %s", e.Path, e.Status, msg)
}

// IsNotFound reports whether err is a 404, such as for a job the JobManager no longer knows.
func IsNotFound(err error) bool {
	var he *HTTPError
	return errors.As(err, &he) && he.Status == http.StatusNotFound
}

// FirstLine is the first non-blank line of s, for one-line error and exception summaries.
func FirstLine(s string) string {
	line, _, _ := strings.Cut(strings.TrimSpace(s), "\n")
	return line
}

// GetRaw reads a REST path as the JobManager sends it.
func (c *Client) GetRaw(ctx context.Context, path string) ([]byte, error) {
	var raw json.RawMessage
	err := c.get(ctx, path, &raw)
	return raw, err
}

func (c *Client) get(ctx context.Context, path string, v any) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, c.base+path, http.NoBody)
	if err != nil {
		return err
	}
	resp, err := c.hc.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(io.LimitReader(resp.Body, 64<<20))
	if err != nil {
		return err
	}
	if resp.StatusCode/100 != 2 {
		he := &HTTPError{Path: path, Status: resp.StatusCode}
		var fe struct {
			Errors []string `json:"errors"`
		}
		if json.Unmarshal(body, &fe) == nil {
			he.Errors = fe.Errors
		}
		return he
	}
	if err := json.Unmarshal(body, v); err != nil {
		return fmt.Errorf("JobManager %s: %w", path, err)
	}
	return nil
}
