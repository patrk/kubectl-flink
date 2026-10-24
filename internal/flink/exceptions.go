package flink

import (
	"cmp"
	"strings"
)

// Exceptions is /jobs/:jid/exceptions. Only exceptionHistory is read: Flink 2.x removed the top-level
// root-exception and all-exceptions fields.
type Exceptions struct {
	History struct {
		Entries   []Exception `json:"entries"`
		Truncated bool        `json:"truncated"`
	} `json:"exceptionHistory"`
}

// Exception is one failure in the history, with the failures that happened together with it.
type Exception struct {
	Name          string            `json:"exceptionName"`
	Stacktrace    string            `json:"stacktrace"`
	Timestamp     Millis            `json:"timestamp"`
	TaskName      string            `json:"taskName,omitempty"`
	Endpoint      string            `json:"endpoint,omitempty"`
	Location      string            `json:"location,omitempty"` // before endpoint replaced it
	TaskManagerID string            `json:"taskManagerId,omitempty"`
	FailureLabels map[string]string `json:"failureLabels,omitempty"`
	Concurrent    []Exception       `json:"concurrentExceptions,omitempty"`
}

// Message is the first line of the stack trace: the exception class and its message.
func (e *Exception) Message() string {
	line, _, _ := strings.Cut(strings.TrimSpace(e.Stacktrace), "\n")
	return cmp.Or(line, e.Name)
}

// Where is the TaskManager the failure happened on, by pod name when known.
func (e *Exception) Where() string { return cmp.Or(e.TaskManagerID, e.Endpoint, e.Location) }

// Cause is the deepest "Caused by" line of the stack trace, which usually names the real problem; empty when
// there is none.
func (e *Exception) Cause() string {
	i := strings.LastIndex(e.Stacktrace, "\nCaused by: ")
	if i < 0 {
		return ""
	}
	line, _, _ := strings.Cut(e.Stacktrace[i+len("\nCaused by: "):], "\n")
	return strings.TrimSpace(line)
}
