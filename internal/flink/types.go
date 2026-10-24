package flink

import (
	"context"
	"net/url"
	"strconv"
	"time"
)

// Millis is a REST timestamp in epoch milliseconds; 0 or -1 mean unset.
type Millis int64

// Time converts m; zero when unset.
func (m Millis) Time() time.Time {
	if m <= 0 {
		return time.Time{}
	}
	return time.UnixMilli(int64(m))
}

// Config is /config: the cluster's Flink version.
type Config struct {
	FlinkVersion  string `json:"flink-version"`
	FlinkRevision string `json:"flink-revision"`
}

// Config reads /config.
func (c *Client) Config(ctx context.Context) (*Config, error) {
	var v Config
	return &v, c.get(ctx, "/config", &v)
}

// JobOverview is one entry of /jobs/overview.
type JobOverview struct {
	JID   string `json:"jid"`
	Name  string `json:"name"`
	State string `json:"state"`
	Start Millis `json:"start-time"`
}

// Jobs reads /jobs/overview: every job the cluster knows, running or archived.
func (c *Client) Jobs(ctx context.Context) ([]JobOverview, error) {
	var v struct {
		Jobs []JobOverview `json:"jobs"`
	}
	return v.Jobs, c.get(ctx, "/jobs/overview", &v)
}

// Job is the subset of /jobs/:jid the tool reads.
type Job struct {
	JID        string            `json:"jid"`
	Name       string            `json:"name"`
	State      string            `json:"state"`
	Start      Millis            `json:"start-time"`
	Now        Millis            `json:"now"`
	Timestamps map[string]Millis `json:"timestamps"`
	Vertices   []Vertex          `json:"vertices"`
}

// Vertex is one operator chain of a job.
type Vertex struct {
	ID          string `json:"id"`
	Name        string `json:"name"`
	Parallelism int    `json:"parallelism"`
	Status      string `json:"status"`
}

// Job reads /jobs/:jid.
func (c *Client) Job(ctx context.Context, jid string) (*Job, error) {
	var v Job
	return &v, c.get(ctx, "/jobs/"+url.PathEscape(jid), &v)
}

// Checkpoints is /jobs/:jid/checkpoints, without per-task detail.
type Checkpoints struct {
	Counts struct {
		Restored   int `json:"restored"`
		Total      int `json:"total"`
		InProgress int `json:"in_progress"`
		Completed  int `json:"completed"`
		Failed     int `json:"failed"`
	} `json:"counts"`
	Latest struct {
		Completed *Checkpoint `json:"completed"`
		Savepoint *Checkpoint `json:"savepoint"`
		Failed    *Checkpoint `json:"failed"`
		Restored  *Restored   `json:"restored"`
	} `json:"latest"`
	History []Checkpoint `json:"history"`
}

// Checkpoint is one checkpoint or savepoint.
type Checkpoint struct {
	ID              int64  `json:"id"`
	Status          string `json:"status"` // IN_PROGRESS, COMPLETED or FAILED
	IsSavepoint     bool   `json:"is_savepoint"`
	Type            string `json:"checkpoint_type"`
	Triggered       Millis `json:"trigger_timestamp"`
	LatestAck       Millis `json:"latest_ack_timestamp"`
	Duration        int64  `json:"end_to_end_duration"`
	CheckpointedSz  int64  `json:"checkpointed_size"`
	StateSize       int64  `json:"state_size"`
	Subtasks        int    `json:"num_subtasks"`
	AckedSubtasks   int    `json:"num_acknowledged_subtasks"`
	ExternalPath    string `json:"external_path"`
	FailedAt        Millis `json:"failure_timestamp"`
	FailureMessage  string `json:"failure_message"`
	AlignmentBuffer int64  `json:"alignment_buffered"`
}

// Restored is the checkpoint the job last restored from.
type Restored struct {
	ID           int64  `json:"id"`
	RestoredAt   Millis `json:"restore_timestamp"`
	IsSavepoint  bool   `json:"is_savepoint"`
	ExternalPath string `json:"external_path"`
}

// Checkpoints reads /jobs/:jid/checkpoints.
func (c *Client) Checkpoints(ctx context.Context, jid string) (*Checkpoints, error) {
	var v Checkpoints
	return &v, c.get(ctx, "/jobs/"+url.PathEscape(jid)+"/checkpoints", &v)
}

// CheckpointConfig is /jobs/:jid/checkpoints/config: how often and how long checkpoints may take.
type CheckpointConfig struct {
	Mode              string `json:"mode"`
	Interval          int64  `json:"interval"` // ms; a very large value means periodic checkpoints are off
	Timeout           int64  `json:"timeout"`
	MinPause          int64  `json:"min_pause"`
	MaxConcurrent     int    `json:"max_concurrent"`
	TolerableFailures int    `json:"tolerable_failed_checkpoints"`
	Unaligned         bool   `json:"unaligned_checkpoints"`
	StateBackend      string `json:"state_backend"`
	CheckpointStorage string `json:"checkpoint_storage"`
	ChangelogEnabled  bool   `json:"state_changelog_enabled"`
	Externalized      struct {
		Enabled bool `json:"enabled"`
	} `json:"externalization"`
}

// CheckpointConfig reads /jobs/:jid/checkpoints/config; a job without checkpointing answers 404.
func (c *Client) CheckpointConfig(ctx context.Context, jid string) (*CheckpointConfig, error) {
	var v CheckpointConfig
	return &v, c.get(ctx, "/jobs/"+url.PathEscape(jid)+"/checkpoints/config", &v)
}

// Exceptions reads /jobs/:jid/exceptions with at most max history entries.
func (c *Client) Exceptions(ctx context.Context, jid string, maxEntries int) (*Exceptions, error) {
	var v Exceptions
	return &v, c.get(ctx, "/jobs/"+url.PathEscape(jid)+"/exceptions?maxExceptions="+strconv.Itoa(maxEntries), &v)
}
