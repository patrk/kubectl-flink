package kube

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"sync"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
)

// Event is the subset of a core/v1 Event the tool reads.
type Event struct {
	metav1.TypeMeta `json:",inline"`
	ObjectMeta      `json:"metadata"`
	InvolvedObject  ObjectReference  `json:"involvedObject"`
	Reason          string           `json:"reason,omitempty"`
	Message         string           `json:"message,omitempty"`
	Type            string           `json:"type,omitempty"` // Normal or Warning
	Count           int32            `json:"count,omitempty"`
	FirstTimestamp  metav1.Time      `json:"firstTimestamp"`
	LastTimestamp   metav1.Time      `json:"lastTimestamp"`
	EventTime       metav1.MicroTime `json:"eventTime"`
	Series          *EventSeries     `json:"series,omitempty"`
}

// ObjectReference names the object an event is about.
type ObjectReference struct {
	Kind string    `json:"kind,omitempty"`
	Name string    `json:"name,omitempty"`
	UID  types.UID `json:"uid,omitempty"`
}

// EventSeries is how newer clients report repeats.
type EventSeries struct {
	Count            int32            `json:"count,omitempty"`
	LastObservedTime metav1.MicroTime `json:"lastObservedTime"`
}

// Last is when the event last happened, whichever of the three time fields its source filled.
func (e *Event) Last() time.Time {
	t := e.LastTimestamp.Time
	if e.EventTime.After(t) {
		t = e.EventTime.Time
	}
	if e.Series != nil && e.Series.LastObservedTime.After(t) {
		t = e.Series.LastObservedTime.Time
	}
	return t
}

// Times is how often the event happened.
func (e *Event) Times() int32 {
	if e.Series != nil && e.Series.Count > 0 {
		return e.Series.Count
	}
	return max(e.Count, 1)
}

// EventList is a list of Events.
type EventList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata"`
	Items           []Event `json:"items"`
}

func (l *EventList) appendPage(p runtime.Object) { l.Items = append(l.Items, p.(*EventList).Items...) }

// DeepCopyObject implements runtime.Object.
func (e *Event) DeepCopyObject() runtime.Object {
	out := *e
	e.DeepCopyInto(&out.ObjectMeta)
	if e.Series != nil {
		s := *e.Series
		out.Series = &s
	}
	return &out
}

// DeepCopyObject implements runtime.Object.
func (l *EventList) DeepCopyObject() runtime.Object {
	out := &EventList{TypeMeta: l.TypeMeta}
	l.DeepCopyInto(&out.ListMeta)
	out.Items = copyItems(l.Items, func(in, o *Event) { *o = *in.DeepCopyObject().(*Event) })
	return out
}

// JobExceptionSelector selects the events in which the operator reports a job's failures, one per distinct
// exception, merged by count when it repeats. JobException is the same test on an event already read.
const JobExceptionSelector = "reason=" + jobExceptionReason + ",type=Warning"

const jobExceptionReason = "JobException"

// JobException reports whether the event is one in which the operator reports a failure of the job.
func (e *Event) JobException() bool { return e.Reason == jobExceptionReason && e.Type == "Warning" }

// Events are the core/v1 events.
var Events = Resource{
	GVR:     CoreGroupVersion.WithResource("events"),
	newObj:  func() runtime.Object { return &Event{} },
	newList: func() runtime.Object { return &EventList{} },
	core:    true,
}

// parallelEvents bounds the event requests of one job, which has one per pod.
const parallelEvents = 8

// EventsFor returns the events about the objects with the given uids, newest first. Events are selected by
// uid, not name: events of an earlier object with the same name linger for an hour after it is deleted. The
// events that arrived are returned with the errors of the requests that failed.
func (c *Client) EventsFor(ctx context.Context, namespace string, uids []types.UID) ([]Event, error) {
	parts := make([][]Event, len(uids))
	errs := make([]error, len(uids))
	var wg sync.WaitGroup
	sem := make(chan struct{}, parallelEvents)
	for i, uid := range uids {
		wg.Go(func() {
			sem <- struct{}{}
			defer func() { <-sem }()
			obj, err := c.list(ctx, Events, namespace, metav1.ListOptions{FieldSelector: "involvedObject.uid=" + string(uid)})
			if err != nil {
				errs[i] = fmt.Errorf("events of %s: %w", uid, err)
				return
			}
			parts[i] = obj.(*EventList).Items
		})
	}
	wg.Wait()
	var out []Event
	for i := range uids {
		out = append(out, parts[i]...)
	}
	slices.SortStableFunc(out, func(a, b Event) int { return b.Last().Compare(a.Last()) })
	return out, errors.Join(errs...)
}
