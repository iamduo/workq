package handlers

import (
	"bytes"
	"sync"
	"testing"
	"time"

	"github.com/iamduo/workq/int/job"
	"github.com/iamduo/workq/int/wqueue"
)

type OutOfSyncController struct{}

func (q *OutOfSyncController) Queues() (map[string]wqueue.QueueInterface, *sync.RWMutex) {
	return make(map[string]wqueue.QueueInterface), &sync.RWMutex{}
}

func (q *OutOfSyncController) Queue(name string) wqueue.QueueInterface {
	return wqueue.NewWorkQueue()
}

func (q *OutOfSyncController) Add(j *job.Job) bool {
	return false
}

func (q *OutOfSyncController) Run(j *job.Job) bool {
	return false
}

func (q *OutOfSyncController) Schedule(j *job.Job) bool {
	return false
}

func (q *OutOfSyncController) Delete(j *job.Job) bool {
	return true
}

func (q *OutOfSyncController) Awake(j *job.Job) bool {
	return true
}

func (q *OutOfSyncController) Exists(j *job.Job) bool {
	return true
}

func (q *OutOfSyncController) Lease(name string) <-chan wqueue.JobProxy {
	return make(chan wqueue.JobProxy, 1)
}

func assertNewRunRecord(t *testing.T, rec *job.RunRecord) {
	ok := rec.Attempts == 0 &&
		rec.State == job.StateNew &&
		rec.Fails == 0 &&
		rec.Job != nil &&
		rec.Result == nil &&
		rec.Timers[job.RunRecTTLTimerIdx] != nil

	if !ok {
		t.Fatalf("Run record does not appear to be 'new', rec=%+v", rec)
	}
}

func assertRunRecords(t *testing.T, a *job.RunRecord, b *job.RunRecord) {
	ok := a.Attempts == b.Attempts &&
		a.State == b.State &&
		a.Fails == b.Fails &&
		bytes.Equal(a.Result, b.Result)

	if !ok {
		t.Fatalf("Run record mismatch, exp=%+v, act=%+v", a, b)
	}

	if a.Job != nil {
		if b.Job == nil || a.Job.ID != b.Job.ID {
			t.Fatalf("Run record job mismatch, exp=%s, act=%s", a.Job.ID, b.Job.ID)
		}
	}

	// Must do diff range acceptance assert on timers

	if a.Timers[job.RunRecSchedTimerIdx] != nil {
		if b.Timers[job.RunRecSchedTimerIdx] == nil {
			t.Fatalf("Unexpected nil scheduled timer")
		}

		bDeadline := a.Timers[job.RunRecSchedTimerIdx].Deadline
		diff := a.Timers[job.RunRecSchedTimerIdx].Deadline.Sub(bDeadline) * time.Millisecond
		if diff < -1000 || diff > 1000 {
			t.Fatalf("Out of range scheduled timer")
		}
	}

	if a.Timers[job.RunRecTTLTimerIdx] != nil {
		if b.Timers[job.RunRecTTLTimerIdx] == nil {
			t.Fatalf("Unexpected nil TTL timer")
		}

		bDeadline := a.Timers[job.RunRecTTLTimerIdx].Deadline
		diff := a.Timers[job.RunRecTTLTimerIdx].Deadline.Sub(bDeadline) * time.Millisecond
		if diff < -1000 || diff > 1000 {
			t.Fatalf("Out of range TTL timer")
		}
	}
}

func assertJobs(t *testing.T, a *job.Job, b *job.Job) {
	createTimeDiff := a.Time.Sub(b.Time)
	ok := a.ID == b.ID &&
		a.Name == b.Name &&
		bytes.Equal(a.Payload, b.Payload) &&
		a.Priority == b.Priority &&
		a.MaxAttempts == b.MaxAttempts &&
		a.MaxFails == b.MaxFails &&
		a.TTR == b.TTR &&
		a.TTL == b.TTL &&
		a.Time == b.Time &&
		createTimeDiff < 500
	if !ok {
		t.Fatalf("Job mismatch, exp=%+v, act=%+v", a, b)
	}
}
