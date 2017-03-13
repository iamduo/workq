package job

import (
	"bytes"
	"sync"
	"testing"
	"time"
)

func TestHandleExpireFunc(t *testing.T) {
	jc := NewController(NewRegistry(), NewQueueController())
	called := false
	fn := func(id ID) {
		called = true
	}

	jc.HandleExpire(fn)
	jc.ExpireFunc()(ID{})

	if !called {
		t.Fatalf("ExpireFunc not called")
	}
}

func TestHandleTimeoutAttemptFunc(t *testing.T) {
	jc := NewController(NewRegistry(), NewQueueController())
	called := false
	fn := func(id ID) {
		called = true
	}

	jc.HandleTimeoutAttempt(fn)
	jc.TimeoutAttemptFunc()(ID{})

	if !called {
		t.Fatalf("TimeoutAttemptFunc not called")
	}
}

type OutOfSyncController struct{}

func (q *OutOfSyncController) Queues() (map[string]QueueInterface, *sync.RWMutex) {
	return make(map[string]QueueInterface), &sync.RWMutex{}
}

func (q *OutOfSyncController) Queue(name string) QueueInterface {
	return NewWorkQueue()
}

func (q *OutOfSyncController) Add(j *Job) bool {
	return false
}

func (q *OutOfSyncController) Run(j *Job) bool {
	return false
}

func (q *OutOfSyncController) Schedule(j *Job) bool {
	return false
}

func (q *OutOfSyncController) Delete(j *Job) bool {
	return true
}

func (q *OutOfSyncController) Awake(j *Job) bool {
	return true
}

func (q *OutOfSyncController) Exists(j *Job) bool {
	return true
}

func (q *OutOfSyncController) Lease(name string) <-chan JobProxy {
	return make(chan JobProxy, 1)
}

func compareRunRecord(a *RunRecord, b *RunRecord) bool {
	return a.Attempts == b.Attempts &&
		a.State == b.State &&
		a.Fails == b.Fails &&
		a.Job == b.Job &&
		bytes.Equal(a.Result, b.Result)
}

func assertNewRunRecord(t *testing.T, rec *RunRecord) {
	ok := rec.Attempts == 0 &&
		rec.State == StateNew &&
		rec.Fails == 0 &&
		rec.Job != nil &&
		rec.Result == nil &&
		rec.Timers[RunRecTTLTimerIdx] != nil

	if !ok {
		t.Fatalf("Run record does not appear to be 'new', rec=%+v", rec)
	}
}

func assertRunRecords(t *testing.T, a *RunRecord, b *RunRecord) {
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

	if a.Timers[RunRecSchedTimerIdx] != nil {
		if b.Timers[RunRecSchedTimerIdx] == nil {
			t.Fatalf("Unexpected nil scheduled timer")
		}

		bDeadline := a.Timers[RunRecSchedTimerIdx].Deadline
		diff := a.Timers[RunRecSchedTimerIdx].Deadline.Sub(bDeadline) * time.Millisecond
		if diff < -1000 || diff > 1000 {
			t.Fatalf("Out of range scheduled timer")
		}
	}

	if a.Timers[RunRecTTLTimerIdx] != nil {
		if b.Timers[RunRecTTLTimerIdx] == nil {
			t.Fatalf("Unexpected nil TTL timer")
		}

		bDeadline := a.Timers[RunRecTTLTimerIdx].Deadline
		diff := a.Timers[RunRecTTLTimerIdx].Deadline.Sub(bDeadline) * time.Millisecond
		if diff < -1000 || diff > 1000 {
			t.Fatalf("Out of range TTL timer")
		}
	}
}

func assertJobs(t *testing.T, a *Job, b *Job) {
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
