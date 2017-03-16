package job

import (
	"fmt"
	"testing"
	"time"

	"github.com/iamduo/workq/int/testutil"
)

func TestAdd(t *testing.T) {
	id := ID(testutil.GenID())
	name := testutil.GenName()
	tests := []struct {
		expJob *Job
	}{
		{
			expJob: &Job{
				ID:      id,
				Name:    name,
				Payload: []byte(""),
				TTR:     1,
				TTL:     100,
			},
		},
		{
			expJob: &Job{
				ID:      id,
				Name:    name,
				Payload: []byte("a"),
				TTR:     1,
				TTL:     100,
			},
		},
		{
			expJob: &Job{
				ID:       id,
				Name:     name,
				Payload:  []byte("a"),
				Priority: 10,
				TTR:      1,
				TTL:      100,
			},
		},
		{
			expJob: &Job{
				ID:          id,
				Name:        name,
				Payload:     []byte("a"),
				MaxAttempts: 3,
				TTR:         1,
				TTL:         100,
			},
		},
		{
			expJob: &Job{
				ID:       id,
				Name:     name,
				Payload:  []byte("a"),
				MaxFails: 3,
				TTR:      1,
				TTL:      100,
			},
		},
		{
			expJob: &Job{
				ID:          id,
				Name:        name,
				Payload:     []byte("a"),
				Priority:    10,
				MaxAttempts: 9,
				MaxFails:    3,
				TTR:         1,
				TTL:         100,
			},
		},
		// Test for MAX payload
		{
			expJob: &Job{
				ID:          id,
				Name:        name,
				Payload:     make([]byte, 1048576),
				MaxAttempts: 9,
				MaxFails:    3,
				TTR:         1,
				TTL:         100,
			},
		},
	}

	for _, tt := range tests {
		tt.expJob.Created = time.Now().UTC()

		reg := NewRegistry()
		qc := NewQueueController()
		jc := NewController(reg, qc)

		_, ok := reg.Record(id)
		if ok {
			t.Fatalf("Expected no record to exist")
		}

		expRec := NewRunRecord()
		expRec.State = StateNew
		expRec.Job = tt.expJob

		err := jc.Add(tt.expJob)
		if err != nil {
			t.Fatalf("Add err=%s", err)
		}

		rec, ok := reg.Record(id)
		if !ok {
			t.Fatalf("Expected record to exist")
		}

		if !qc.Exists(tt.expJob) {
			t.Fatalf("Expected job to queued")
		}

		rec.Mu.RLock()
		assertRunRecords(t, expRec, rec)
		actJob := rec.Job
		rec.Mu.RUnlock()
		assertJobs(t, tt.expJob, actJob)

		// Verify job still exists right before TTL, elapsing 90% of TTL.
		expireDelay := time.Duration(tt.expJob.TTL) * time.Millisecond
		time.Sleep((expireDelay / 10) * 8)

		if _, ok = reg.Record(id); !ok {
			t.Fatalf("Expected record to exist")
		}

		if !qc.Exists(tt.expJob) {
			t.Fatalf("Expected job to queued")
		}

		// Verify job expires after ttl.
		// Test overhead padding added.
		time.Sleep((expireDelay / 10) * 4)

		if qc.Exists(actJob) {
			t.Fatalf("Expected job to be expired")
		}

		_, ok = reg.Record(id)
		if ok {
			t.Fatalf("Expected record to be expired")
		}
	}
}

func TestAddDuplicate(t *testing.T) {
	reg := NewRegistry()
	qc := NewQueueController()
	jc := NewController(reg, qc)

	expJob := &Job{
		ID:          ID(testutil.GenID()),
		Name:        testutil.GenName(),
		Payload:     []byte("a"),
		MaxAttempts: 0,
		MaxFails:    0,
		TTR:         100,
		TTL:         1000,
		Created:     time.Now().UTC(),
	}

	err := jc.Add(expJob)
	if err != nil {
		t.Fatalf("Add err=%v", err)
	}

	err = jc.Add(expJob)
	if err != ErrDuplicateJob {
		t.Fatalf("Expected duplicate job err=%s", err)
	}
}

func TestAddCancelledTTLTimer(t *testing.T) {
	reg := NewRegistry()
	qc := NewQueueController()
	jc := NewController(reg, qc)

	id := ID(testutil.GenID())
	name := testutil.GenName()

	expJob := &Job{
		ID:          id,
		Name:        name,
		Payload:     []byte("a"),
		MaxAttempts: 0,
		MaxFails:    0,
		TTR:         100,
		TTL:         100,
		Created:     time.Now().UTC(),
	}

	err := jc.Add(expJob)
	if err != nil {
		t.Fatalf("Add err=%s", err)
	}

	rec, ok := reg.Record(id)
	if !ok {
		t.Fatalf("Expected record to exist")
	}

	rec.Mu.Lock()
	actJob := rec.Job
	assertNewRunRecord(t, rec)
	assertJobs(t, expJob, actJob)

	rec.Timers[RunRecTTLTimerIdx].Cancel()
	rec.Mu.Unlock()

	// Wait for TTL and ensure job still exists
	// Test overhead padding added.
	ttlDelay := time.Duration(expJob.TTL) * time.Millisecond
	time.Sleep(ttlDelay + ttlDelay/10)

	if qc.Add(actJob) {
		t.Fatalf("Job seems to have expired after TTL cancellation")
	}

	_, ok = reg.Record(id)
	if !ok {
		t.Fatalf("Unable to find record after TTL cancellation")
	}
}

// This is not expected during normal use. Testing for coverage.
func TestAddOutofSyncQueue(t *testing.T) {
	jc := NewController(NewRegistry(), &OutOfSyncController{})

	expJob := &Job{
		ID:      ID(testutil.GenID()),
		Name:    testutil.GenName(),
		TTR:     1,
		TTL:     1,
		Created: time.Now().UTC(),
	}

	err := jc.Add(expJob)
	if err != ErrQueueOutOfSync {
		t.Fatalf("Add response mismatch")
	}
}

func TestAddInvalidArgs(t *testing.T) {
	reg := NewRegistry()
	qc := NewQueueController()
	jc := NewController(reg, qc)

	id := ID(testutil.GenID())
	name := testutil.GenName()
	tests := []struct {
		job *Job
		err error
	}{
		{
			job: &Job{
				Name:    name,
				TTR:     100,
				TTL:     1000,
				Created: time.Now().UTC(),
			},
			err: ErrInvalidID,
		},
		{
			job: &Job{
				ID:      id,
				TTR:     100,
				TTL:     1000,
				Created: time.Now().UTC(),
			},
			err: ErrInvalidName,
		},
		{
			job: &Job{
				ID:      id,
				Name:    "*",
				TTR:     100,
				TTL:     1000,
				Created: time.Now().UTC(),
			},
			err: ErrInvalidName,
		},
		{
			job: &Job{
				ID:      id,
				Name:    name,
				TTL:     100,
				Created: time.Now().UTC(),
			},
			err: ErrInvalidTTR,
		},
		{
			job: &Job{
				ID:      id,
				Name:    name,
				TTR:     100,
				Created: time.Now().UTC(),
			},
			err: ErrInvalidTTL,
		},
		{
			job: &Job{
				ID:      id,
				Name:    name,
				TTR:     100,
				Created: time.Now().UTC(),
			},
			err: ErrInvalidTTL,
		},
		{
			job: &Job{
				ID:      id,
				Name:    name,
				TTR:     100,
				TTL:     1000,
				Created: time.Now().Add(-10 * time.Second).UTC(),
			},
			err: ErrInvalidTime,
		},
		{
			job: &Job{
				ID:      id,
				Name:    name,
				TTR:     100,
				TTL:     1000,
				Payload: make([]byte, MaxPayload+1),
				Created: time.Now().UTC(),
			},
			err: ErrInvalidPayload,
		},
	}

	for _, tt := range tests {
		err := jc.Add(tt.job)
		if err != tt.err {
			t.Fatalf("Err mismatch, err=%s, exp=%s", err, tt.err)
		}

		if _, ok := reg.Record(tt.job.ID); ok {
			t.Fatalf("Unepected run record")
		}

		if qc.Exists(tt.job) {
			t.Fatalf("Unexpected job")
		}
	}
}

func BenchmarkAdd(b *testing.B) {
	reg := NewRegistry()
	qc := NewQueueController()
	jc := NewController(reg, qc)

	jobs := make([]*Job, b.N)
	for i := 0; i < b.N; i++ {
		jobs[i] = &Job{
			ID:      ID(testutil.GenID()),
			Name:    "bench",
			TTR:     5000,
			TTL:     360000,
			Created: time.Now().UTC(),
		}
	}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		err := jc.Add(jobs[i])
		if err != nil {
			fmt.Println(err)
		}
	}
}
