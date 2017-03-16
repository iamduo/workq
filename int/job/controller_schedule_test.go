package job

import (
	"testing"
	"time"

	"github.com/iamduo/workq/int/testutil"
)

func TestSchedule(t *testing.T) {
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
				Priority:    10,
				MaxAttempts: 9,
				MaxFails:    3,
				TTR:         1,
				TTL:         100,
			},
		},
	}

	for _, tt := range tests {
		reg := NewRegistry()
		qc := NewQueueController()
		jc := NewController(reg, qc)

		_, ok := reg.Record(id)
		if ok {
			t.Fatalf("Expected no record to exist")
		}

		delay := 100 * time.Millisecond
		tt.expJob.Created = time.Now().UTC()
		tt.expJob.Time = time.Now().UTC().Add(delay)

		expRec := NewRunRecord()
		expRec.State = StateNew
		expRec.Job = tt.expJob
		expRec.Timers[RunRecSchedTimerIdx] = NewTimer(delay)

		err := jc.Schedule(tt.expJob)
		if err != nil {
			t.Fatalf("Schedule err=%s", err)
		}

		rec, ok := reg.Record(id)
		if !ok {
			t.Fatalf("Expected record to exist")
		}

		rec.Mu.RLock()
		assertRunRecords(t, expRec, rec)
		actJob := rec.Job
		rec.Mu.RUnlock()

		assertJobs(t, tt.expJob, actJob)

		// Time the successful lease of the scheduled job.
		// Time should match approximately the actual scheduled time.
		start := time.Now().UTC()
		leaseCh := qc.Lease(name)
		for {
			proxy := <-leaseCh
			if _, ok := proxy(); ok {
				break
			}
		}
		elapsed := time.Now().Sub(start)
		// Test overhead padding added (20%).
		pad := ((delay / 10) * 2)
		if elapsed < delay-pad || elapsed > delay+pad {
			t.Fatalf("Scheduled time mismatch, act=%s, exp=~%s", elapsed, delay)
		}

		// Verify job still exists right before TTL, elapsing 80% of TTL.
		expireDelay := time.Duration(tt.expJob.TTL) * time.Millisecond
		time.Sleep((expireDelay / 10) * 8)

		if _, ok = reg.Record(id); !ok {
			t.Fatalf("Expected record to exist")
		}

		// Verify job expires after ttl.
		// Test overhead padding added.
		time.Sleep((expireDelay / 10) * 4)

		_, ok = reg.Record(id)
		if ok {
			t.Fatalf("Expected record to be expired")
		}
	}
}

func TestScheduleDuplicate(t *testing.T) {
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
		Time:        time.Now().UTC().Add(1 * time.Second),
	}

	err := jc.Schedule(expJob)
	if err != nil {
		t.Fatalf("Schedule err=%v", err)
	}

	err = jc.Schedule(expJob)
	if err != ErrDuplicateJob {
		t.Fatalf("Expected duplicate job err=%s", err)
	}
}

// Test to ensure scheduled jobs before their scheduled time can gracefully be
// deleted.
func TestScheduleDeleteBeforeTime(t *testing.T) {
	reg := NewRegistry()
	qc := NewQueueController()
	jc := NewController(reg, qc)

	delay := 100 * time.Millisecond
	expJob := &Job{
		ID:      ID(testutil.GenID()),
		Name:    testutil.GenName(),
		Payload: []byte("a"),
		TTR:     1,
		TTL:     100,
		Created: time.Now().UTC(),
		Time:    time.Now().UTC().Add(delay),
	}

	expRec := NewRunRecord()
	expRec.State = StateNew
	expRec.Job = expJob
	expRec.Timers[RunRecSchedTimerIdx] = NewTimer(delay)

	err := jc.Schedule(expJob)
	if err != nil {
		t.Fatalf("Schedule err=%s", err)
	}

	if err := jc.Delete(expJob.ID); err != nil {
		t.Fatalf("Delete after schedule err=%s", err)
	}

	if qc.Exists(expJob) {
		t.Fatalf("Expected job to be deleted")
	}

	_, ok := reg.Record(expJob.ID)
	if ok {
		t.Fatalf("Expected job run record to be deleted")
	}
}

// Test to ensure scheduled jobs after their scheduled time and before expiration
// can gracefully be deleted.
func TestScheduleDeleteAfterAwake(t *testing.T) {
	reg := NewRegistry()
	qc := NewQueueController()
	jc := NewController(reg, qc)

	delay := 100 * time.Millisecond
	expJob := &Job{
		ID:      ID(testutil.GenID()),
		Name:    testutil.GenName(),
		Payload: []byte("a"),
		TTR:     1,
		TTL:     100,
		Created: time.Now().UTC(),
		Time:    time.Now().UTC().Add(delay),
	}

	expRec := NewRunRecord()
	expRec.State = StateNew
	expRec.Job = expJob
	expRec.Timers[RunRecSchedTimerIdx] = NewTimer(delay)

	err := jc.Schedule(expJob)
	if err != nil {
		t.Fatalf("Schedule err=%s", err)
	}

	// Extra padding for test overhead.
	time.Sleep(delay + delay/10)
	if err := jc.Delete(expJob.ID); err != nil {
		t.Fatalf("Delete after schedule err=%s", err)
	}

	if qc.Exists(expJob) {
		t.Fatalf("Expected job to be deleted")
	}

	_, ok := reg.Record(expJob.ID)
	if ok {
		t.Fatalf("Expected job run record to be deleted")
	}
}

// This is not expected during normal use. Testing for coverage.
func TestScheduleOutofSyncQueue(t *testing.T) {
	jc := NewController(NewRegistry(), &OutOfSyncController{})

	j := NewEmptyJob()
	j.ID = ID(testutil.GenID())
	j.Name = testutil.GenName()
	j.TTR = 1
	j.TTL = 1
	j.Time = time.Now().UTC().Add(1 * time.Second)

	err := jc.Schedule(j)
	if err != ErrQueueOutOfSync {
		t.Fatalf("Err mismatch, err=%s", err)
	}
}

// Test to ensure, awake on a invalid job id is a graceful NO-OP.
func TestScheduleAwakeInvalidJob(t *testing.T) {
	reg := NewRegistry()
	qc := NewQueueController()
	jc := NewController(reg, qc)

	jc.awake(ID(testutil.GenID()))
}

func TestScheduleInvalidArgs(t *testing.T) {
	reg := NewRegistry()
	qc := NewQueueController()
	jc := NewController(reg, qc)
	//handler := NewAddHandler(&AddControllerMock{})
	id := ID(testutil.GenID())
	name := testutil.GenName()
	tests := []struct {
		job *Job
		err error
	}{
		{
			job: &Job{
				Name: name,
				TTR:  1,
				TTL:  1,
				Time: time.Now().Add(1 * time.Second),
			},
			err: ErrInvalidID,
		},
		{
			job: &Job{
				ID:   id,
				TTR:  1,
				TTL:  1,
				Time: time.Now().Add(1 * time.Second),
			},
			err: ErrInvalidName,
		},
		{
			job: &Job{
				ID:   id,
				Name: "*",
				TTR:  1,
				TTL:  1,
				Time: time.Now().Add(1 * time.Second),
			},
			err: ErrInvalidName,
		},
		{
			job: &Job{
				ID:   id,
				Name: name,
				TTL:  1,
				Time: time.Now().Add(1 * time.Second),
			},
			err: ErrInvalidTTR,
		},
		{
			job: &Job{
				ID:   id,
				Name: name,
				TTR:  1,
				Time: time.Now().Add(1 * time.Second),
			},
			err: ErrInvalidTTL,
		},
		{
			job: &Job{
				ID:   id,
				Name: name,
				TTR:  1,
				TTL:  1,
				Time: time.Now().Add(-1 * time.Second),
			},
			err: ErrInvalidTime,
		},
		{
			job: &Job{
				ID:      id,
				Name:    name,
				TTR:     1,
				TTL:     1,
				Time:    time.Now().Add(1 * time.Second),
				Payload: make([]byte, MaxPayload+1),
			},
			err: ErrInvalidPayload,
		},
	}

	for _, tt := range tests {
		err := jc.Schedule(tt.job)
		if err != tt.err {
			t.Fatalf("Err mismatch, err=%s, exp=%s", err, tt.err)
		}

		if _, ok := reg.Record(tt.job.ID); ok {
			t.Fatalf("Unexpected run record")
		}

		if qc.Exists(tt.job) {
			t.Fatalf("Unexpected job")
		}
	}
}
