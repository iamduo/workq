package job

import (
	"bytes"
	"testing"
	"time"

	"github.com/iamduo/workq/int/testutil"
)

func TestRunWaitTimeout(t *testing.T) {
	name := testutil.GenName()
	tests := []struct {
		expJob     *Job
		expTimeout int
	}{
		{
			expJob: &Job{
				ID:          ID(testutil.GenID()),
				Name:        name,
				Payload:     []byte("a"),
				MaxAttempts: 0,
				MaxFails:    0,
				TTR:         5,
				Created:     time.Now().UTC(),
			},
			expTimeout: 10,
		},
		{
			expJob: &Job{
				ID:          ID(testutil.GenID()),
				Name:        name,
				Payload:     []byte("a"),
				MaxAttempts: 0,
				MaxFails:    0,
				Priority:    3,
				TTR:         5,
				Created:     time.Now().UTC(),
			},
			expTimeout: 10,
		},
	}

	for _, tt := range tests {
		reg := NewRegistry()
		qc := NewQueueController()
		jc := NewController(reg, qc)

		// Assert for timeout timeliness in a goroutine to unblock
		// standard job assertions in parallel.
		done := make(chan struct{})
		go func() {
			defer close(done)
			start := time.Now()

			res, err := jc.Run(tt.expJob, uint32(tt.expTimeout))
			if res != nil || err != ErrTimeout {
				t.Fatalf("Expected ErrTimeout, res=%v, err=%s", res, err)
			}

			diff := time.Now().Sub(start)
			// 10ms padding for test load overhead.
			if diff <= time.Duration(tt.expTimeout) || diff >= time.Duration(tt.expTimeout+10)*time.Millisecond {
				t.Fatalf("Unexpected timeout length, diff=%s", diff)
			}
		}()

		// Job & Queue assertions while the wait timeout is in process.

		time.Sleep(500 * time.Microsecond)
		rec, ok := reg.Record(tt.expJob.ID)
		if !ok {
			t.Fatalf("Expected record to exist")
		}

		rec.Mu.RLock()
		actJob := rec.Job
		if !qc.Exists(actJob) {
			t.Fatalf("Expected job to exist")
		}

		expRec := NewRunRecord()
		expRec.State = StateNew
		assertRunRecords(t, expRec, rec)
		rec.Mu.RUnlock()

		assertJobs(t, tt.expJob, actJob)
		<-done

		_, ok = reg.Record(actJob.ID)
		if ok {
			t.Fatalf("Expected record to be deleted after run")
		}

		if qc.Exists(actJob) {
			t.Fatalf("Expected job to be expired")
		}
	}
}

func TestRunResult(t *testing.T) {
	reg := NewRegistry()
	qc := NewQueueController()
	jc := NewController(reg, qc)

	expRes := &RunResult{Success: true, Result: []byte("b")}
	expJob := &Job{
		ID:      ID(testutil.GenID()),
		Name:    testutil.GenName(),
		Payload: []byte("a"),
		TTR:     5,
		Created: time.Now().UTC(),
	}

	done := make(chan struct{}, 1)
	go func() {
		defer close(done)

		res, err := jc.Run(expJob, 10)
		if err != nil {
			t.Fatalf("Run err=%v", err)
		}
		if res == nil ||
			expRes.Success != res.Success ||
			!bytes.Equal(expRes.Result, res.Result) {
			t.Fatalf("Run res mismatch, res=%+v", res)
		}
	}()

	// Delay for run exec above
	time.Sleep(500 * time.Microsecond)

	rec, ok := reg.Record(expJob.ID)
	if !ok {
		t.Fatalf("Expected record to exist")
	}

	rec.Mu.RLock()
	actJob := rec.Job
	if !qc.Exists(actJob) {
		t.Fatalf("Expected job to exist")
	}

	expRec := NewRunRecord()
	expRec.State = StateNew
	assertRunRecords(t, expRec, rec)
	rec.Mu.RUnlock()

	assertJobs(t, expJob, actJob)

	rec.Mu.Lock()
	rec.State = StateCompleted
	rec.WriteResult([]byte(expRes.Result), true)
	rec.Mu.Unlock()

	<-done

	_, ok = reg.Record(actJob.ID)
	if ok {
		t.Fatalf("Expected record to be deleted after run")
	}

	if qc.Exists(actJob) {
		t.Fatalf("Expected job to be deleted")
	}
}

// This is not expected during normal use. Testing for coverage.
func TestRunOutofSyncQueue(t *testing.T) {
	jc := NewController(NewRegistry(), &OutOfSyncController{})

	j := NewEmptyJob()
	j.ID = ID(testutil.GenID())
	j.Name = testutil.GenName()
	j.TTR = 1
	j.TTL = 1

	res, err := jc.Run(j, 10)
	if res != nil || err != ErrQueueOutOfSync {
		t.Fatalf("Run err mismatch, res=%+v, err=%s", res, err)
	}
}

// Test result after wait-timeout, but before ttr.
func TestRunResultAfterWaitTimeout(t *testing.T) {
	reg := NewRegistry()
	qc := NewQueueController()
	jc := NewController(reg, qc)

	expRes := &RunResult{Success: true, Result: []byte("b")}
	expJob := &Job{
		ID:      ID(testutil.GenID()),
		Name:    testutil.GenName(),
		Payload: []byte("a"),
		TTR:     100,
		Created: time.Now().UTC(),
	}

	done := make(chan struct{}, 1)
	go func() {
		defer close(done)

		res, err := jc.Run(expJob, 10)
		if err != nil {
			t.Fatalf("Run err=%v", err)
		}
		if res == nil ||
			expRes.Success != res.Success ||
			!bytes.Equal(expRes.Result, res.Result) {
			t.Fatalf("Run response mismatch, res=%+v", res)
		}
	}()

	// Delay for run exec above
	time.Sleep(500 * time.Microsecond)

	rec, ok := reg.Record(expJob.ID)
	if !ok {
		t.Fatalf("Expected record to exist")
	}

	rec.Mu.RLock()
	actJob := rec.Job
	if !qc.Exists(actJob) {
		t.Fatalf("Expected job to exist")
	}

	expRec := NewRunRecord()
	expRec.State = StateNew
	assertRunRecords(t, expRec, rec)
	rec.Mu.RUnlock()

	assertJobs(t, expJob, actJob)

	rec.Mu.Lock()
	rec.State = StateLeased
	rec.Timers[RunRecTTRTimerIdx] = NewTimer(time.Duration(expJob.TTR) * time.Millisecond)
	rec.Mu.Unlock()

	// Sleep after wait-timeout
	time.Sleep(10 * time.Millisecond)
	rec.Mu.Lock()
	rec.State = StateCompleted
	rec.WriteResult([]byte(expRes.Result), true)
	rec.Mu.Unlock()

	<-done

	rec, ok = reg.Record(actJob.ID)
	if ok {
		t.Fatalf("Expected record to be deleted after run")
	}

	if qc.Exists(actJob) {
		t.Fatalf("Expected job to be deleted")
	}
}

func TestRunTTRTimeout(t *testing.T) {
	reg := NewRegistry()
	qc := NewQueueController()
	jc := NewController(reg, qc)

	expJob := &Job{
		ID:      ID(testutil.GenID()),
		Name:    testutil.GenName(),
		Payload: []byte("a"),
		TTR:     2,
		Created: time.Now().UTC(),
	}

	done := make(chan struct{}, 1)
	go func() {
		defer close(done)

		res, err := jc.Run(expJob, 10)
		if res != nil || err != ErrTimeout {
			t.Fatalf("Run result mismatch, res=%v, err=%v", res, err)
		}
	}()

	// Pick up work before wait-timeout
	time.Sleep(500 * time.Microsecond)

	rec, ok := reg.Record(expJob.ID)
	if !ok {
		t.Fatalf("Expected record to exist")
	}

	rec.Mu.RLock()
	actJob := rec.Job
	if !qc.Exists(actJob) {
		t.Fatalf("Expected job to exist")
	}

	expRec := NewRunRecord()
	expRec.Job = expJob
	expRec.State = StateNew
	assertRunRecords(t, expRec, rec)
	rec.Mu.RUnlock()

	assertJobs(t, expJob, actJob)

	rec.Mu.Lock()
	rec.State = StateLeased
	rec.Timers[RunRecTTRTimerIdx] = NewTimer(2 * time.Millisecond)
	rec.Mu.Unlock()

	// Sleep after wait-timeout + ttr
	time.Sleep(time.Millisecond * 2)
	<-done

	_, ok = reg.Record(actJob.ID)
	if ok {
		t.Fatalf("Expected record to be deleted after run")
	}

	if qc.Exists(actJob) {
		t.Fatalf("Expected job to be deleted")
	}
}

// Tests to ensure TTR timeout when TTR timer is cleared
func TestRunTTRTimeoutNoTimer(t *testing.T) {
	reg := NewRegistry()
	qc := NewQueueController()
	jc := NewController(reg, qc)

	expJob := &Job{
		ID:      ID(testutil.GenID()),
		Name:    testutil.GenName(),
		Payload: []byte("a"),
		TTR:     2,
		Created: time.Now().UTC(),
	}

	done := make(chan struct{}, 1)
	go func() {
		defer close(done)

		res, err := jc.Run(expJob, 10)
		if err != ErrTimeout || res != nil {
			t.Fatalf("Run result mismatch, res=%v, err=%v", res, err)
		}
	}()

	// Pick up work before wait-timeout
	time.Sleep(500 * time.Microsecond)

	rec, ok := reg.Record(expJob.ID)
	if !ok {
		t.Fatalf("Expected record to exist")
	}

	rec.Mu.RLock()
	actJob := rec.Job
	if !qc.Exists(actJob) {
		t.Fatalf("Expected job to exist")
	}

	expRec := NewRunRecord()
	expRec.Job = expJob
	expRec.State = StateNew
	assertRunRecords(t, expRec, rec)
	rec.Mu.RUnlock()

	assertJobs(t, expJob, actJob)

	rec.Mu.Lock()
	rec.State = StateLeased
	rec.Timers[RunRecTTRTimerIdx] = nil
	rec.Mu.Unlock()

	// Sleep after wait-timeout + ttr
	time.Sleep(time.Millisecond * 2)
	<-done
}

func TestRunDuplicate(t *testing.T) {
	reg := NewRegistry()
	qc := NewQueueController()
	jc := NewController(reg, qc)

	expJob := &Job{
		ID:      ID(testutil.GenID()),
		Name:    testutil.GenName(),
		Payload: []byte("a"),
		TTR:     5,
		Created: time.Now().UTC(),
	}

	// Run 2 jobs in sequence with a 10ms timeout, causing the second job
	// to be a duplicate.
	done := make(chan struct{}, 2)
	go func() {
		defer func() { done <- struct{}{} }()

		res, err := jc.Run(expJob, 10)
		if res != nil || err != ErrTimeout {
			t.Fatalf("Run result mismatch, res=%+v, err=%v", res, err)
		}
	}()
	time.Sleep(250 * time.Microsecond)
	go func() {
		defer func() { done <- struct{}{} }()

		res, err := jc.Run(expJob, 10)
		if res != nil || err != ErrDuplicateJob {
			t.Fatalf("Run result mismatch, res=%v, err=%v", res, err)
		}
	}()

	// Sleep after wait-timeout
	time.Sleep(time.Millisecond * 10)
	<-done
	<-done
}

func TestRunInvalidArgs(t *testing.T) {
	reg := NewRegistry()
	qc := NewQueueController()
	jc := NewController(reg, qc)

	id := ID(testutil.GenID())
	name := testutil.GenName()
	tests := []struct {
		job     *Job
		timeout uint32
		err     error
	}{
		{
			&Job{
				Name: name,
				TTR:  1,
			},
			1,
			ErrInvalidID,
		},
		{
			&Job{
				ID:   id,
				Name: name,
			},
			1,
			ErrInvalidTTR,
		},
		{
			&Job{
				ID:   id,
				Name: name,
				TTR:  MaxTTR + 1,
			},
			1,
			ErrInvalidTTR,
		},
		{
			&Job{
				ID:  id,
				TTR: 1,
			},
			1,
			ErrInvalidName,
		},
		{
			&Job{
				ID:   id,
				Name: "*",
				TTR:  1,
			},
			1,
			ErrInvalidName,
		},
		{
			&Job{
				ID:   id,
				Name: name,
				TTR:  1,
			},
			MaxTimeout + 1,
			ErrInvalidTimeout,
		},
		{
			&Job{
				ID:      id,
				Name:    name,
				TTR:     1,
				Payload: make([]byte, MaxPayload+1),
			},
			1,
			ErrInvalidPayload,
		},
	}

	for _, tt := range tests {
		res, err := jc.Run(tt.job, tt.timeout)
		if err != tt.err {
			t.Fatalf("Err mismatch, err=%s, exp=%s", err, tt.err)
		}

		if res != nil {
			t.Fatalf("Unexpected result, res=%+v", res)
		}

		if _, ok := reg.Record(tt.job.ID); ok {
			t.Fatalf("Unexpected run record")
		}

		if qc.Exists(tt.job) {
			t.Fatalf("Unexpected job")
		}
	}
}
