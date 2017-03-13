package job

import (
	"testing"
	"time"

	"github.com/iamduo/workq/int/testutil"
)

func TestLeaseDefault(t *testing.T) {
	reg := NewRegistry()
	qc := NewQueueController()
	jc := NewController(reg, qc)

	id := ID(testutil.GenID())
	name := testutil.GenName()

	actJ, err := jc.Lease([]string{name}, 10)
	if err != ErrTimeout || actJ != nil {
		t.Fatalf("No work should be available yet, job=%v, err=%v", actJ, err)
	}

	j := NewEmptyJob()
	j.ID = id
	j.Name = name
	j.Payload = []byte("a")
	j.TTR = 100

	rec := NewRunRecord()
	rec.Job = j
	ok := reg.Add(rec)
	if !ok {
		t.Fatalf("Unable to add record")
	}

	ok = qc.Add(j)
	if !ok {
		t.Fatalf("Unable to add job to queue")
	}

	actJ, err = jc.Lease([]string{name}, 10)
	if err != nil {
		t.Fatalf("Lease err=%v", err)
	}

	if j != actJ {
		t.Fatalf("Lease job mismatch, exp=%+v, act=%+v", j, actJ)
	}

	if qc.Exists(j) {
		t.Fatalf("Job expected to be dequeued")
	}

	rec.Mu.RLock()
	// Assert Attempts
	if rec.Attempts != 1 {
		t.Fatalf("Expected attempts to be 1, act=%v", rec.Attempts)
	}

	if rec.State != StateLeased {
		t.Fatalf("Expected job to be leased state=%v", rec.State)
	}
	rec.Mu.RUnlock()

	// Sleep until after TTR
	// Test overhead padding added.
	ttrDelay := time.Duration(j.TTR) * time.Millisecond
	time.Sleep(ttrDelay + ttrDelay/10)

	rec.Mu.RLock()
	if rec.State != StatePending {
		t.Fatalf("Expected job to be requeued due to TTR, state=%v", rec.State)
	}
	rec.Mu.RUnlock()

	if !qc.Exists(j) {
		t.Fatalf("Unable to find job, expected it to be requeued")
	}
}

func TestLeaseMissingRunRecord(t *testing.T) {
	reg := NewRegistry()
	qc := NewQueueController()
	jc := NewController(reg, qc)

	j := NewEmptyJob()
	j.ID = ID(testutil.GenID())
	j.Name = testutil.GenName()
	j.Payload = []byte("a")

	if !qc.Add(j) {
		t.Fatalf("Unable to add job to queue")
	}

	actJ, err := jc.Lease([]string{j.Name}, 10)
	if err != ErrTimeout {
		t.Fatalf("Lease err mismatch, err=%s", err)
	}

	if actJ != nil {
		t.Fatalf("Expected nil leased job, act=%+v", actJ)
	}
}

func TestLeaseStartAttemptOnAlreadyRunningJob(t *testing.T) {
	reg := NewRegistry()
	jc := NewController(reg, NewQueueController())

	expJ := NewEmptyJob()
	expJ.Name = testutil.GenName()
	expJ.ID = ID(testutil.GenID())

	rec := NewRunRecord()
	rec.Job = expJ
	rec.State = StateLeased
	if !reg.Add(rec) {
		t.Fatalf("Unable to add record")
	}

	if err := jc.StartAttempt(expJ.ID); err != ErrAlreadyLeased {
		t.Fatalf("Start Attempt err mismatch, act=%s, exp=%s", err, ErrAlreadyLeased)
	}
}

func TestLeaseStartAttemptOnAlreadyProcessedJob(t *testing.T) {
	reg := NewRegistry()
	jc := NewController(reg, NewQueueController())

	expJ := NewEmptyJob()
	expJ.Name = testutil.GenName()
	expJ.ID = ID(testutil.GenID())

	rec := NewRunRecord()
	rec.Job = expJ
	rec.State = StateCompleted
	if !reg.Add(rec) {
		t.Fatalf("Unable to add record")
	}

	if err := jc.StartAttempt(expJ.ID); err != ErrAlreadyProcessed {
		t.Fatalf("Start Attempt err mismatch, act=%s, exp=%s", err, ErrAlreadyProcessed)
	}
}

func TestLeaseMaxAttempts(t *testing.T) {
	reg := NewRegistry()
	qc := NewQueueController()
	jc := NewController(reg, qc)

	tests := []struct {
		MaxAttempts uint8
		Attempts    uint64
	}{
		{MaxAttempts: 3, Attempts: 3},
		{MaxAttempts: MaxHardAttempts, Attempts: MaxAttempts},
		{MaxAttempts: 3, Attempts: MaxAttempts},
	}

	for _, tt := range tests {
		j := NewEmptyJob()
		j.ID = ID(testutil.GenID())
		j.Name = testutil.GenName()
		j.Payload = []byte("a")
		j.MaxAttempts = tt.MaxAttempts

		rec := NewRunRecord()
		rec.Job = j
		rec.Attempts = tt.Attempts
		if !reg.Add(rec) {
			t.Fatalf("Unable to add record")
		}

		if err := jc.StartAttempt(j.ID); err != ErrMaxAttemptsReached {
			t.Fatalf("Start Attempt err mismatch, act=%s, exp=%s", err, ErrMaxAttemptsReached)
		}
	}
}

func TestLeaseWaitTimeout(t *testing.T) {
	reg := NewRegistry()
	qc := NewQueueController()
	jc := NewController(reg, qc)

	start := time.Now().UTC()
	j, err := jc.Lease([]string{testutil.GenName()}, 10)
	diff := time.Since(start)
	if err != ErrTimeout || j != nil {
		t.Fatalf("No work should be available yet, err=%v, job=%v", err, j)
	}

	if diff < (10*time.Millisecond) || diff >= (15*time.Millisecond) {
		t.Errorf("Run wait-timeout not in range, actual-diff=%s", diff)
	}
}

func TestLeaseMultipleNames(t *testing.T) {
	reg := NewRegistry()
	qc := NewQueueController()
	jc := NewController(reg, qc)

	type testData struct {
		name    string
		payload []byte
	}

	tests := make(map[ID]*Job)
	j := &Job{
		ID:      ID(testutil.GenID()),
		Name:    testutil.GenName(),
		Payload: []byte("a"),
		TTR:     100,
	}
	tests[j.ID] = j

	j = &Job{
		ID:      ID(testutil.GenID()),
		Name:    testutil.GenName(),
		Payload: []byte("b"),
		TTR:     100,
	}
	tests[j.ID] = j

	j = &Job{
		ID:      ID(testutil.GenID()),
		Name:    testutil.GenName(),
		Payload: []byte("c"),
		TTR:     100,
	}
	tests[j.ID] = j

	var names []string
	expJobs := make(map[ID]*Job, len(tests))
	for k, j := range tests {

		rec := NewRunRecord()
		rec.Job = j
		ok := reg.Add(rec)
		if !ok {
			t.Fatalf("Unable to add record")
		}

		ok = qc.Add(j)
		if !ok {
			t.Fatalf("Unable to add job to queue")
		}

		names = append(names, j.Name)
		expJobs[k] = j
	}

	for i := 0; i < len(names); i++ {
		j, err := jc.Lease(names, 10)
		if err != nil {
			t.Fatalf("Unexpected lease err=%v", err)
		}

		assertJobs(t, expJobs[j.ID], j)

		rec, ok := reg.Record(j.ID)
		if !ok {
			t.Fatalf("Run record missing")
		}

		if qc.Exists(rec.Job) {
			t.Fatalf("Job expected to be dequeued")
		}

		// Assert Attempts
		rec.Mu.RLock()
		if rec.Attempts != 1 {
			t.Fatalf("Expected attempts to be 1, id=%s, attempts=%v", rec.Job.ID, rec.Attempts)
		}
		rec.Mu.RUnlock()

		ttrDelay := time.Duration(j.TTR) * time.Millisecond
		// Test overhead padding added.
		time.Sleep(ttrDelay + ttrDelay/10)
		rec.Mu.RLock()
		if rec.State != StatePending {
			t.Fatalf("Expected job to be requeued due to TTR, state=%v", rec.State)
		}
		rec.Mu.RUnlock()

		if !qc.Exists(rec.Job) {
			t.Fatalf("Unable to find job, expected it to be requeued")
		}

		qc.Delete(rec.Job)
	}
}

func TestTimeoutAttempt(t *testing.T) {
	reg := NewRegistry()
	qc := NewQueueController()
	jc := NewController(reg, qc)

	id := ID(testutil.GenID())
	name := testutil.GenName()

	j := NewEmptyJob()
	j.ID = id
	j.Name = name
	j.Payload = []byte("a")
	j.TTR = 10

	rec := NewRunRecord()
	rec.Job = j
	if !reg.Add(rec) {
		t.Fatalf("Unable to add record")
	}

	if !qc.Add(j) {
		t.Fatalf("Unable to add job to queue")
	}

	rec.Mu.Lock()
	rec.State = StateCompleted
	rec.Mu.Unlock()
	copy := *rec
	jc.TimeoutAttempt(rec.Job.ID)
	if !compareRunRecord(&copy, rec) {
		t.Fatalf("timeoutAttempt expected to be NO-OP")
	}

	rec.Mu.Lock()
	rec.State = StateLeased
	rec.Job.MaxAttempts = 3
	rec.Attempts = 3
	rec.Mu.Unlock()
	jc.TimeoutAttempt(rec.Job.ID)
	if rec.State != StateFailed {
		t.Fatalf("Expected timeoutAttempt to fail job, state=%v", rec.State)
	}

	select {
	case res := <-rec.Wait:
		if res.Success != false {
			t.Fatalf("TimeoutAttempt final attempt success mismatch")
		}
	default:
		t.Fatalf("Expected TimeoutAttempt final attempt to send type RunResult")
	}

	rec.Mu.Lock()
	rec.State = StateNew
	rec.Attempts = 0
	rec.Mu.Unlock()

	qc.Delete(rec.Job)
	reg.Delete(rec.Job.ID)
	jc.TimeoutAttempt(rec.Job.ID)
	if qc.Exists(rec.Job) {
		t.Fatalf("Expected timeoutAttempt to NO-OP")
	}
}

// Internal test for code coverage.
// Simulates a leased job deleted mid-attempt.
func TestLeaseTTRTimerCancelled(t *testing.T) {
	reg := NewRegistry()
	qc := NewQueueController()
	jc := NewController(reg, qc)

	j := NewEmptyJob()
	j.ID = ID(testutil.GenID())
	j.Name = testutil.GenName()
	j.Payload = []byte("a")
	j.TTR = 100
	j.MaxAttempts = 1

	rec := NewRunRecord()
	rec.Job = j
	if !reg.Add(rec) {
		t.Fatalf("Unable to add record")
	}

	if !qc.Add(j) {
		t.Fatalf("Unable to add job to queue")
	}

	_, err := jc.Lease([]string{j.Name}, uint32(100))
	if err != nil {
		t.Fatalf("Lease err=%v", err)
	}

	if qc.Exists(j) {
		t.Fatalf("Job expected to be dequeued")
	}

	rec.Mu.RLock()
	rec.Timers[RunRecTTRTimerIdx].Cancel()
	rec.Mu.RUnlock()

	// Verify that TTR cancellation stops all further TTR event processing.
	// Specifically after the TTR is up.
	// Test overhead padding addded.
	ttrDelay := time.Duration(j.TTR) * time.Millisecond
	time.Sleep(ttrDelay + ttrDelay/10)

	rec.Mu.RLock()
	defer rec.Mu.RUnlock()
	if rec.State != StateLeased {
		t.Fatalf("TTR Timer cancellation failed, state=%v", rec.State)
	}
}

func TestLeaseInvalidArgs(t *testing.T) {
	reg := NewRegistry()
	qc := NewQueueController()
	jc := NewController(reg, qc)

	tests := []struct {
		names   []string
		timeout uint32
		err     error
	}{
		{
			[]string{},
			10,
			ErrInvalidName,
		},
		{
			[]string{"%"},
			10,
			ErrInvalidName,
		},
		{
			[]string{"a", "%"},
			10,
			ErrInvalidName,
		},
		{
			[]string{"a"},
			MaxTimeout + 1,
			ErrInvalidTimeout,
		},
	}

	for _, tt := range tests {
		j, err := jc.Lease(tt.names, tt.timeout)
		if tt.err != err {
			t.Fatalf("Err mismatch, exp=%v, got=%v", tt.err, err)
		}

		if j != nil {
			t.Fatalf("Expected nil job, got=%+v", j)
		}
	}
}
