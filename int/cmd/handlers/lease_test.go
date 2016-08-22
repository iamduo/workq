package handlers

import (
	"bytes"
	"fmt"
	"log"
	"os"
	"runtime/pprof"
	"testing"
	"time"

	"github.com/iamduo/workq/int/job"
	"github.com/iamduo/workq/int/prot"
	"github.com/iamduo/workq/int/testutil"
	"github.com/iamduo/workq/int/wqueue"
)

func TestLeaseDefault(t *testing.T) {
	reg := job.NewRegistry()
	qc := wqueue.NewController()
	handler := NewLeaseHandler(reg, qc)

	id := testutil.GenId()
	name := testutil.GenName()
	cmd := prot.NewCmd(
		"lease",
		[][]byte{
			[]byte(name),
			[]byte("10"),
		},
		prot.CmdFlags{},
	)
	resp, err := handler.Exec(cmd)
	if err != prot.ErrTimedOut {
		t.Fatalf("No work should be available yet, resp=%v, err=%v", resp, err)
	}

	j := job.NewEmptyJob()
	j.ID, _ = job.IDFromBytes([]byte(id))
	j.Name = name
	j.Payload = []byte("a")

	rec := job.NewRunRecord()
	rec.Job = j
	ok := reg.Add(rec)
	if !ok {
		t.Fatalf("Unable to add record")
	}

	ok = qc.Add(j)
	if !ok {
		t.Fatalf("Unable to add job to queue")
	}

	expResp := []byte(fmt.Sprintf(
		"OK 1\r\n%s %s %d\r\n%s\r\n",
		id,
		name,
		len(j.Payload),
		j.Payload,
	))
	resp, err = handler.Exec(cmd)
	if err != nil {
		t.Fatalf("Add err=%v", err)
	}

	if !bytes.Equal(resp, expResp) {
		t.Fatalf("Add response mismatch, resp=%s", resp)
	}

	if qc.Exists(j) {
		t.Fatalf("Job expected to be dequeued")
	}

	// Assert Attempts
	if rec.Attempts != 1 {
		t.Fatalf("Expected attempts to be 1, act=%v", rec.Attempts)
	}

	time.Sleep(1 * time.Millisecond)
	if rec.State != job.StatePending {
		t.Fatalf("Expected job to be requeued due to TTR, state=%v", rec.State)
	}

	if !qc.Exists(j) {
		t.Fatalf("Unable to find job, expected it to be requeued")
	}
}

/*func TestLeaseMaxAttempts(t *testing.T) {
	reg := job.NewRegistry()
	qc := wqueue.NewController()
	handler := NewLeaseHandler(reg, qc)

	id := testutil.GenId()
	name := testutil.GenName()
	cmd := prot.NewCmd(
		"lease",
		[][]byte{
			[]byte(name),
			[]byte("1"),
		},
		prot.CmdFlags{},
	)
	resp, err := handler.Exec(cmd)
	if err != prot.ErrTimedOut {
		t.Fatalf("No work should be available yet, resp=%v, err=%v", resp, err)
	}

	j := job.NewEmptyJob()
	j.ID, _ = job.IDFromBytes([]byte(id))
	j.Name = name
	j.Payload = []byte("a")

	rec := job.NewRunRecord()
	rec.Job = j
	ok := reg.Add(rec)
	if !ok {
		t.Fatalf("Unable to add record")
	}

	ok = qc.Add(j)
	if !ok {
		t.Fatalf("Unable to add job to queue")
	}

	expResp := []byte(fmt.Sprintf(
		"OK 1\r\n%s %s %d\r\n%s\r\n",
		id,
		name,
		len(j.Payload),
		j.Payload,
	))
	resp, err = handler.Exec(cmd)
	if err != nil {
		t.Fatalf("Add err=%v", err)
	}

	if !bytes.Equal(resp, expResp) {
		t.Fatalf("Add response mismatch, resp=%s", resp)
	}

	// Assert Attempts
	if rec.Attempts != 1 {
		t.Fatalf("Expected attempts to be 1, act=%v", rec.Attempts)
	}

	time.Sleep(1 * time.Microsecond)
	if rec.State != job.StatePending {
		t.Fatalf("Expected job to be requeued due to TTR, state=%v", rec.State)
	}

	if !qc.Exists(j) {
		t.Fatalf("Unable to find job, expected it to be requeued")
	}
}*/

func TestLeaseWaitTimeout(t *testing.T) {
	reg := job.NewRegistry()
	qc := wqueue.NewController()
	handler := NewLeaseHandler(reg, qc)

	name := testutil.GenName()
	cmd := prot.NewCmd(
		"lease",
		[][]byte{
			[]byte(name),
			[]byte("1"),
		},
		prot.CmdFlags{},
	)
	start := time.Now().UTC()
	resp, err := handler.Exec(cmd)
	diff := time.Since(start)
	if err != prot.ErrTimedOut {
		t.Fatalf("No work should be available yet, resp=%v, err=%v", resp, err)
	}

	if diff < (1*time.Millisecond) || diff >= (2*time.Millisecond) {
		t.Errorf("Run wait-timeout not in range, actual-diff=%s", diff)
	}
}

func TestLeaseMultipleNames(t *testing.T) {
	f, err := os.Create("./cpu.out")
	if err != nil {
		log.Fatal(err)
	}
	pprof.StartCPUProfile(f)
	defer pprof.StopCPUProfile()

	reg := job.NewRegistry()
	qc := wqueue.NewController()
	handler := NewLeaseHandler(reg, qc)

	/*tests := map[string]struct{
	    id
	    name string,
	    payload []byte
	  }{
	    {id: testutil.GenId(),name: testutil.GenName(), payload: []byte("a")},
	  }*/

	type testData struct {
		name    string
		payload []byte
	}
	tests := make(map[string]*testData)
	tests[testutil.GenId()] = &testData{
		name:    testutil.GenName(),
		payload: []byte("a"),
	}
	tests[testutil.GenId()] = &testData{
		name:    testutil.GenName(),
		payload: []byte("b"),
	}
	tests[testutil.GenId()] = &testData{
		name:    testutil.GenName(),
		payload: []byte("c"),
	}

	var names []string
	expResults := make(map[string][]byte, len(tests))
	for k, v := range tests {
		j := job.NewEmptyJob()
		j.ID, _ = job.IDFromBytes([]byte(k))
		j.Name = v.name
		j.Payload = v.payload
		j.TTR = 1

		rec := job.NewRunRecord()
		rec.Job = j
		ok := reg.Add(rec)
		if !ok {
			t.Fatalf("Unable to add record")
		}

		ok = qc.Add(j)
		if !ok {
			t.Fatalf("Unable to add job to queue")
		}

		names = append(names, v.name)
		expResults[k] = []byte(fmt.Sprintf("OK 1\r\n%s %s 1\r\n%s\r\n", k, v.name, v.payload))
	}

	//done := make(chan struct{}, len(names))
	for i := 0; i < len(names); i++ {
		cmd := prot.NewCmd(
			"lease",
			[][]byte{
				[]byte(names[0]),
				[]byte(names[1]),
				[]byte(names[2]),
				[]byte("10"),
			},
			prot.CmdFlags{},
		)
		resp, err := handler.Exec(cmd)
		if err != nil {
			t.Fatalf("Unexpected lease err=%v", err)
		}

		id := resp[6:42]
		expResp, ok := expResults[string(id)]
		if !ok || !bytes.Equal(expResp, resp) {
			t.Fatalf("Response mismatch, ok=%v, exp=%s, act=%s", ok, expResp, resp)
		}

		jid, _ := job.IDFromBytes(id)

		rec, ok := reg.Record(jid)
		if !ok {
			t.Fatalf("Run record missing")
		}

		if qc.Exists(rec.Job) {
			t.Fatalf("Job expected to be dequeued")
		}

		// Assert Attempts
		if rec.Attempts != 1 {
			t.Fatalf("Expected attempts to be 1, id=%s, attempts=%v", rec.Job.ID, rec.Attempts)
		}

		// Small bit of padding for test overhead
		// TODO Perf issue here. Running lower than 40ms causes requeue failure
		time.Sleep(2 * time.Millisecond)
		//defer func() { done <- struct{}{} }()
		if rec.State != job.StatePending {
			t.Fatalf("Expected job to be requeued due to TTR, state=%v", rec.State)
		}

		if !qc.Exists(rec.Job) {
			t.Fatalf("Unable to find job, expected it to be requeued")
		}

		rec.State = job.StateCompleted
		qc.Delete(rec.Job)
	}

	/*for i := 0; i < len(names); i++ {
		<-done
	}*/
}

func TestLeaseMissingRunRecord(t *testing.T) {
	reg := job.NewRegistry()
	qc := wqueue.NewController()
	handler := NewLeaseHandler(reg, qc)

	id := testutil.GenId()
	name := testutil.GenName()
	cmd := prot.NewCmd(
		"lease",
		[][]byte{
			[]byte(name),
			[]byte("1"),
		},
		prot.CmdFlags{},
	)

	j := job.NewEmptyJob()
	j.ID, _ = job.IDFromBytes([]byte(id))
	j.Name = name
	j.Payload = []byte("a")

	if !qc.Add(j) {
		t.Fatalf("Unable to add job to queue")
	}

	expErr := prot.NewServerErr("Job record unavailable")
	resp, err := handler.Exec(cmd)
	if err.Error() != expErr.Error() {
		t.Fatalf("Lease err mismatch, err=%v", err)
	}

	if resp != nil {
		t.Fatalf("Lease response mismatch, resp=%s", resp)
	}
}

func TestLeaseAlreadyRunningJob(t *testing.T) {
	reg := job.NewRegistry()
	qc := wqueue.NewController()
	handler := NewLeaseHandler(reg, qc)

	id := testutil.GenId()
	name := testutil.GenName()
	cmd := prot.NewCmd(
		"lease",
		[][]byte{
			[]byte(name),
			[]byte("1"),
		},
		prot.CmdFlags{},
	)

	j := job.NewEmptyJob()
	j.ID, _ = job.IDFromBytes([]byte(id))
	j.Name = name
	j.Payload = []byte("a")

	rec := job.NewRunRecord()
	rec.Job = j
	rec.State = job.StateLeased
	if !reg.Add(rec) {
		t.Fatalf("Unable to add record")
	}

	if !qc.Add(j) {
		t.Fatalf("Unable to add job to queue")
	}

	expErr := prot.NewClientErr("Job already leased")
	resp, err := handler.Exec(cmd)
	if err.Error() != expErr.Error() {
		t.Fatalf("Lease err mismatch, err=%v", err)
	}

	if resp != nil {
		t.Fatalf("Lease response mismatch, resp=%s", resp)
	}
}

func TestLeaseAlreadyProcessedJob(t *testing.T) {
	reg := job.NewRegistry()
	qc := wqueue.NewController()
	handler := NewLeaseHandler(reg, qc)

	id := testutil.GenId()
	name := testutil.GenName()
	cmd := prot.NewCmd(
		"lease",
		[][]byte{
			[]byte(name),
			[]byte("1"),
		},
		prot.CmdFlags{},
	)

	j := job.NewEmptyJob()
	j.ID, _ = job.IDFromBytes([]byte(id))
	j.Name = name
	j.Payload = []byte("a")

	rec := job.NewRunRecord()
	rec.Job = j
	rec.State = job.StateCompleted
	if !reg.Add(rec) {
		t.Fatalf("Unable to add record")
	}

	if !qc.Add(j) {
		t.Fatalf("Unable to add job to queue")
	}

	expErr := prot.NewClientErr("Job already processed")
	resp, err := handler.Exec(cmd)
	if err.Error() != expErr.Error() {
		t.Fatalf("Lease err mismatch, err=%v", err)
	}

	if resp != nil {
		t.Fatalf("Lease response mismatch, resp=%s", resp)
	}
}

func TestLeaseMaxAttempts(t *testing.T) {
	reg := job.NewRegistry()
	qc := wqueue.NewController()
	handler := NewLeaseHandler(reg, qc)

	tests := []struct {
		MaxAttempts uint8
		Attempts    uint64
	}{
		{MaxAttempts: 3, Attempts: 3},
		{MaxAttempts: job.MaxHardAttempts, Attempts: job.MaxAttempts},
		{MaxAttempts: 3, Attempts: job.MaxAttempts},
	}

	for _, tt := range tests {
		id := testutil.GenId()
		name := testutil.GenName()
		cmd := prot.NewCmd(
			"lease",
			[][]byte{
				[]byte(name),
				[]byte("1"),
			},
			prot.CmdFlags{},
		)

		j := job.NewEmptyJob()
		j.ID, _ = job.IDFromBytes([]byte(id))
		j.Name = name
		j.Payload = []byte("a")
		j.MaxAttempts = tt.MaxAttempts

		rec := job.NewRunRecord()
		rec.Job = j
		rec.Attempts = tt.Attempts
		if !reg.Add(rec) {
			t.Fatalf("Unable to add record")
		}
		if !qc.Add(j) {
			t.Fatalf("Unable to add job to queue")
		}

		expErr := prot.NewClientErr("Attempts max reached")
		resp, err := handler.Exec(cmd)
		if err.Error() != expErr.Error() {
			t.Fatalf("Lease err mismatch, err=%v", err)
		}

		if resp != nil {
			t.Fatalf("Lease response mismatch, resp=%s", resp)
		}
	}
}

func TestLeaseTTRTimerCancelled(t *testing.T) {
	reg := job.NewRegistry()
	qc := wqueue.NewController()
	handler := NewLeaseHandler(reg, qc)

	id := testutil.GenId()
	name := testutil.GenName()
	cmd := prot.NewCmd(
		"lease",
		[][]byte{
			[]byte(name),
			[]byte("1"),
		},
		prot.CmdFlags{},
	)

	j := job.NewEmptyJob()
	j.ID, _ = job.IDFromBytes([]byte(id))
	j.Name = name
	j.Payload = []byte("a")
	j.TTR = 10

	rec := job.NewRunRecord()
	rec.Job = j
	if !reg.Add(rec) {
		t.Fatalf("Unable to add record")
	}

	if !qc.Add(j) {
		t.Fatalf("Unable to add job to queue")
	}

	expResp := []byte(fmt.Sprintf(
		"OK 1\r\n%s %s %d\r\n%s\r\n",
		id,
		name,
		len(j.Payload),
		j.Payload,
	))
	resp, err := handler.Exec(cmd)
	if err != nil {
		t.Fatalf("Lease err=%v", err)
	}

	if !bytes.Equal(resp, expResp) {
		t.Fatalf("Lease response mismatch, resp=%s", resp)
	}

	if qc.Exists(j) {
		t.Fatalf("Job expected to be dequeued")
	}

	rec.Mu.RLock()
	rec.Timers[job.RunRecTTRTimerIdx].Cancel()
	rec.Mu.RUnlock()
}

func TestLeaseInternalTimeoutAttempt(t *testing.T) {
	reg := job.NewRegistry()
	qc := wqueue.NewController()
	handler := NewLeaseHandler(reg, qc)

	id := testutil.GenId()
	name := testutil.GenName()

	j := job.NewEmptyJob()
	j.ID, _ = job.IDFromBytes([]byte(id))
	j.Name = name
	j.Payload = []byte("a")
	j.TTR = 10

	rec := job.NewRunRecord()
	rec.Job = j
	if !reg.Add(rec) {
		t.Fatalf("Unable to add record")
	}

	if !qc.Add(j) {
		t.Fatalf("Unable to add job to queue")
	}

	rec.State = job.StateCompleted
	copy := *rec
	handler.timeoutAttempt(rec.Job.ID)
	if !compareRunRecord(&copy, rec) {
		t.Fatalf("timeoutAttempt expected to be NOOP")
	}

	rec.State = job.StateLeased
	rec.Job.MaxAttempts = 3
	rec.Attempts = 3
	handler.timeoutAttempt(rec.Job.ID)
	if rec.State != job.StateFailed {
		t.Fatalf("Expected timeoutAttempt to fail job, state=%v", rec.State)
	}

	rec.State = job.StateNew
	rec.Attempts = 0
	qc.Delete(rec.Job)
	reg.Delete(rec.Job.ID)
	handler.timeoutAttempt(rec.Job.ID)
	if qc.Exists(rec.Job) {
		t.Fatalf("Expected timeoutAttempt to NOOP")
	}
}

func TestLeaseInvalidArgs(t *testing.T) {
	reg := job.NewRegistry()
	qc := wqueue.NewController()
	handler := NewLeaseHandler(reg, qc)

	tests := []struct {
		cmd  *prot.Cmd
		resp []byte
		err  error
	}{
		{
			prot.NewCmd(
				"lease",
				[][]byte{},
				prot.CmdFlags{},
			),
			nil,
			prot.ErrInvalidCmdArgs,
		},
		{
			prot.NewCmd(
				"lease",
				[][]byte{
					[]byte("q1"),
				},
				prot.CmdFlags{},
			),
			nil,
			prot.ErrInvalidCmdArgs,
		},
		{
			prot.NewCmd(
				"not-lease",
				[][]byte{
					[]byte("q1"),
					[]byte("1"),
				},
				prot.CmdFlags{},
			),
			nil,
			prot.ErrInvalidCmdArgs,
		},
		{
			prot.NewCmd(
				"lease",
				[][]byte{
					[]byte("q1"),
					[]byte("-1"),
				},
				prot.CmdFlags{},
			),
			nil,
			ErrInvalidWaitTimeout,
		},
		{
			prot.NewCmd(
				"lease",
				[][]byte{
					[]byte("*"),
					[]byte("1"),
				},
				prot.CmdFlags{},
			),
			nil,
			prot.NewClientErr(job.ErrInvalidName.Error()),
		},
	}

	for _, tt := range tests {
		resp, err := handler.Exec(tt.cmd)
		if !bytes.Equal(tt.resp, resp) {
			t.Fatalf("response mismatch, expResp=%v, actResp=%v", tt.resp, resp)
		}

		if err.Error() != tt.err.Error() {
			t.Fatalf("err mismatch, expErr=%v, actErr=%v", tt.err, err)
		}
	}
}

func compareRunRecord(a *job.RunRecord, b *job.RunRecord) bool {
	return a.Attempts == b.Attempts &&
		a.State == b.State &&
		a.Fails == b.Fails &&
		a.Job == b.Job &&
		bytes.Equal(a.Result, b.Result)
}
