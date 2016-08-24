package handlers

import (
	"bytes"
	"fmt"
	"testing"
	"time"

	"github.com/iamduo/workq/int/job"
	"github.com/iamduo/workq/int/prot"
	"github.com/iamduo/workq/int/testutil"
	"github.com/iamduo/workq/int/wqueue"
)

func TestRunWaitTimeout(t *testing.T) {
	id := []byte(testutil.GenId())
	jid, _ := job.IDFromBytes(id)
	name := testutil.GenName()
	tests := []struct {
		expJob     *job.Job
		expTimeout int
		cmd        *prot.Cmd
	}{
		{
			expJob: &job.Job{
				ID:          jid,
				Name:        name,
				Payload:     []byte("a"),
				MaxAttempts: 0,
				MaxFails:    0,
				TTR:         5,
				Created:     time.Now().UTC(),
			},
			expTimeout: 10,
			cmd: prot.NewCmd(
				"run",
				[][]byte{
					id,
					[]byte(name),
					[]byte("5"),
					[]byte("10"),
					[]byte("1"),
					[]byte("a"),
				},
				prot.CmdFlags{},
			),
		},
		{
			expJob: &job.Job{
				ID:          jid,
				Name:        name,
				Payload:     []byte("a"),
				MaxAttempts: 0,
				MaxFails:    0,
				Priority:    3,
				TTR:         5,
				Created:     time.Now().UTC(),
			},
			expTimeout: 10,
			cmd: prot.NewCmd(
				"run",
				[][]byte{
					id,
					[]byte(name),
					[]byte("5"),
					[]byte("10"),
					[]byte("1"),
					[]byte("a"),
				},
				prot.CmdFlags{"priority": []byte("3")},
			),
		},
	}

	for _, tt := range tests {
		reg := job.NewRegistry()
		qc := wqueue.NewController()
		handler := NewRunHandler(reg, qc, &Usage{})

		tt.expJob.Created = time.Now().UTC()
		done := make(chan struct{})
		go func() {
			defer close(done)
			start := time.Now()
			resp, err := handler.Exec(tt.cmd)
			if err != prot.ErrTimedOut || resp != nil {
				t.Fatalf("Run expected to time out, resp=%v, err=%v", resp, err)
			}
			diff := time.Now().Sub(start)
			// 10ms padding for test load overhead.
			if diff <= time.Duration(tt.expTimeout) || diff >= time.Duration(tt.expTimeout+10)*time.Millisecond {
				t.Fatalf("Unexpected timeout length, diff=%s", diff)
			}
		}()

		// Delay for run exec above
		time.Sleep(500 * time.Microsecond)
		rec, ok := reg.Record(jid)
		if !ok {
			t.Fatalf("Expected record to exist")
		}

		rec.Mu.RLock()
		actJob := rec.Job
		if !qc.Exists(actJob) {
			t.Fatalf("Expected job to be exist")
		}

		expRec := job.NewRunRecord()
		expRec.State = job.StateNew
		assertRunRecords(t, expRec, rec)
		rec.Mu.RUnlock()

		assertJobs(t, tt.expJob, actJob)
		<-done

		if qc.Exists(actJob) {
			t.Fatalf("Expected job to be expired")
		}
	}
}

func TestRunResult(t *testing.T) {
	reg := job.NewRegistry()
	qc := wqueue.NewController()
	handler := NewRunHandler(reg, qc, &Usage{})

	id := []byte(testutil.GenId())
	jid, _ := job.IDFromBytes(id)
	expResult := "b"
	expJob := &job.Job{
		ID:      jid,
		Name:    testutil.GenName(),
		Payload: []byte("a"),
		TTR:     5,
		Created: time.Now().UTC(),
	}
	done := make(chan struct{}, 1)
	go func() {
		defer close(done)
		expResp := []byte(fmt.Sprintf(
			"OK 1\r\n%s 1 %d\r\n%s\r\n",
			id,
			len(expResult),
			expResult,
		))
		cmd := prot.NewCmd(
			"run",
			[][]byte{
				id,
				[]byte(expJob.Name),
				[]byte("5"),
				[]byte("10"),
				[]byte(fmt.Sprintf("%d", len(expJob.Payload))),
				[]byte(expJob.Payload),
			},
			prot.CmdFlags{},
		)
		resp, err := handler.Exec(cmd)
		if err != nil {
			t.Fatalf("Run err=%v", err)
		}
		if !bytes.Equal(resp, expResp) {
			t.Fatalf("Run response mismatch, resp=%s", resp)
		}
	}()

	// Delay for run exec above
	time.Sleep(500 * time.Microsecond)

	rec, ok := reg.Record(jid)
	if !ok {
		t.Fatalf("Expected record to exist")
	}

	rec.Mu.RLock()
	actJob := rec.Job
	if !qc.Exists(actJob) {
		t.Fatalf("Expected job to be exist")
	}

	expRec := job.NewRunRecord()
	expRec.State = job.StateNew
	assertRunRecords(t, expRec, rec)
	rec.Mu.RUnlock()

	assertJobs(t, expJob, actJob)

	rec.Mu.Lock()
	rec.State = job.StateCompleted
	rec.WriteResult([]byte(expResult), true)
	rec.Mu.Unlock()
	<-done
}

// Test result after wait-timeout, but before ttr
func TestRunResultAfterWaitTimeout(t *testing.T) {
	reg := job.NewRegistry()
	qc := wqueue.NewController()
	handler := NewRunHandler(reg, qc, &Usage{})

	id := []byte(testutil.GenId())
	jid, _ := job.IDFromBytes(id)
	expResult := "b"
	expJob := &job.Job{
		ID:      jid,
		Name:    testutil.GenName(),
		Payload: []byte("a"),
		TTR:     100,
		Created: time.Now().UTC(),
	}

	done := make(chan struct{}, 1)
	go func() {
		defer close(done)

		expResp := []byte(fmt.Sprintf(
			"OK 1\r\n%s 1 %d\r\n%s\r\n",
			id,
			len(expResult),
			expResult,
		))
		cmd := prot.NewCmd(
			"run",
			[][]byte{
				id,
				[]byte(expJob.Name),
				[]byte("100"),
				[]byte("10"),
				[]byte(fmt.Sprintf("%d", len(expJob.Payload))),
				[]byte(expJob.Payload),
			},
			prot.CmdFlags{},
		)
		resp, err := handler.Exec(cmd)
		if err != nil {
			t.Fatalf("Run err=%v", err)
		}
		if !bytes.Equal(resp, expResp) {
			t.Fatalf("Run response mismatch, resp=%s", resp)
		}
	}()

	// Delay for run exec above
	time.Sleep(500 * time.Microsecond)

	rec, ok := reg.Record(jid)
	if !ok {
		t.Fatalf("Expected record to exist")
	}

	rec.Mu.RLock()
	actJob := rec.Job
	if !qc.Exists(actJob) {
		t.Fatalf("Expected job to be exist")
	}

	expRec := job.NewRunRecord()
	expRec.State = job.StateNew
	assertRunRecords(t, expRec, rec)
	rec.Mu.RUnlock()

	assertJobs(t, expJob, actJob)

	rec.Mu.Lock()
	rec.State = job.StateLeased
	rec.Timers[job.RunRecTTRTimerIdx] = job.NewTimer(time.Duration(expJob.TTR) * time.Millisecond)
	rec.Mu.Unlock()

	// Sleep after wait-timeout
	time.Sleep(10 * time.Millisecond)
	rec.Mu.Lock()
	rec.State = job.StateCompleted
	rec.WriteResult([]byte(expResult), true)
	rec.Mu.Unlock()

	<-done
}

func TestRunTTRTimeout(t *testing.T) {
	reg := job.NewRegistry()
	qc := wqueue.NewController()
	handler := NewRunHandler(reg, qc, &Usage{})

	id := []byte(testutil.GenId())
	jid, _ := job.IDFromBytes(id)
	expJob := &job.Job{
		ID:      jid,
		Name:    testutil.GenName(),
		Payload: []byte("a"),
		TTR:     2,
		Created: time.Now().UTC(),
	}

	done := make(chan struct{}, 1)
	go func() {
		defer close(done)

		cmd := prot.NewCmd(
			"run",
			[][]byte{
				id,
				[]byte(expJob.Name),
				[]byte("2"),
				[]byte("10"),
				[]byte(fmt.Sprintf("%d", len(expJob.Payload))),
				[]byte(expJob.Payload),
			},
			prot.CmdFlags{},
		)

		resp, err := handler.Exec(cmd)
		if err != prot.ErrTimedOut || resp != nil {
			t.Fatalf("Run response mismatch, resp=%v, err=%v", resp, err)
		}
	}()

	// Pick up work before wait-timeout
	time.Sleep(500 * time.Microsecond)

	rec, ok := reg.Record(jid)
	if !ok {
		t.Fatalf("Expected record to exist")
	}

	rec.Mu.RLock()
	actJob := rec.Job
	if !qc.Exists(actJob) {
		t.Fatalf("Expected job to be exist")
	}

	expRec := job.NewRunRecord()
	expRec.Job = expJob
	expRec.State = job.StateNew
	assertRunRecords(t, expRec, rec)
	rec.Mu.RUnlock()

	assertJobs(t, expJob, actJob)

	rec.Mu.Lock()
	rec.State = job.StateLeased
	rec.Timers[job.RunRecTTRTimerIdx] = job.NewTimer(2 * time.Millisecond)
	rec.Mu.Unlock()

	// Sleep after wait-timeout + ttr
	time.Sleep(time.Millisecond * 2)
	<-done
}

// Tests to ensure TTR timeout when TTR timer is cleared
func TestRunTTRTimeoutNoTimer(t *testing.T) {
	reg := job.NewRegistry()
	qc := wqueue.NewController()
	handler := NewRunHandler(reg, qc, &Usage{})

	id := []byte(testutil.GenId())
	jid, _ := job.IDFromBytes(id)
	expJob := &job.Job{
		ID:      jid,
		Name:    testutil.GenName(),
		Payload: []byte("a"),
		TTR:     2,
		Created: time.Now().UTC(),
	}

	done := make(chan struct{}, 1)
	go func() {
		defer close(done)

		cmd := prot.NewCmd(
			"run",
			[][]byte{
				id,
				[]byte(expJob.Name),
				[]byte("2"),
				[]byte("10"),
				[]byte(fmt.Sprintf("%d", len(expJob.Payload))),
				[]byte(expJob.Payload),
			},
			prot.CmdFlags{},
		)

		resp, err := handler.Exec(cmd)
		if err != prot.ErrTimedOut || resp != nil {
			t.Fatalf("Run response mismatch, resp=%v, err=%v", resp, err)
		}
	}()

	// Pick up work before wait-timeout
	time.Sleep(500 * time.Microsecond)

	rec, ok := reg.Record(jid)
	if !ok {
		t.Fatalf("Expected record to exist")
	}

	rec.Mu.RLock()
	actJob := rec.Job
	if !qc.Exists(actJob) {
		t.Fatalf("Expected job to be exist")
	}

	expRec := job.NewRunRecord()
	expRec.Job = expJob
	expRec.State = job.StateNew
	assertRunRecords(t, expRec, rec)
	rec.Mu.RUnlock()

	assertJobs(t, expJob, actJob)

	rec.Mu.Lock()
	rec.State = job.StateLeased
	rec.Timers[job.RunRecTTRTimerIdx] = nil
	rec.Mu.Unlock()

	// Sleep after wait-timeout + ttr
	time.Sleep(time.Millisecond * 2)
	<-done
}

func TestRunDuplicate(t *testing.T) {
	reg := job.NewRegistry()
	qc := wqueue.NewController()
	handler := NewRunHandler(reg, qc, &Usage{})

	id := []byte(testutil.GenId())
	jid, _ := job.IDFromBytes(id)
	expJob := &job.Job{
		ID:      jid,
		Name:    testutil.GenName(),
		Payload: []byte("a"),
		TTR:     5,
		Created: time.Now().UTC(),
	}

	done := make(chan struct{}, 2)
	cmd := prot.NewCmd(
		"run",
		[][]byte{
			id,
			[]byte(expJob.Name),
			[]byte("5"),
			[]byte("10"),
			[]byte(fmt.Sprintf("%d", len(expJob.Payload))),
			[]byte(expJob.Payload),
		},
		prot.CmdFlags{},
	)
	go func() {
		defer func() { done <- struct{}{} }()

		resp, err := handler.Exec(cmd)
		if err != prot.ErrTimedOut || resp != nil {
			t.Fatalf("Run response mismatch, resp=%v, err=%v", resp, err)
		}
	}()
	time.Sleep(250 * time.Microsecond)
	go func() {
		defer func() { done <- struct{}{} }()

		resp, err := handler.Exec(cmd)
		if err != ErrDuplicateJob || resp != nil {
			t.Fatalf("Run response mismatch, resp=%v, err=%v", resp, err)
		}
	}()

	// Sleep after wait-timeout
	time.Sleep(time.Millisecond * 10)
	<-done
	<-done
}

// This is not expected during normal use. Testing for coverage.
func TestRunOutofSyncQueue(t *testing.T) {

	reg := job.NewRegistry()
	qc := &OutOfSyncController{}

	handler := NewRunHandler(reg, qc, &Usage{})

	id := testutil.GenId()
	name := testutil.GenName()
	expPayload := "a"

	cmd := prot.NewCmd(
		"run",
		[][]byte{
			[]byte(id),
			[]byte(name),
			[]byte("2"),
			[]byte("1"),
			[]byte(fmt.Sprintf("%d", len(expPayload))),
			[]byte(expPayload),
		},
		prot.CmdFlags{},
	)

	expErr := prot.NewServerErr("Unable to run")
	resp, err := handler.Exec(cmd)
	if err.Error() != expErr.Error() || resp != nil {
		t.Fatalf("Run response mismatch, resp=%v, err=%v", resp, err)
	}
}

func TestRunInvalidArgs(t *testing.T) {
	reg := job.NewRegistry()
	qc := wqueue.NewController()
	handler := NewRunHandler(reg, qc, &Usage{})

	tests := []struct {
		cmd  *prot.Cmd
		resp []byte
		err  error
	}{
		{
			prot.NewCmd(
				"run",
				[][]byte{
					[]byte("123"),
				},
				prot.CmdFlags{},
			),
			nil,
			prot.ErrInvalidCmdArgs,
		},
		{
			prot.NewCmd(
				"run",
				[][]byte{
					[]byte("123"),
					[]byte("q1"),
				},
				prot.CmdFlags{},
			),
			nil,
			prot.ErrInvalidCmdArgs,
		},
		{
			prot.NewCmd(
				"run",
				[][]byte{
					[]byte("123"),
					[]byte("q1"),
					[]byte("10"),
				},
				prot.CmdFlags{},
			),
			nil,
			prot.ErrInvalidCmdArgs,
		},
		{
			prot.NewCmd(
				"run",
				[][]byte{
					[]byte("123"),
					[]byte("q1"),
					[]byte("10"),
					[]byte("1000"),
					[]byte("1"),
				},
				prot.CmdFlags{},
			),
			nil,
			prot.ErrInvalidCmdArgs,
		},
		{
			prot.NewCmd(
				"run",
				[][]byte{
					[]byte("*"),
					[]byte("q1"),
					[]byte("10"),
					[]byte("1000"),
					[]byte("1"),
					[]byte("a"),
				},
				prot.CmdFlags{},
			),
			nil,
			prot.NewClientErr(job.ErrInvalidID.Error()),
		},
		{
			prot.NewCmd(
				"run",
				[][]byte{
					[]byte("00000000-0000-0000-0000-000000000000"),
					[]byte("*"),
					[]byte("10"),
					[]byte("1000"),
					[]byte("1"),
					[]byte("a"),
				},
				prot.CmdFlags{},
			),
			nil,
			prot.NewClientErr(job.ErrInvalidName.Error()),
		},
		{
			prot.NewCmd(
				"run",
				[][]byte{
					[]byte("00000000-0000-0000-0000-000000000000"),
					[]byte("q1"),
					[]byte("-10"),
					[]byte("1000"),
					[]byte("1"),
					[]byte("a"),
				},
				prot.CmdFlags{},
			),
			nil,
			prot.NewClientErr(job.ErrInvalidTTR.Error()),
		},
		{
			prot.NewCmd(
				"run",
				[][]byte{
					[]byte("00000000-0000-0000-0000-000000000000"),
					[]byte("q1"),
					[]byte("10"),
					[]byte("-1000"),
					[]byte("1"),
					[]byte("a"),
				},
				prot.CmdFlags{},
			),
			nil,
			ErrInvalidWaitTimeout,
		},
		{
			prot.NewCmd(
				"run",
				[][]byte{
					[]byte("00000000-0000-0000-0000-000000000000"),
					[]byte("q1"),
					[]byte("10"),
					[]byte("1000"),
					[]byte("1"),
					[]byte("a"),
				},
				prot.CmdFlags{"priority": []byte("*")},
			),
			nil,
			prot.NewClientErr(job.ErrInvalidPriority.Error()),
		},
		{
			prot.NewCmd(
				"run",
				[][]byte{
					[]byte("00000000-0000-0000-0000-000000000000"),
					[]byte("q1"),
					[]byte("10"),
					[]byte("1000"),
					[]byte("1"),
					[]byte("aa"),
				},
				prot.CmdFlags{},
			),
			nil,
			prot.NewClientErr(job.ErrInvalidPayload.Error()),
		},
		{
			prot.NewCmd(
				"run",
				[][]byte{
					[]byte("00000000-0000-0000-0000-000000000000"),
					[]byte("q1"),
					[]byte("10"),
					[]byte("1000"),
					[]byte(fmt.Sprintf("%d", job.MaxPayload+1)),
					make([]byte, job.MaxPayload+1),
				},
				prot.CmdFlags{},
			),
			nil,
			prot.NewClientErr(job.ErrInvalidPayload.Error()),
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

		if len(tt.cmd.Args) < 2 {
			continue
		}

		queue := qc.Queue(string(tt.cmd.Args[1]))
		if reg.Len() != 0 || queue.Len() != 0 {
			t.Fatalf("Expected no jobs to be enqueued")
		}
	}
}
