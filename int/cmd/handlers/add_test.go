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

func TestAdd(t *testing.T) {
	id := []byte(testutil.GenId())
	jid, _ := job.IDFromBytes(id)
	name := testutil.GenName()
	tests := []struct {
		expJob *job.Job
		cmd    *prot.Cmd
	}{
		{
			expJob: &job.Job{
				ID:      jid,
				Name:    name,
				Payload: []byte(""),
				TTR:     1,
				TTL:     10,
			},
			cmd: prot.NewCmd(
				"add",
				[][]byte{
					id,
					[]byte(name),
					[]byte("1"),
					[]byte("10"),
					[]byte("0"),
					[]byte(""),
				},
				prot.CmdFlags{},
			),
		},
		{
			expJob: &job.Job{
				ID:      jid,
				Name:    name,
				Payload: []byte("a"),
				TTR:     1,
				TTL:     10,
			},
			cmd: prot.NewCmd(
				"add",
				[][]byte{
					id,
					[]byte(name),
					[]byte("1"),
					[]byte("10"),
					[]byte("1"),
					[]byte("a"),
				},
				prot.CmdFlags{},
			),
		},
		{
			expJob: &job.Job{
				ID:       jid,
				Name:     name,
				Payload:  []byte("a"),
				Priority: 10,
				TTR:      1,
				TTL:      10,
			},
			cmd: prot.NewCmd(
				"add",
				[][]byte{
					id,
					[]byte(name),
					[]byte("1"),
					[]byte("10"),
					[]byte("1"),
					[]byte("a"),
				},
				prot.CmdFlags{
					"priority": []byte("10"),
				},
			),
		},
		{
			expJob: &job.Job{
				ID:          jid,
				Name:        name,
				Payload:     []byte("a"),
				MaxAttempts: 3,
				TTR:         1,
				TTL:         10,
			},
			cmd: prot.NewCmd(
				"add",
				[][]byte{
					id,
					[]byte(name),
					[]byte("1"),
					[]byte("10"),
					[]byte("1"),
					[]byte("a"),
				},
				prot.CmdFlags{
					"max-attempts": []byte("3"),
				},
			),
		},
		{
			expJob: &job.Job{
				ID:       jid,
				Name:     name,
				Payload:  []byte("a"),
				MaxFails: 3,
				TTR:      1,
				TTL:      10,
			},
			cmd: prot.NewCmd(
				"add",
				[][]byte{
					id,
					[]byte(name),
					[]byte("1"),
					[]byte("10"),
					[]byte("1"),
					[]byte("a"),
				},
				prot.CmdFlags{
					"max-fails": []byte("3"),
				},
			),
		},
		{
			expJob: &job.Job{
				ID:          jid,
				Name:        name,
				Payload:     []byte("a"),
				Priority:    10,
				MaxAttempts: 9,
				MaxFails:    3,
				TTR:         1,
				TTL:         10,
			},
			cmd: prot.NewCmd(
				"add",
				[][]byte{
					id,
					[]byte(name),
					[]byte("1"),
					[]byte("10"),
					[]byte("1"),
					[]byte("a"),
				},
				prot.CmdFlags{
					"priority":     []byte("10"),
					"max-attempts": []byte("9"),
					"max-fails":    []byte("3"),
				},
			),
		},
		// Test for MAX payload
		{
			expJob: &job.Job{
				ID:          jid,
				Name:        name,
				Payload:     make([]byte, 1048576),
				MaxAttempts: 9,
				MaxFails:    3,
				TTR:         1,
				TTL:         10,
			},
			cmd: prot.NewCmd(
				"add",
				[][]byte{
					id,
					[]byte(name),
					[]byte("1"),
					[]byte("10"),
					[]byte("1048576"),
					make([]byte, 1048576),
				},
				prot.CmdFlags{
					"max-attempts": []byte("9"),
					"max-fails":    []byte("3"),
				},
			),
		},
	}

	for _, tt := range tests {
		tt.expJob.Created = time.Now().UTC()

		reg := job.NewRegistry()
		qc := wqueue.NewController()
		handler := NewAddHandler(reg, qc, &Usage{})

		_, ok := reg.Record(jid)
		if ok {
			t.Fatalf("Expected no record to exist")
		}

		expRec := job.NewRunRecord()
		expRec.State = job.StateNew
		expRec.Job = tt.expJob

		expResp := []byte("OK\r\n")
		resp, err := handler.Exec(tt.cmd)
		if err != nil {
			t.Fatalf("Add err=%v", err)
		}

		if !bytes.Equal(resp, expResp) {
			t.Fatalf("Add response mismatch, resp=%s", resp)
		}

		rec, ok := reg.Record(jid)
		if !ok {
			t.Fatalf("Expected record to exist")
		}

		rec.Mu.RLock()
		assertRunRecords(t, expRec, rec)
		actJob := rec.Job
		rec.Mu.RUnlock()
		assertJobs(t, tt.expJob, actJob)

		// Sleep additional 10% of TTL for test load overhead
		time.Sleep(time.Duration(tt.expJob.TTL) * time.Millisecond)
		time.Sleep(time.Duration(tt.expJob.TTL/10) * time.Millisecond)
		if qc.Exists(actJob) {
			t.Fatalf("Expected job to be expired")
		}

		_, ok = reg.Record(jid)
		if ok {
			t.Fatalf("Expected record to be expired")
		}
	}
}

func TestAddDuplicate(t *testing.T) {
	reg := job.NewRegistry()
	qc := wqueue.NewController()
	handler := NewAddHandler(reg, qc, &Usage{})

	id := []byte(testutil.GenId())
	jid, _ := job.IDFromBytes(id)
	name := testutil.GenName()

	expJob := &job.Job{
		ID:          jid,
		Name:        name,
		Payload:     []byte("a"),
		MaxAttempts: 0,
		MaxFails:    0,
		TTR:         100,
		TTL:         1000,
		Created:     time.Now().UTC(),
	}
	cmd := prot.NewCmd(
		"add",
		[][]byte{
			id,
			[]byte(name),
			[]byte("100"),
			[]byte("1000"),
			[]byte("1"),
			[]byte("a"),
		},
		prot.CmdFlags{},
	)

	expResp := []byte("OK\r\n")
	resp, err := handler.Exec(cmd)
	if err != nil {
		t.Fatalf("Add err=%v", err)
	}

	if !bytes.Equal(resp, expResp) {
		t.Fatalf("Add response mismatch, resp=%s", resp)
	}

	rec, ok := reg.Record(jid)
	if !ok {
		t.Fatalf("Expected no record to exist")
	}

	rec.Mu.RLock()
	actJob := rec.Job
	assertNewRunRecord(t, rec)
	rec.Mu.RUnlock()

	assertJobs(t, expJob, actJob)

	resp, err = handler.Exec(cmd)
	if err != ErrDuplicateJob || resp != nil {
		t.Fatalf("Add response mismatch, resp=%v, err=%v", resp, err)
	}
}

func TestAddCancelledTTLTimer(t *testing.T) {
	reg := job.NewRegistry()
	qc := wqueue.NewController()
	handler := NewAddHandler(reg, qc, &Usage{})

	id := []byte(testutil.GenId())
	jid, _ := job.IDFromBytes(id)
	name := testutil.GenName()

	expJob := &job.Job{
		ID:          jid,
		Name:        name,
		Payload:     []byte("a"),
		MaxAttempts: 0,
		MaxFails:    0,
		TTR:         100,
		TTL:         1000,
		Created:     time.Now().UTC(),
	}
	cmd := prot.NewCmd(
		"add",
		[][]byte{
			id,
			[]byte(name),
			[]byte("100"),
			[]byte("1000"),
			[]byte("1"),
			[]byte("a"),
		},
		prot.CmdFlags{},
	)

	expResp := []byte("OK\r\n")
	resp, err := handler.Exec(cmd)
	if err != nil {
		t.Fatalf("Add err=%v", err)
	}

	if !bytes.Equal(resp, expResp) {
		t.Fatalf("Add response mismatch, resp=%s", resp)
	}

	rec, ok := reg.Record(jid)
	if !ok {
		t.Fatalf("Expected record to exist")
	}

	rec.Mu.Lock()
	actJob := rec.Job
	assertNewRunRecord(t, rec)
	assertJobs(t, expJob, actJob)

	rec.Timers[job.RunRecTTLTimerIdx].Cancel()
	rec.Mu.Unlock()

	// Wait for TTL and ensure job still exists
	// 10% TTL padding for test overhead
	time.Sleep(time.Duration(expJob.TTL+(expJob.TTL/10)) * time.Millisecond)

	if qc.Add(actJob) {
		t.Fatalf("Job seems to have expired after TTL cancellation")
	}

	_, ok = reg.Record(jid)
	if !ok {
		t.Fatalf("Unable to find record after TTL cancellation")
	}
}

// This is not expected during normal use. Testing for coverage.
func TestAddOutofSyncQueue(t *testing.T) {

	reg := job.NewRegistry()
	qc := &OutOfSyncController{}
	handler := NewAddHandler(reg, qc, &Usage{})

	cmd := prot.NewCmd(
		"add",
		[][]byte{
			[]byte(testutil.GenId()),
			[]byte(testutil.GenName()),
			[]byte("2"),
			[]byte("1"),
			[]byte("1"),
			[]byte("a"),
		},
		prot.CmdFlags{},
	)

	expErr := prot.NewServerErr("Unable to add job")
	resp, err := handler.Exec(cmd)
	if err.Error() != expErr.Error() || resp != nil {
		t.Fatalf("Run response mismatch, resp=%v, err=%v", resp, err)
	}
}

func TestAddInvalidArgs(t *testing.T) {
	reg := job.NewRegistry()
	qc := wqueue.NewController()
	handler := NewAddHandler(reg, qc, &Usage{})

	tests := []struct {
		cmd  *prot.Cmd
		resp []byte
		err  error
	}{
		{
			prot.NewCmd(
				"add",
				[][]byte{},
				prot.CmdFlags{},
			),
			nil,
			prot.ErrInvalidCmdArgs,
		},
		{
			prot.NewCmd(
				"add",
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
				"add",
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
				"add",
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
				"add",
				[][]byte{
					[]byte("123"),
					[]byte("q1"),
					[]byte("10"),
					[]byte("1000"),
				},
				prot.CmdFlags{},
			),
			nil,
			prot.ErrInvalidCmdArgs,
		},
		{
			prot.NewCmd(
				"add",
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
				"add",
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
				"add",
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
				"add",
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
				"add",
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
			prot.NewClientErr(job.ErrInvalidTTL.Error()),
		},
		{
			prot.NewCmd(
				"add",
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
				"add",
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
		{
			prot.NewCmd(
				"run", // Wrong CmdFlags
				[][]byte{
					[]byte("00000000-0000-0000-0000-000000000000"),
					[]byte("q1"),
					[]byte("10"),
					[]byte("1000"),
					[]byte("1"),
					[]byte("a"),
				},
				prot.CmdFlags{},
			),
			nil,
			prot.ErrInvalidCmdArgs,
		},
		{
			prot.NewCmd(
				"add",
				[][]byte{
					[]byte("00000000-0000-0000-0000-000000000000"),
					[]byte("q1"),
					[]byte("10"),
					[]byte("1000"),
					[]byte("1"),
					[]byte("a"),
				},
				prot.CmdFlags{"max-attempts": []byte("256")},
			),
			nil,
			prot.NewClientErr(job.ErrInvalidMaxAttempts.Error()),
		},
		{
			prot.NewCmd(
				"add",
				[][]byte{
					[]byte("00000000-0000-0000-0000-000000000000"),
					[]byte("q1"),
					[]byte("10"),
					[]byte("1000"),
					[]byte("1"),
					[]byte("a"),
				},
				prot.CmdFlags{"max-attempts": []byte("-1")},
			),
			nil,
			prot.NewClientErr(job.ErrInvalidMaxAttempts.Error()),
		},
		{
			prot.NewCmd(
				"add",
				[][]byte{
					[]byte("00000000-0000-0000-0000-000000000000"),
					[]byte("q1"),
					[]byte("10"),
					[]byte("1000"),
					[]byte("1"),
					[]byte("a"),
				},
				prot.CmdFlags{"max-attempts": []byte("")},
			),
			nil,
			prot.NewClientErr(job.ErrInvalidMaxAttempts.Error()),
		},
		{
			prot.NewCmd(
				"add",
				[][]byte{
					[]byte("00000000-0000-0000-0000-000000000000"),
					[]byte("q1"),
					[]byte("10"),
					[]byte("1000"),
					[]byte("1"),
					[]byte("a"),
				},
				prot.CmdFlags{"max-attempts": []byte("*")},
			),
			nil,
			prot.NewClientErr(job.ErrInvalidMaxAttempts.Error()),
		},
		{
			prot.NewCmd(
				"add",
				[][]byte{
					[]byte("00000000-0000-0000-0000-000000000000"),
					[]byte("q1"),
					[]byte("10"),
					[]byte("1000"),
					[]byte("1"),
					[]byte("a"),
				},
				prot.CmdFlags{"max-fails": []byte("")},
			),
			nil,
			prot.NewClientErr(job.ErrInvalidMaxFails.Error()),
		},
		{
			prot.NewCmd(
				"add",
				[][]byte{
					[]byte("00000000-0000-0000-0000-000000000000"),
					[]byte("q1"),
					[]byte("10"),
					[]byte("1000"),
					[]byte("1"),
					[]byte("a"),
				},
				prot.CmdFlags{"max-fails": []byte("-1")},
			),
			nil,
			prot.NewClientErr(job.ErrInvalidMaxFails.Error()),
		},
		{
			prot.NewCmd(
				"add",
				[][]byte{
					[]byte("00000000-0000-0000-0000-000000000000"),
					[]byte("q1"),
					[]byte("10"),
					[]byte("1000"),
					[]byte("1"),
					[]byte("a"),
				},
				prot.CmdFlags{"max-fails": []byte("256")},
			),
			nil,
			prot.NewClientErr(job.ErrInvalidMaxFails.Error()),
		},
		{
			prot.NewCmd(
				"add",
				[][]byte{
					[]byte("00000000-0000-0000-0000-000000000000"),
					[]byte("q1"),
					[]byte("10"),
					[]byte("1000"),
					[]byte("1"),
					[]byte("a"),
				},
				prot.CmdFlags{"max-fails": []byte("*")},
			),
			nil,
			prot.NewClientErr(job.ErrInvalidMaxFails.Error()),
		},
		{
			prot.NewCmd(
				"add",
				[][]byte{
					[]byte("00000000-0000-0000-0000-000000000000"),
					[]byte("q1"),
					[]byte("10"),
					[]byte("1000"),
					[]byte("1"),
					[]byte("a"),
				},
				prot.CmdFlags{"priority": []byte("")},
			),
			nil,
			prot.NewClientErr(job.ErrInvalidPriority.Error()),
		},
		{
			prot.NewCmd(
				"add",
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
				"add",
				[][]byte{
					[]byte("00000000-0000-0000-0000-000000000000"),
					[]byte("q1"),
					[]byte("10"),
					[]byte("1000"),
					[]byte("1"),
					[]byte("a"),
				},
				prot.CmdFlags{"priority": []byte("-1")},
			),
			nil,
			prot.NewClientErr(job.ErrInvalidPriority.Error()),
		},
		{
			prot.NewCmd(
				"add",
				[][]byte{
					[]byte("00000000-0000-0000-0000-000000000000"),
					[]byte("q1"),
					[]byte("10"),
					[]byte("1000"),
					[]byte("1"),
					[]byte("a"),
				},
				prot.CmdFlags{"priority": []byte("4294967296")},
			),
			nil,
			prot.NewClientErr(job.ErrInvalidPriority.Error()),
		},
	}

	for _, tt := range tests {
		resp, err := handler.Exec(tt.cmd)
		if !bytes.Equal(tt.resp, resp) {
			t.Fatalf("Response mismatch, expResp=%v, actResp=%v", tt.resp, resp)
		}

		if err.Error() != tt.err.Error() {
			t.Fatalf("Err mismatch, cmd=%s, expErr=%v, actErr=%v", tt.cmd.Args, tt.err, err)
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
