package handlers

import (
	"bytes"
	"fmt"
	"testing"
	"time"

	"github.com/iamduo/workq/int/job"
	"github.com/iamduo/workq/int/prot"
	"github.com/iamduo/workq/int/testutil"
)

func TestResultNotFound(t *testing.T) {
	reg := job.NewRegistry()
	qc := job.NewQueueController()
	handler := NewResultHandler(reg, qc)

	id := testutil.GenIDString()
	cmd := prot.NewCmd(
		"result",
		[][]byte{
			[]byte(id),
			[]byte("1"),
		},
		prot.CmdFlags{},
	)

	resp, err := handler.Exec(cmd)
	if err != prot.ErrNotFound || resp != nil {
		t.Fatalf("Expected NOT-FOUND, err=%s, resp=%v", err, resp)
	}
}

func TestResultTimedOut(t *testing.T) {
	reg := job.NewRegistry()
	qc := job.NewQueueController()
	handler := NewResultHandler(reg, qc)

	id := testutil.GenIDString()
	j := job.NewEmptyJob()
	j.ID, _ = parseID([]byte(id))
	j.Name = testutil.GenName()

	rec := job.NewRunRecord()
	rec.Job = j
	reg.Add(rec)

	cmd := prot.NewCmd(
		"result",
		[][]byte{
			[]byte(id),
			[]byte("1"),
		},
		prot.CmdFlags{},
	)

	resp, err := handler.Exec(cmd)
	if err != prot.ErrTimeout || resp != nil {
		t.Fatalf("Expected TIMEOUT err=%s, resp=%v", err, resp)
	}
}

func TestResultAlreadyProcessed(t *testing.T) {
	reg := job.NewRegistry()
	qc := job.NewQueueController()
	handler := NewResultHandler(reg, qc)

	id := testutil.GenIDString()
	j := job.NewEmptyJob()
	j.ID, _ = parseID([]byte(id))
	j.Name = testutil.GenName()

	rec := job.NewRunRecord()
	rec.Job = j
	rec.State = job.StateCompleted
	rec.Result = []byte("a")
	reg.Add(rec)

	cmd := prot.NewCmd(
		"result",
		[][]byte{
			[]byte(id),
			[]byte("1"),
		},
		prot.CmdFlags{},
	)

	expResp := []byte(fmt.Sprintf(
		"OK 1\r\n%s 1 %d\r\n%s\r\n",
		id,
		len(rec.Result),
		rec.Result,
	))
	resp, err := handler.Exec(cmd)
	if !bytes.Equal(resp, expResp) {
		t.Fatalf("Result response mismatch, exp=%s, act=%s", expResp, resp)
	}

	if err != nil {
		t.Fatalf("Result err mismatch, expected nil, err=%s", err)
	}
}

func TestResultWhileProcessing(t *testing.T) {
	reg := job.NewRegistry()
	qc := job.NewQueueController()
	handler := NewResultHandler(reg, qc)

	id := testutil.GenIDString()
	j := job.NewEmptyJob()
	j.ID, _ = parseID([]byte(id))
	j.Name = testutil.GenName()

	rec := job.NewRunRecord()
	rec.Job = j
	reg.Add(rec)

	expResult := []byte("a")

	done := make(chan struct{}, 1)
	go func() {
		defer func() { done <- struct{}{} }()
		cmd := prot.NewCmd(
			"result",
			[][]byte{
				[]byte(id),
				[]byte("1000"),
			},
			prot.CmdFlags{},
		)
		expResp := []byte(fmt.Sprintf(
			"OK 1\r\n%s 1 %d\r\n%s\r\n",
			id,
			len(expResult),
			expResult,
		))
		resp, err := handler.Exec(cmd)
		if err != nil {
			t.Fatalf("Result err mismatch, expected nil, err=%s", err)
		}
		if !bytes.Equal(resp, expResp) {
			t.Fatalf("Result response mismatch, exp=%s, act=%s", expResp, resp)
		}
	}()

	time.Sleep(100 * time.Microsecond)
	rec.Mu.Lock()
	rec.State = job.StateCompleted
	rec.WriteResult(expResult, true)
	rec.Mu.Unlock()
	<-done
}

func TestResultInvalidArgs(t *testing.T) {
	reg := job.NewRegistry()
	qc := job.NewQueueController()
	handler := NewResultHandler(reg, qc)

	tests := []struct {
		cmd  *prot.Cmd
		resp []byte
		err  error
	}{
		{
			prot.NewCmd(
				"result",
				[][]byte{},
				prot.CmdFlags{},
			),
			nil,
			prot.ErrInvalidCmdArgs,
		},
		{
			prot.NewCmd(
				"result",
				[][]byte{[]byte("")},
				prot.CmdFlags{},
			),
			nil,
			prot.ErrInvalidCmdArgs,
		},
		{
			prot.NewCmd(
				"result",
				[][]byte{[]byte("*")},
				prot.CmdFlags{},
			),
			nil,
			prot.ErrInvalidCmdArgs,
		},
		{
			prot.NewCmd(
				"result",
				[][]byte{
					[]byte("*"),
					[]byte("1"),
				},
				prot.CmdFlags{},
			),
			nil,
			prot.NewClientErr(job.ErrInvalidID.Error()),
		},
		{
			prot.NewCmd(
				"result",
				[][]byte{
					[]byte("61a444a0-6128-41c0-8078-cc757d3bd2d8"),
					[]byte("-1"),
				},
				prot.CmdFlags{},
			),
			nil,
			ErrInvalidWaitTimeout,
		},
		// WRONG CMD
		{
			prot.NewCmd(
				"delete",
				[][]byte{
					[]byte("61a444a0-6128-41c0-8078-cc757d3bd2d8"),
					[]byte("-1"),
				},
				prot.CmdFlags{},
			),
			nil,
			prot.ErrInvalidCmdArgs,
		},
	}

	for _, tt := range tests {
		resp, err := handler.Exec(tt.cmd)
		if err.Error() != tt.err.Error() {
			t.Fatalf("Result err mismatch, cmd=%s, expErr=%v, actErr=%v", tt.cmd.Args, tt.err, err)
		}
		if !bytes.Equal(tt.resp, resp) {
			t.Fatalf("Result response mismatch, expResp=%v, actResp=%v", tt.resp, resp)
		}
	}
}
