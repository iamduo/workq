package handlers

import (
	"bytes"
	"strconv"
	"testing"
	"time"

	"github.com/iamduo/workq/int/job"
	"github.com/iamduo/workq/int/prot"
	"github.com/iamduo/workq/int/testutil"
	"github.com/iamduo/workq/int/wqueue"
)

func TestComplete(t *testing.T) {
	tests := []struct {
		result []byte
	}{
		{result: []byte("")},
		{result: []byte("a")},
		{result: make([]byte, job.MaxResult)},
	}

	for _, tt := range tests {
		reg := job.NewRegistry()
		qc := wqueue.NewController()
		handler := NewCompleteHandler(reg, qc)

		id := []byte(testutil.GenId())
		j := job.NewEmptyJob()
		j.ID, _ = job.IDFromBytes(id)
		j.Name = "q1"

		rec := job.NewRunRecord()
		rec.Job = j
		rec.Timers[job.RunRecTTRTimerIdx] = job.NewTimer(10 * time.Millisecond)
		reg.Add(rec)

		qc.Add(j)

		expResp := []byte("OK\r\n")
		cmd := prot.NewCmd(
			"complete",
			[][]byte{
				id,
				[]byte(strconv.Itoa(len(tt.result))),
				tt.result,
			},
			prot.CmdFlags{},
		)
		resp, err := handler.Exec(cmd)
		if err != nil {
			t.Fatalf("Complete unexpected err=%v", err)
		}
		if !bytes.Equal(expResp, resp) {
			t.Fatalf("Complete response mismatch, exp=%v, act=%v", expResp, resp)
		}

		rec, ok := reg.Record(j.ID)
		if !ok {
			t.Fatalf("Expected run record")
		}
		if qc.Exists(j) {
			t.Fatalf("Unexpected job found after complete")
		}
		if rec.State != job.StateCompleted {
			t.Fatalf("Unexpected job state=%v", rec.State)
		}

		resp, err = handler.Exec(cmd)
		if err != ErrDuplicateResult || resp != nil {
			t.Fatalf("Complete unexpected err=%v, resp=%v", err, resp)
		}
	}
}

func TestCompleteNotFound(t *testing.T) {
	reg := job.NewRegistry()
	qc := wqueue.NewController()
	handler := NewCompleteHandler(reg, qc)

	id := []byte(testutil.GenId())
	cmd := prot.NewCmd(
		"complete",
		[][]byte{
			id,
			[]byte("1"),
			[]byte("a"),
		},
		prot.CmdFlags{},
	)
	resp, err := handler.Exec(cmd)
	if err != prot.ErrNotFound {
		t.Fatalf("Complete unexpected err=%v", err)
	}

	if resp != nil {
		t.Fatalf("Complete response mismatch, resp=%v", resp)
	}
}

func TestCompleteInvalidArgs(t *testing.T) {
	reg := job.NewRegistry()
	qc := wqueue.NewController()
	handler := NewCompleteHandler(reg, qc)

	tests := []struct {
		cmd  *prot.Cmd
		resp []byte
		err  error
	}{
		{
			prot.NewCmd(
				"complete",
				[][]byte{},
				prot.CmdFlags{},
			),
			nil,
			prot.ErrInvalidCmdArgs,
		},
		{
			prot.NewCmd(
				"complete",
				[][]byte{[]byte("")},
				prot.CmdFlags{},
			),
			nil,
			prot.ErrInvalidCmdArgs,
		},
		{
			prot.NewCmd(
				"complete",
				[][]byte{[]byte("00000000-0000-0000-0000-000000000000")},
				prot.CmdFlags{},
			),
			nil,
			prot.ErrInvalidCmdArgs,
		},
		{
			prot.NewCmd(
				"complete",
				[][]byte{
					[]byte("00000000-0000-0000-0000-000000000000"),
					[]byte("1"),
				},
				prot.CmdFlags{},
			),
			nil,
			prot.ErrInvalidCmdArgs,
		},
		// Wrong CMD
		{
			prot.NewCmd(
				"fail",
				[][]byte{
					[]byte("00000000-0000-0000-0000-000000000000"),
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
				"complete",
				[][]byte{
					[]byte("*"),
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
				"complete",
				[][]byte{
					[]byte("00000000-0000-0000-0000-000000000000"),
					[]byte("-1"),
					[]byte("a"),
				},
				prot.CmdFlags{},
			),
			nil,
			prot.NewClientErr(job.ErrInvalidResult.Error()),
		},
		{
			prot.NewCmd(
				"complete",
				[][]byte{
					[]byte("00000000-0000-0000-0000-000000000000"),
					[]byte("1"),
					[]byte(""),
				},
				prot.CmdFlags{},
			),
			nil,
			prot.NewClientErr(job.ErrInvalidResult.Error()),
		},
	}

	for _, tt := range tests {
		resp, err := handler.Exec(tt.cmd)
		if !bytes.Equal(tt.resp, resp) {
			t.Fatalf("Fail response mismatch, expResp=%v, actResp=%v", tt.resp, resp)
		}

		if err.Error() != tt.err.Error() {
			t.Fatalf("Fail err mismatch, cmd=%s, expErr=%v, actErr=%v", tt.cmd.Args, tt.err, err)
		}
	}
}
