package handlers

import (
	"bytes"
	"testing"

	"github.com/iamduo/workq/int/job"
	"github.com/iamduo/workq/int/prot"
	"github.com/iamduo/workq/int/testutil"
	"github.com/iamduo/workq/int/wqueue"
)

func TestDelete(t *testing.T) {
	reg := job.NewRegistry()
	qc := wqueue.NewController()
	handler := NewDeleteHandler(reg, qc)

	id := []byte(testutil.GenId())
	j := job.NewEmptyJob()
	j.ID, _ = job.IDFromBytes(id)
	j.Name = "q1"

	rec := job.NewRunRecord()
	rec.Job = j
	reg.Add(rec)

	qc.Add(j)

	expResp := []byte("OK\r\n")
	cmd := prot.NewCmd(
		"delete",
		[][]byte{id},
		prot.CmdFlags{},
	)
	resp, err := handler.Exec(cmd)
	if !bytes.Equal(expResp, resp) {
		t.Fatalf("Delete response mismatch, exp=%v, act=%v", expResp, resp)
	}

	if err != nil {
		t.Fatalf("Delete unexpected err=%v", err)
	}

	_, ok := reg.Record(j.ID)
	if ok {
		t.Fatalf("Unexpected run record after delete")
	}

	if qc.Exists(j) {
		t.Fatalf("Unexpected job found after delete")
	}
}

func TestDeleteNotFound(t *testing.T) {
	reg := job.NewRegistry()
	qc := wqueue.NewController()
	handler := NewDeleteHandler(reg, qc)

	id := []byte(testutil.GenId())
	cmd := prot.NewCmd(
		"delete",
		[][]byte{id},
		prot.CmdFlags{},
	)
	resp, err := handler.Exec(cmd)
	if err != prot.ErrNotFound {
		t.Fatalf("Delete unexpected err=%v", err)
	}

	if resp != nil {
		t.Fatalf("Delete response mismatch, res%v", resp)
	}
}

func TestDeleteInvalidArgs(t *testing.T) {
	reg := job.NewRegistry()
	qc := wqueue.NewController()
	handler := NewDeleteHandler(reg, qc)

	tests := []struct {
		cmd  *prot.Cmd
		resp []byte
		err  error
	}{
		{
			prot.NewCmd(
				"delete",
				[][]byte{},
				prot.CmdFlags{},
			),
			nil,
			prot.ErrInvalidCmdArgs,
		},
		{
			prot.NewCmd(
				"delete",
				[][]byte{[]byte("")},
				prot.CmdFlags{},
			),
			nil,
			prot.NewClientErr(job.ErrInvalidID.Error()),
		},
		{
			prot.NewCmd(
				"delete",
				[][]byte{[]byte("*")},
				prot.CmdFlags{},
			),
			nil,
			prot.NewClientErr(job.ErrInvalidID.Error()),
		},
	}

	for _, tt := range tests {
		resp, err := handler.Exec(tt.cmd)
		if !bytes.Equal(tt.resp, resp) {
			t.Fatalf("Delete response mismatch, expResp=%v, actResp=%v", tt.resp, resp)
		}

		if err.Error() != tt.err.Error() {
			t.Fatalf("Delete err mismatch, cmd=%s, expErr=%v, actErr=%v", tt.cmd.Args, tt.err, err)
		}
	}
}
