package handlers

import (
	"bytes"
	"testing"

	"github.com/iamduo/workq/int/job"
	"github.com/iamduo/workq/int/prot"
	"github.com/iamduo/workq/int/testutil"
)

type deleteControllerMock struct {
	deleteMethod func(id job.ID) error
}

func (c *deleteControllerMock) Delete(id job.ID) error {
	return c.deleteMethod(id)
}

func TestDelete(t *testing.T) {
	jc := &deleteControllerMock{
		deleteMethod: func(id job.ID) error {
			return nil
		},
	}
	handler := NewDeleteHandler(jc)
	expResp := []byte("OK\r\n")
	cmd := prot.NewCmd(
		"delete",
		[][]byte{
			[]byte(testutil.GenIDString()),
		},
		prot.CmdFlags{},
	)
	resp, err := handler.Exec(cmd)
	if !bytes.Equal(expResp, resp) {
		t.Fatalf("Delete response mismatch, exp=%v, act=%v", expResp, resp)
	}

	if err != nil {
		t.Fatalf("Delete unexpected err=%v", err)
	}
}

func TestDeleteNotFound(t *testing.T) {
	jc := &deleteControllerMock{
		deleteMethod: func(id job.ID) error {
			return job.ErrNotFound
		},
	}
	handler := NewDeleteHandler(jc)

	id := []byte(testutil.GenIDString())
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
	jc := &deleteControllerMock{
		deleteMethod: func(id job.ID) error {
			t.Fatalf("Unexpected call to job controller")
			return nil
		},
	}
	handler := NewDeleteHandler(jc)

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
