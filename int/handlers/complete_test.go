package handlers

import (
	"bytes"
	"strconv"
	"testing"

	"github.com/iamduo/workq/int/job"
	"github.com/iamduo/workq/int/prot"
	"github.com/iamduo/workq/int/testutil"
)

type completeControllerMock struct {
	completeMethod func(id job.ID, result []byte) error
}

func (c *completeControllerMock) Complete(id job.ID, result []byte) error {
	return c.completeMethod(id, result)
}

func TestComplete(t *testing.T) {
	tests := []struct {
		result []byte
	}{
		{result: []byte("")},
		{result: []byte("a")},
		{result: make([]byte, job.MaxResult)},
	}
	jc := &completeControllerMock{
		completeMethod: func(id job.ID, result []byte) error {
			return nil
		},
	}
	for _, tt := range tests {
		handler := NewCompleteHandler(jc)
		expResp := []byte("OK\r\n")
		cmd := prot.NewCmd(
			"complete",
			[][]byte{
				[]byte(testutil.GenIDString()),
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
	}
}

func TestCompleteJobErrors(t *testing.T) {
	tests := []struct {
		jobErr error
		cmdErr error
	}{
		{job.ErrNotFound, prot.ErrNotFound},
		{job.ErrDuplicateJob, prot.NewClientErr(job.ErrDuplicateJob.Error())},
	}

	for _, tt := range tests {
		jc := &completeControllerMock{
			completeMethod: func(id job.ID, result []byte) error {
				return tt.jobErr
			},
		}
		handler := NewCompleteHandler(jc)

		id := []byte(testutil.GenIDString())
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
		if err == nil || err.Error() != tt.cmdErr.Error() {
			t.Fatalf("Complete unexpected err=%v", err)
		}

		if resp != nil {
			t.Fatalf("Complete response mismatch, resp=%v", resp)
		}
	}
}

func TestCompleteInvalidArgs(t *testing.T) {
	jc := &completeControllerMock{
		completeMethod: func(id job.ID, result []byte) error {
			t.Fatalf("Unexpected call to job controller")
			return nil
		},
	}
	handler := NewCompleteHandler(jc)
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
				[][]byte{[]byte("61a444a0-6128-41c0-8078-cc757d3bd2d8")},
				prot.CmdFlags{},
			),
			nil,
			prot.ErrInvalidCmdArgs,
		},
		{
			prot.NewCmd(
				"complete",
				[][]byte{
					[]byte("61a444a0-6128-41c0-8078-cc757d3bd2d8"),
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
					[]byte("61a444a0-6128-41c0-8078-cc757d3bd2d8"),
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
					[]byte("61a444a0-6128-41c0-8078-cc757d3bd2d8"),
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
					[]byte("61a444a0-6128-41c0-8078-cc757d3bd2d8"),
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
