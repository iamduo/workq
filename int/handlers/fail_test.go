package handlers

import (
	"bytes"
	"strconv"
	"testing"

	"github.com/iamduo/workq/int/job"
	"github.com/iamduo/workq/int/prot"
	"github.com/iamduo/workq/int/testutil"
)

type failControllerMock struct {
	failMethod func(id job.ID, result []byte) error
}

func (c *failControllerMock) Fail(id job.ID, result []byte) error {
	return c.failMethod(id, result)
}

func TestFail(t *testing.T) {
	tests := []struct {
		result []byte
	}{
		{result: []byte("")},
		{result: []byte("a")},
		{result: make([]byte, job.MaxResult)},
	}
	jc := &failControllerMock{
		failMethod: func(id job.ID, result []byte) error {
			return nil
		},
	}
	for _, tt := range tests {
		handler := NewFailHandler(jc)
		expResp := []byte("OK\r\n")
		cmd := prot.NewCmd(
			"fail",
			[][]byte{
				[]byte(testutil.GenIDString()),
				[]byte(strconv.Itoa(len(tt.result))),
				tt.result,
			},
			prot.CmdFlags{},
		)
		resp, err := handler.Exec(cmd)
		if err != nil {
			t.Fatalf("Fail unexpected err=%v", err)
		}
		if !bytes.Equal(expResp, resp) {
			t.Fatalf("Fail response mismatch, exp=%v, act=%v", expResp, resp)
		}
	}
}

func TestFailJobErrors(t *testing.T) {
	tests := []struct {
		jobErr error
		cmdErr error
	}{
		{job.ErrNotFound, prot.ErrNotFound},
		{job.ErrDuplicateJob, prot.NewClientErr(job.ErrDuplicateJob.Error())},
	}

	for _, tt := range tests {
		jc := &failControllerMock{
			failMethod: func(id job.ID, result []byte) error {
				return tt.jobErr
			},
		}
		handler := NewFailHandler(jc)

		id := []byte(testutil.GenIDString())
		cmd := prot.NewCmd(
			"fail",
			[][]byte{
				id,
				[]byte("1"),
				[]byte("a"),
			},
			prot.CmdFlags{},
		)
		resp, err := handler.Exec(cmd)
		if err == nil || err.Error() != tt.cmdErr.Error() {
			t.Fatalf("Fail unexpected err=%v", err)
		}

		if resp != nil {
			t.Fatalf("Fail response mismatch, resp=%v", resp)
		}
	}
}

func TestFailInvalidArgs(t *testing.T) {
	jc := &failControllerMock{
		failMethod: func(id job.ID, result []byte) error {
			t.Fatalf("Unexpected call to job controller")
			return nil
		},
	}
	handler := NewFailHandler(jc)
	tests := []struct {
		cmd  *prot.Cmd
		resp []byte
		err  error
	}{
		{
			prot.NewCmd(
				"fail",
				[][]byte{},
				prot.CmdFlags{},
			),
			nil,
			prot.ErrInvalidCmdArgs,
		},
		{
			prot.NewCmd(
				"fail",
				[][]byte{[]byte("")},
				prot.CmdFlags{},
			),
			nil,
			prot.ErrInvalidCmdArgs,
		},
		{
			prot.NewCmd(
				"fail",
				[][]byte{[]byte("61a444a0-6128-41c0-8078-cc757d3bd2d8")},
				prot.CmdFlags{},
			),
			nil,
			prot.ErrInvalidCmdArgs,
		},
		{
			prot.NewCmd(
				"fail",
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
				"complete",
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
				"fail",
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
				"fail",
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
				"fail",
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
