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

type runControllerMock struct {
	runMethod func(j *job.Job, timeout uint32) (*job.RunResult, error)
}

func (c *runControllerMock) Run(j *job.Job, timeout uint32) (*job.RunResult, error) {
	return c.runMethod(j, timeout)
}

func TestRunWaitTimeout(t *testing.T) {
	id := job.ID(testutil.GenID())
	name := testutil.GenName()
	tests := []struct {
		expJob     *job.Job
		expTimeout int
		cmd        *prot.Cmd
	}{
		{
			expJob: &job.Job{
				ID:          id,
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
					[]byte(id.String()),
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
				ID:          id,
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
					[]byte(id.String()),
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
		tt.expJob.Created = time.Now().UTC()
		jc := &runControllerMock{
			runMethod: func(j *job.Job, timeout uint32) (*job.RunResult, error) {
				assertJobs(t, tt.expJob, j)

				if uint32(tt.expTimeout) != timeout {
					t.Fatalf("Timeout mismatch, act=%d", timeout)
				}

				return nil, job.ErrTimeout
			},
		}
		handler := NewRunHandler(jc)
		resp, err := handler.Exec(tt.cmd)
		if resp != nil || err == nil || err != prot.ErrTimeout {
			t.Fatalf("Run response mismatch, resp=%s, err=%s", resp, err)
		}
	}
}

func TestRunResult(t *testing.T) {
	expJob := &job.Job{
		ID:      job.ID(testutil.GenID()),
		Name:    testutil.GenName(),
		Payload: []byte("a"),
		TTR:     5,
		Created: time.Now().UTC(),
	}
	expTimeout := 1000
	expResult := &job.RunResult{Success: true, Result: []byte("b")}

	jc := &runControllerMock{
		runMethod: func(j *job.Job, timeout uint32) (*job.RunResult, error) {
			assertJobs(t, expJob, j)

			if uint32(expTimeout) != timeout {
				t.Fatalf("Timeout mismatch, act=%d", timeout)
			}

			return expResult, nil
		},
	}
	handler := NewRunHandler(jc)

	expResp := []byte(fmt.Sprintf(
		"OK 1\r\n%s 1 %d\r\n%s\r\n",
		expJob.ID.String(),
		len(expResult.Result),
		expResult.Result,
	))
	cmd := prot.NewCmd(
		"run",
		[][]byte{
			[]byte(expJob.ID.String()),
			[]byte(expJob.Name),
			[]byte("5"),
			[]byte("1000"),
			[]byte(fmt.Sprintf("%d", len(expJob.Payload))),
			[]byte(expJob.Payload),
		},
		prot.CmdFlags{},
	)
	resp, err := handler.Exec(cmd)
	if err != nil {
		t.Fatalf("Run err=%v", err)
	}
	if !bytes.Equal(expResp, resp) {
		t.Fatalf("Run response mismatch, resp=%s", resp)
	}
}

func TestRunDuplicate(t *testing.T) {
	jc := &runControllerMock{
		runMethod: func(j *job.Job, timeout uint32) (*job.RunResult, error) {
			return nil, job.ErrDuplicateJob
		},
	}
	handler := NewRunHandler(jc)
	cmd := prot.NewCmd(
		"run",
		[][]byte{
			[]byte(testutil.GenIDString()),
			[]byte(testutil.GenName()),
			[]byte("5"),
			[]byte("10"),
			[]byte("0"),
			[]byte{},
		},
		prot.CmdFlags{},
	)

	expErr := prot.NewClientErr(job.ErrDuplicateJob.Error())
	resp, err := handler.Exec(cmd)
	if resp != nil || err == nil || err.Error() != expErr.Error() {
		t.Fatalf("Expected ErrDuplicateJob, resp=%s, err=%s", resp, err)
	}
}

func TestRunInvalidArgs(t *testing.T) {
	jc := &runControllerMock{
		runMethod: func(j *job.Job, timeout uint32) (*job.RunResult, error) {
			t.Fatalf("Unexpected job controller")
			return nil, nil
		},
	}
	handler := NewRunHandler(jc)

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
					[]byte("61a444a0-6128-41c0-8078-cc757d3bd2d8"),
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
					[]byte("61a444a0-6128-41c0-8078-cc757d3bd2d8"),
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
					[]byte("61a444a0-6128-41c0-8078-cc757d3bd2d8"),
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
					[]byte("61a444a0-6128-41c0-8078-cc757d3bd2d8"),
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
	}

	for _, tt := range tests {
		resp, err := handler.Exec(tt.cmd)
		if !bytes.Equal(tt.resp, resp) {
			t.Fatalf("Run response mismatch, expResp=%v, actResp=%v", tt.resp, resp)
		}

		if err.Error() != tt.err.Error() {
			t.Fatalf("Err mismatch, expErr=%v, actErr=%v", tt.err, err)
		}
	}
}
