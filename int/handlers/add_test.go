package handlers

import (
	"bytes"
	"testing"
	"time"

	"github.com/iamduo/workq/int/job"
	"github.com/iamduo/workq/int/prot"
	"github.com/iamduo/workq/int/testutil"
)

type AddControllerMock struct {
	addMethod    func(*job.Job) error
	expireMethod func(job.ID)
}

func (c *AddControllerMock) Add(j *job.Job) error {
	return c.addMethod(j)
}

func (c *AddControllerMock) Expire(id job.ID) {
	c.expireMethod(id)
}

func (c *AddControllerMock) HandleExpire(func(job.ID)) {
	// NO-OP
}

func (c *AddControllerMock) ExpireFunc() func(job.ID) {
	// NO-OP
	return c.expireMethod
}

func TestAdd(t *testing.T) {
	id := job.ID(testutil.GenID())
	name := testutil.GenName()
	tests := []struct {
		expJob *job.Job
		cmd    *prot.Cmd
	}{
		{
			expJob: &job.Job{
				ID:      id,
				Name:    name,
				Payload: []byte(""),
				TTR:     1,
				TTL:     10,
			},
			cmd: prot.NewCmd(
				"add",
				[][]byte{
					[]byte(id.String()),
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
				ID:      id,
				Name:    name,
				Payload: []byte("a"),
				TTR:     1,
				TTL:     10,
			},
			cmd: prot.NewCmd(
				"add",
				[][]byte{
					[]byte(id.String()),
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
				ID:       id,
				Name:     name,
				Payload:  []byte("a"),
				Priority: 10,
				TTR:      1,
				TTL:      10,
			},
			cmd: prot.NewCmd(
				"add",
				[][]byte{
					[]byte(id.String()),
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
				ID:          id,
				Name:        name,
				Payload:     []byte("a"),
				MaxAttempts: 3,
				TTR:         1,
				TTL:         10,
			},
			cmd: prot.NewCmd(
				"add",
				[][]byte{
					[]byte(id.String()),
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
				ID:       id,
				Name:     name,
				Payload:  []byte("a"),
				MaxFails: 3,
				TTR:      1,
				TTL:      10,
			},
			cmd: prot.NewCmd(
				"add",
				[][]byte{
					[]byte(id.String()),
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
				ID:          id,
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
					[]byte(id.String()),
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
				ID:          id,
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
					[]byte(id.String()),
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

		jc := &AddControllerMock{
			addMethod: func(j *job.Job) error {
				assertJobs(t, tt.expJob, j)
				return nil
			},
		}
		handler := NewAddHandler(jc)

		expResp := []byte("OK\r\n")
		resp, err := handler.Exec(tt.cmd)
		if err != nil {
			t.Fatalf("Add err=%v", err)
		}

		if !bytes.Equal(resp, expResp) {
			t.Fatalf("Add response mismatch, resp=%s", resp)
		}
	}
}

func TestAddJobErrors(t *testing.T) {
	tests := []struct {
		jobErr error
		cmdErr error
	}{
		{
			jobErr: job.ErrDuplicateJob,
			cmdErr: prot.NewClientErr(job.ErrDuplicateJob.Error()),
		},
		{
			jobErr: job.ErrQueueOutOfSync,
			cmdErr: prot.NewServerErr(job.ErrQueueOutOfSync.Error()),
		},
	}

	for _, tt := range tests {
		jc := &AddControllerMock{
			addMethod: func(j *job.Job) error {
				return tt.jobErr
			},
		}
		handler := NewAddHandler(jc)
		cmd := prot.NewCmd(
			"add",
			[][]byte{
				[]byte(testutil.GenIDString()),
				[]byte(testutil.GenName()),
				[]byte("100"),
				[]byte("1000"),
				[]byte("1"),
				[]byte("a"),
			},
			prot.CmdFlags{},
		)

		resp, err := handler.Exec(cmd)
		if err == nil || err.Error() != tt.cmdErr.Error() || resp != nil {
			t.Fatalf("Add response mismatch, resp=%v, err=%v", resp, err)
		}
	}
}

// Only client cmd specific args + parsing are tested.
// Job specific properties are owned by the job controller.
// Avoids heavy repeated test cases.
func TestAddInvalidArgs(t *testing.T) {
	jc := &AddControllerMock{
		addMethod: func(j *job.Job) error {
			t.Fatalf("Unexpected call to job controller")
			return nil
		},
	}
	handler := NewAddHandler(jc)
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
					[]byte("61a444a0-6128-1c0-8078-cc757d3bd2d9"), // Bad UUID
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
					[]byte("61a444a0-6128-41c0-8078-cc757d3bd2d8"),
					[]byte(""), // Bad Name
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
					[]byte("61a444a0-6128-41c0-8078-cc757d3bd2d8"),
					[]byte("q1"),
					[]byte("-10"), // Bad TTR
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
					[]byte("61a444a0-6128-41c0-8078-cc757d3bd2d8"),
					[]byte("q1"),
					[]byte("10"),
					[]byte("-1000"), // Bad TTL
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
		{
			prot.NewCmd(
				"run", // Wrong Cmd
				[][]byte{
					[]byte("61a444a0-6128-41c0-8078-cc757d3bd2d8"),
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
	}

	for _, tt := range tests {
		resp, err := handler.Exec(tt.cmd)
		if !bytes.Equal(tt.resp, resp) {
			t.Fatalf("Response mismatch, expResp=%v, actResp=%v", tt.resp, resp)
		}

		if err.Error() != tt.err.Error() {
			t.Fatalf("Err mismatch, cmd=%s, expErr=%v, actErr=%v", tt.cmd.Args, tt.err, err)
		}
	}
}
