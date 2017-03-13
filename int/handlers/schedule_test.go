package handlers

import (
	"bytes"
	"testing"
	"time"

	"github.com/iamduo/workq/int/job"
	"github.com/iamduo/workq/int/prot"
	"github.com/iamduo/workq/int/testutil"
)

const testTimeFormat = "2006-01-02T15:04:05Z"

type scheduleControllerMock struct {
	scheduleMethod func(j *job.Job) error
}

func (c *scheduleControllerMock) Schedule(j *job.Job) error {
	return c.scheduleMethod(j)
}

func TestSchedule(t *testing.T) {
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
				"schedule",
				[][]byte{
					[]byte(id.String()),
					[]byte(name),
					[]byte("1"),
					[]byte("10"),
					[]byte("TIME_PLACEHOLDER"),
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
				"schedule",
				[][]byte{
					[]byte(id.String()),
					[]byte(name),
					[]byte("1"),
					[]byte("10"),
					[]byte("TIME_PLACEHOLDER"),
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
				"schedule",
				[][]byte{
					[]byte(id.String()),
					[]byte(name),
					[]byte("1"),
					[]byte("10"),
					[]byte("TIME_PLACEHOLDER"),
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
				"schedule",
				[][]byte{
					[]byte(id.String()),
					[]byte(name),
					[]byte("1"),
					[]byte("10"),
					[]byte("TIME_PLACEHOLDER"),
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
				"schedule",
				[][]byte{
					[]byte(id.String()),
					[]byte(name),
					[]byte("1"),
					[]byte("10"),
					[]byte("TIME_PLACEHOLDER"),
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
				"schedule",
				[][]byte{
					[]byte(id.String()),
					[]byte(name),
					[]byte("1"),
					[]byte("10"),
					[]byte("TIME_PLACEHOLDER"),
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
				Priority:    10,
				MaxAttempts: 9,
				MaxFails:    3,
				TTR:         1,
				TTL:         10,
			},
			cmd: prot.NewCmd(
				"schedule",
				[][]byte{
					[]byte(id.String()),
					[]byte(name),
					[]byte("1"),
					[]byte("10"),
					[]byte("TIME_PLACEHOLDER"),
					[]byte("1048576"),
					make([]byte, 1048576),
				},
				prot.CmdFlags{
					"priority":     []byte("10"),
					"max-attempts": []byte("9"),
					"max-fails":    []byte("3"),
				},
			),
		},
	}

	for _, tt := range tests {
		duration := 1 * time.Second
		tt.expJob.Created = time.Now().UTC()
		fmtSchedTime := time.Now().UTC().Add(duration).Format(testTimeFormat)
		// Reparsing time from string zeroes out any nanoseconds
		tt.expJob.Time, _ = time.Parse(testTimeFormat, fmtSchedTime)
		tt.cmd.Args[schedArgTime] = []byte(fmtSchedTime)

		jc := &scheduleControllerMock{
			scheduleMethod: func(j *job.Job) error {
				assertJobs(t, tt.expJob, j)
				return nil
			},
		}
		handler := NewScheduleHandler(jc)

		expResp := []byte("OK\r\n")
		resp, err := handler.Exec(tt.cmd)
		if err != nil {
			t.Fatalf("Schedule err=%v", err)
		}

		if !bytes.Equal(resp, expResp) {
			t.Fatalf("Schedule response mismatch, resp=%s", resp)
		}
	}
}

func TestScheduleInvalidArgs(t *testing.T) {
	jc := &scheduleControllerMock{
		scheduleMethod: func(j *job.Job) error {
			t.Fatalf("Unexpected cal to job controller")
			return nil
		},
	}
	handler := NewScheduleHandler(jc)

	fmtSchedTime := time.Now().UTC().Add(100 * time.Second).Format(testTimeFormat)

	tests := []struct {
		cmd  *prot.Cmd
		resp []byte
		err  error
	}{
		{
			prot.NewCmd(
				"schedule",
				[][]byte{},
				prot.CmdFlags{},
			),
			nil,
			prot.ErrInvalidCmdArgs,
		},
		{
			prot.NewCmd(
				"schedule",
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
				"schedule",
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
				"schedule",
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
				"schedule",
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
				"schedule",
				[][]byte{
					[]byte("123"),
					[]byte("q1"),
					[]byte("10"),
					[]byte("1000"),
					[]byte(fmtSchedTime),
				},
				prot.CmdFlags{},
			),
			nil,
			prot.ErrInvalidCmdArgs,
		},
		{
			prot.NewCmd(
				"schedule",
				[][]byte{
					[]byte("123"),
					[]byte("q1"),
					[]byte("10"),
					[]byte("1000"),
					[]byte(fmtSchedTime),
					[]byte("1"),
				},
				prot.CmdFlags{},
			),
			nil,
			prot.ErrInvalidCmdArgs,
		},
		{
			prot.NewCmd(
				"schedule",
				[][]byte{
					[]byte("*"),
					[]byte("q1"),
					[]byte("10"),
					[]byte("1000"),
					[]byte(fmtSchedTime),
					[]byte("1"),
					[]byte("a"),
				},
				prot.CmdFlags{},
			),
			nil,
			prot.NewClientErr(job.ErrInvalidID.Error()),
		},
		/*{
			prot.NewCmd(
				"schedule",
				[][]byte{
					[]byte("61a444a0-6128-41c0-8078-cc757d3bd2d8"),
					[]byte("*"),
					[]byte("10"),
					[]byte("1000"),
					[]byte(fmtSchedTime),
					[]byte("1"),
					[]byte("a"),
				},
				prot.CmdFlags{},
			),
			nil,
			prot.NewClientErr(job.ErrInvalidName.Error()),
		},*/
		{
			prot.NewCmd(
				"schedule",
				[][]byte{
					[]byte("61a444a0-6128-41c0-8078-cc757d3bd2d8"),
					[]byte("q1"),
					[]byte("-10"),
					[]byte("1000"),
					[]byte(fmtSchedTime),
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
				"schedule",
				[][]byte{
					[]byte("61a444a0-6128-41c0-8078-cc757d3bd2d8"),
					[]byte("q1"),
					[]byte("10"),
					[]byte("-1000"),
					[]byte(fmtSchedTime),
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
				"schedule",
				[][]byte{
					[]byte("61a444a0-6128-41c0-8078-cc757d3bd2d8"),
					[]byte("q1"),
					[]byte("10"),
					[]byte("1000"),
					[]byte(time.Now().UTC().Add(100 * time.Second).Format("2006-01-02T15:04:05")),
					[]byte("1"),
					[]byte("a"),
				},
				prot.CmdFlags{},
			),
			nil,
			prot.NewClientErr(job.ErrInvalidTime.Error()),
		},
		{
			prot.NewCmd(
				"schedule",
				[][]byte{
					[]byte("61a444a0-6128-41c0-8078-cc757d3bd2d8"),
					[]byte("q1"),
					[]byte("10"),
					[]byte("1000"),
					[]byte(fmtSchedTime),
					[]byte("1"),
					[]byte("aa"),
				},
				prot.CmdFlags{},
			),
			nil,
			prot.NewClientErr(job.ErrInvalidPayload.Error()),
		},
		/*{
			prot.NewCmd(
				"schedule",
				[][]byte{
					[]byte("61a444a0-6128-41c0-8078-cc757d3bd2d8"),
					[]byte("q1"),
					[]byte("10"),
					[]byte("1000"),
					[]byte(fmtSchedTime),
					[]byte(fmt.Sprintf("%d", job.MaxPayload+1)),
					make([]byte, job.MaxPayload+1),
				},
				prot.CmdFlags{},
			),
			nil,
			prot.NewClientErr(job.ErrInvalidPayload.Error()),
		},*/
		{
			prot.NewCmd(
				"add", // Wrong CMD
				[][]byte{
					[]byte("61a444a0-6128-41c0-8078-cc757d3bd2d8"),
					[]byte("q1"),
					[]byte("10"),
					[]byte("1000"),
					[]byte(fmtSchedTime),
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
				"schedule",
				[][]byte{
					[]byte("61a444a0-6128-41c0-8078-cc757d3bd2d8"),
					[]byte("q1"),
					[]byte("10"),
					[]byte("1000"),
					[]byte(fmtSchedTime),
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
				"schedule",
				[][]byte{
					[]byte("61a444a0-6128-41c0-8078-cc757d3bd2d8"),
					[]byte("q1"),
					[]byte("10"),
					[]byte("1000"),
					[]byte(fmtSchedTime),
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
				"schedule",
				[][]byte{
					[]byte("61a444a0-6128-41c0-8078-cc757d3bd2d8"),
					[]byte("q1"),
					[]byte("10"),
					[]byte("1000"),
					[]byte(fmtSchedTime),
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
				"schedule",
				[][]byte{
					[]byte("61a444a0-6128-41c0-8078-cc757d3bd2d8"),
					[]byte("q1"),
					[]byte("10"),
					[]byte("1000"),
					[]byte(fmtSchedTime),
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
				"schedule",
				[][]byte{
					[]byte("61a444a0-6128-41c0-8078-cc757d3bd2d8"),
					[]byte("q1"),
					[]byte("10"),
					[]byte("1000"),
					[]byte(fmtSchedTime),
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
				"schedule",
				[][]byte{
					[]byte("61a444a0-6128-41c0-8078-cc757d3bd2d8"),
					[]byte("q1"),
					[]byte("10"),
					[]byte("1000"),
					[]byte(fmtSchedTime),
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
				"schedule",
				[][]byte{
					[]byte("61a444a0-6128-41c0-8078-cc757d3bd2d8"),
					[]byte("q1"),
					[]byte("10"),
					[]byte("1000"),
					[]byte(fmtSchedTime),
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
				"schedule",
				[][]byte{
					[]byte("61a444a0-6128-41c0-8078-cc757d3bd2d8"),
					[]byte("q1"),
					[]byte("10"),
					[]byte("1000"),
					[]byte(fmtSchedTime),
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
				"schedule",
				[][]byte{
					[]byte("61a444a0-6128-41c0-8078-cc757d3bd2d8"),
					[]byte("q1"),
					[]byte("10"),
					[]byte("1000"),
					[]byte(fmtSchedTime),
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
				"schedule",
				[][]byte{
					[]byte("61a444a0-6128-41c0-8078-cc757d3bd2d8"),
					[]byte("q1"),
					[]byte("10"),
					[]byte("1000"),
					[]byte(fmtSchedTime),
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
				"schedule",
				[][]byte{
					[]byte("61a444a0-6128-41c0-8078-cc757d3bd2d8"),
					[]byte("q1"),
					[]byte("10"),
					[]byte("1000"),
					[]byte(fmtSchedTime),
					[]byte("1"),
					[]byte("a"),
				},
				prot.CmdFlags{"priority": []byte("-2147483649")},
			),
			nil,
			prot.NewClientErr(job.ErrInvalidPriority.Error()),
		},
		{
			prot.NewCmd(
				"schedule",
				[][]byte{
					[]byte("61a444a0-6128-41c0-8078-cc757d3bd2d8"),
					[]byte("q1"),
					[]byte("10"),
					[]byte("1000"),
					[]byte(fmtSchedTime),
					[]byte("1"),
					[]byte("a"),
				},
				prot.CmdFlags{"priority": []byte("2147483648")},
			),
			nil,
			prot.NewClientErr(job.ErrInvalidPriority.Error()),
		},
		/*{
			prot.NewCmd(
				"schedule",
				[][]byte{
					[]byte("61a444a0-6128-41c0-8078-cc757d3bd2d8"),
					[]byte("q1"),
					[]byte("10"),
					[]byte("1000"),
					[]byte(time.Now().UTC().Add(-1 * time.Second).Format(timeFormat)),
					[]byte("1"),
					[]byte("a"),
				},
				prot.CmdFlags{},
			),
			nil,
			prot.NewClientErr(job.ErrInvalidTime.Error()),
		},*/
	}

	for _, tt := range tests {
		resp, err := handler.Exec(tt.cmd)
		if !bytes.Equal(tt.resp, resp) {
			t.Fatalf("response mismatch, cmd=%s, expResp=%s, actResp=%s", tt.cmd.Args, tt.resp, resp)
		}

		if err.Error() != tt.err.Error() {
			t.Fatalf("err mismatch, cmd=%s, expErr=%s, actErr=%s", tt.cmd.Args, tt.err, err)
		}
	}
}
