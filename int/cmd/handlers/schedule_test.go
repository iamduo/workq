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

const testTimeFormat = "2006-01-02T15:04:05Z"

func TestSchedule(t *testing.T) {
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
				"schedule",
				[][]byte{
					id,
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
				ID:      jid,
				Name:    name,
				Payload: []byte("a"),
				TTR:     1,
				TTL:     10,
			},
			cmd: prot.NewCmd(
				"schedule",
				[][]byte{
					id,
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
				ID:       jid,
				Name:     name,
				Payload:  []byte("a"),
				Priority: 10,
				TTR:      1,
				TTL:      10,
			},
			cmd: prot.NewCmd(
				"schedule",
				[][]byte{
					id,
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
				ID:          jid,
				Name:        name,
				Payload:     []byte("a"),
				MaxAttempts: 3,
				TTR:         1,
				TTL:         10,
			},
			cmd: prot.NewCmd(
				"schedule",
				[][]byte{
					id,
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
				ID:       jid,
				Name:     name,
				Payload:  []byte("a"),
				MaxFails: 3,
				TTR:      1,
				TTL:      10,
			},
			cmd: prot.NewCmd(
				"schedule",
				[][]byte{
					id,
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
				"schedule",
				[][]byte{
					id,
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
				ID:          jid,
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
					id,
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
		reg := job.NewRegistry()
		qc := wqueue.NewController()
		handler := NewScheduleHandler(reg, qc, &Usage{})

		_, ok := reg.Record(jid)
		if ok {
			t.Fatalf("Expected no record to exist")
		}

		duration := 1 * time.Second
		tt.expJob.Created = time.Now().UTC()
		fmtSchedTime := time.Now().UTC().Add(duration).Format(testTimeFormat)
		// Reparsing time from string zeroes out any nanoseconds
		tt.expJob.Time, _ = time.Parse(testTimeFormat, fmtSchedTime)
		tt.cmd.Args[schedArgTime] = []byte(fmtSchedTime)

		expRec := job.NewRunRecord()
		expRec.State = job.StateNew
		expRec.Job = tt.expJob
		expRec.Timers[job.RunRecSchedTimerIdx] = job.NewTimer(duration)

		expResp := []byte("OK\r\n")
		resp, err := handler.Exec(tt.cmd)
		if err != nil {
			t.Fatalf("Schedule err=%v", err)
		}

		if !bytes.Equal(resp, expResp) {
			t.Fatalf("Schedule response mismatch, resp=%s", resp)
		}

		rec, ok := reg.Record(jid)
		if !ok {
			t.Fatalf("Expected record to exist")
		}

		rec.Mu.RLock()
		actJob := rec.Job
		assertRunRecords(t, expRec, rec)
		rec.Mu.RUnlock()
		assertJobs(t, tt.expJob, actJob)

		if qc.Schedule(actJob) {
			t.Fatalf("Schedule job does not exist, was able to re-schedule")
		}

		resp, err = handler.Exec(tt.cmd)
		if err != ErrDuplicateJob || resp != nil {
			t.Fatalf("Expected duplicate err response, resp=%v, err=%v", resp, err)
		}

		// Scheduled time asserts
		// ----------------------
		diff := tt.expJob.Time.Sub(time.Now().UTC())
		if diff < 0 {
			t.Fatalf("Unexpected time diff for scheduled time assert")
		}

		// Additional sleep padding for test overhead
		time.Sleep(diff + (diff / 10))
		if qc.Awake(actJob) {
			t.Fatalf("Expected job to be already actively enqueued from scheduled state")
		}

		// TTL expiration asserts
		// ----------------------
		time.Sleep(time.Duration(tt.expJob.TTL) * time.Millisecond)
		if qc.Exists(actJob) {
			t.Fatalf("Expected job to be expired")
		}

		_, ok = reg.Record(jid)
		if ok {
			t.Fatalf("Expected record to be expired")
		}
	}
}

func TestScheduleCancelledScheduleTimer(t *testing.T) {
	reg := job.NewRegistry()
	qc := wqueue.NewController()
	handler := NewScheduleHandler(reg, qc, &Usage{})

	id := []byte(testutil.GenId())
	jid, _ := job.IDFromBytes(id)
	name := testutil.GenName()

	duration := 1 * time.Second
	fmtSchedTime := time.Now().UTC().Add(duration).Format(testTimeFormat)
	// Reparsing time from string zeroes out any nanoseconds
	schedTime, _ := time.Parse(testTimeFormat, fmtSchedTime)
	expJob := &job.Job{
		ID:      jid,
		Name:    name,
		Payload: []byte("a"),
		TTR:     1,
		TTL:     10,
		Created: time.Now().UTC(),
		Time:    schedTime,
	}

	cmd := prot.NewCmd(
		"schedule",
		[][]byte{
			id,
			[]byte(name),
			[]byte("1"),
			[]byte("10"),
			[]byte(fmtSchedTime),
			[]byte("1"),
			[]byte("a"),
		},
		prot.CmdFlags{},
	)

	expRec := job.NewRunRecord()
	expRec.State = job.StateNew
	expRec.Job = expJob
	expRec.Timers[job.RunRecSchedTimerIdx] = job.NewTimer(duration)

	expResp := []byte("OK\r\n")
	resp, err := handler.Exec(cmd)
	if err != nil {
		t.Fatalf("Schedule err=%v", err)
	}

	if !bytes.Equal(resp, expResp) {
		t.Fatalf("Schedule response mismatch, resp=%s", resp)
	}

	rec, ok := reg.Record(jid)
	if !ok {
		t.Fatalf("Expected record to exist")
	}

	rec.Mu.Lock()
	actJob := rec.Job
	assertRunRecords(t, expRec, rec)
	assertJobs(t, expJob, actJob)

	if qc.Schedule(actJob) {
		t.Fatalf("Schedule job does not exist, was able to re-schedule")
	}

	rec.Timers[job.RunRecSchedTimerIdx].Cancel()
	rec.Mu.Unlock()

	// Wait for scheduled time and ensure job still exists
	// 10% padding for test overhead
	time.Sleep(duration + (duration / 10))

	if qc.Schedule(actJob) {
		t.Fatalf("Job seems to have awoken after scheduled cancellation")
	}
}

// This is not expected during normal use. Testing for coverage.
func TestScheduleOutofSyncQueue(t *testing.T) {

	reg := job.NewRegistry()
	qc := &OutOfSyncController{}
	handler := NewScheduleHandler(reg, qc, &Usage{})

	fmtSchedTime := time.Now().UTC().Add(1 * time.Second).Format(testTimeFormat)
	cmd := prot.NewCmd(
		"schedule",
		[][]byte{
			[]byte(testutil.GenId()),
			[]byte(testutil.GenName()),
			[]byte("2"),
			[]byte("1"),
			[]byte(fmtSchedTime),
			[]byte("1"),
			[]byte("a"),
		},
		prot.CmdFlags{},
	)

	expErr := prot.NewServerErr("Unable to schedule job")
	resp, err := handler.Exec(cmd)
	if err == nil || resp != nil || err.Error() != expErr.Error() {
		t.Fatalf("Run response mismatch, resp=%s, err=%s", resp, err)
	}
}

func TestScheduleInternalRunScheduledCb(t *testing.T) {
	reg := job.NewRegistry()
	qc := wqueue.NewController()
	handler := NewScheduleHandler(reg, qc, &Usage{})

	j := job.NewEmptyJob()
	j.ID, _ = job.IDFromBytes([]byte(testutil.GenId()))
	j.Name = testutil.GenName()
	j.Created = time.Now().UTC()
	j.Time = j.Created.Add(1 * time.Second)
	j.TTL = 1
	if !qc.Schedule(j) {
		t.Fatalf("Unable to schedule job")
	}

	rec := job.NewRunRecord()
	rec.Job = j

	// Pass #1 - No Run Record
	handler.runScheduled(j.ID)

	// Assert runScheduled NOOP
	if rec.Timers[job.RunRecTTLTimerIdx] != nil {
		t.Fatalf("Unexpected TTL timer started")
	}

	if qc.Schedule(j) {
		t.Fatalf("Expected original scheduled job to exist")
	}

	// Pass #2 - Valid Input
	if !reg.Add(rec) {
		t.Fatalf("Unable to add run record")
	}
	handler.runScheduled(j.ID)

	if rec.Timers[job.RunRecTTLTimerIdx] == nil {
		t.Fatalf("Expected TTL timer to be started")
	}

	// Additional sleep padding for test overhead
	time.Sleep(time.Duration(j.TTL) * time.Millisecond)
	time.Sleep(500 * time.Microsecond)

	if qc.Exists(j) {
		t.Fatalf("Expected job to be expired")
	}

	_, ok := reg.Record(j.ID)
	if ok {
		t.Fatalf("Expected job run record to be expired")
	}

	// Pass #3 - TTL Cancellation
	if !reg.Add(rec) {
		t.Fatalf("Unable to add run record")
	}
	if !qc.Add(j) {
		t.Fatalf("Unable to add job")
	}

	handler.runScheduled(j.ID)
	rec.Timers[job.RunRecTTLTimerIdx].Cancel()

	// Additional sleep padding for test overhead
	time.Sleep(time.Duration(j.TTL) * time.Millisecond)
	time.Sleep(500 * time.Microsecond)

	if !qc.Exists(j) {
		t.Fatalf("Expected job to still exist, TTL cancellation")
	}

	_, ok = reg.Record(j.ID)
	if !ok {
		t.Fatalf("Expected job run record to still exist, TTL cancellation")
	}
}

func TestScheduleInvalidArgs(t *testing.T) {
	reg := job.NewRegistry()
	qc := wqueue.NewController()
	handler := NewScheduleHandler(reg, qc, &Usage{})

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
		{
			prot.NewCmd(
				"schedule",
				[][]byte{
					[]byte("00000000-0000-0000-0000-000000000000"),
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
		},
		{
			prot.NewCmd(
				"schedule",
				[][]byte{
					[]byte("00000000-0000-0000-0000-000000000000"),
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
					[]byte("00000000-0000-0000-0000-000000000000"),
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
					[]byte("00000000-0000-0000-0000-000000000000"),
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
					[]byte("00000000-0000-0000-0000-000000000000"),
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
		{
			prot.NewCmd(
				"schedule",
				[][]byte{
					[]byte("00000000-0000-0000-0000-000000000000"),
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
		},
		{
			prot.NewCmd(
				"add", // Wrong CMD
				[][]byte{
					[]byte("00000000-0000-0000-0000-000000000000"),
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
					[]byte("00000000-0000-0000-0000-000000000000"),
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
					[]byte("00000000-0000-0000-0000-000000000000"),
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
					[]byte("00000000-0000-0000-0000-000000000000"),
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
					[]byte("00000000-0000-0000-0000-000000000000"),
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
					[]byte("00000000-0000-0000-0000-000000000000"),
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
					[]byte("00000000-0000-0000-0000-000000000000"),
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
					[]byte("00000000-0000-0000-0000-000000000000"),
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
					[]byte("00000000-0000-0000-0000-000000000000"),
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
					[]byte("00000000-0000-0000-0000-000000000000"),
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
					[]byte("00000000-0000-0000-0000-000000000000"),
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
					[]byte("00000000-0000-0000-0000-000000000000"),
					[]byte("q1"),
					[]byte("10"),
					[]byte("1000"),
					[]byte(fmtSchedTime),
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
				"schedule",
				[][]byte{
					[]byte("00000000-0000-0000-0000-000000000000"),
					[]byte("q1"),
					[]byte("10"),
					[]byte("1000"),
					[]byte(fmtSchedTime),
					[]byte("1"),
					[]byte("a"),
				},
				prot.CmdFlags{"priority": []byte("4294967296")},
			),
			nil,
			prot.NewClientErr(job.ErrInvalidPriority.Error()),
		},
		{
			prot.NewCmd(
				"schedule",
				[][]byte{
					[]byte("00000000-0000-0000-0000-000000000000"),
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
		},
	}

	for _, tt := range tests {
		resp, err := handler.Exec(tt.cmd)
		if !bytes.Equal(tt.resp, resp) {
			t.Fatalf("response mismatch, cmd=%s, expResp=%s, actResp=%s", tt.cmd.Args, tt.resp, resp)
		}

		if err.Error() != tt.err.Error() {
			t.Fatalf("err mismatch, cmd=%s, expErr=%s, actErr=%s", tt.cmd.Args, tt.err, err)
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
