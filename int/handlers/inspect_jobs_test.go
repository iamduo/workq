package handlers

import (
	"bytes"
	"testing"
	"time"

	"github.com/iamduo/workq/int/job"
	"github.com/iamduo/workq/int/prot"
)

func TestInspectJobs(t *testing.T) {
	reg := job.NewRegistry()
	qc := job.NewQueueController()
	handler := NewInspectJobsHandler(reg, qc)

	cmd := prot.NewCmd(
		"inspect",
		[][]byte{
			[]byte("jobs"),
			[]byte("q1"),
			[]byte("0"),
			[]byte("2"),
		},
		prot.CmdFlags{},
	)
	expErr := prot.ErrNotFound
	resp, err := handler.Exec(cmd)
	if err != expErr || resp != nil {
		t.Fatalf("Response mismatch, resp=%s, err=%s", resp, err)
	}

	// Generates an empty queue
	qc.Queue("q1")

	expResp := []byte("OK 0\r\n")
	resp, err = handler.Exec(cmd)
	if err != nil || !bytes.Equal(expResp, resp) {
		t.Fatalf("Response mismatch, resp=%s, err=%s", resp, err)
	}

	j := &job.Job{}
	j.ID, _ = parseID([]byte("61a444a0-6128-41c0-8078-cc757d3bd2d8"))
	j.Name = "q1"
	j.TTR = 1000
	j.TTL = 60000
	j.Payload = []byte("a")
	j.MaxAttempts = 3
	j.MaxFails = 1
	j.Priority = 100
	j.Created, _ = time.Parse(testTimeFormat, "2016-06-13T14:08:18Z")

	rec := job.NewRunRecord()
	rec.Job = j
	rec.Attempts = 1
	rec.State = job.StatePending
	reg.Add(rec)
	qc.Add(j)

	expResp = []byte(
		"OK 1\r\n" +
			"61a444a0-6128-41c0-8078-cc757d3bd2d8 12\r\n" +
			"name q1\r\n" +
			"ttr 1000\r\n" +
			"ttl 60000\r\n" +
			"payload-size 1\r\n" +
			"payload a\r\n" +
			"max-attempts 3\r\n" +
			"attempts 1\r\n" +
			"max-fails 1\r\n" +
			"fails 0\r\n" +
			"priority 100\r\n" +
			"state 3\r\n" +
			"created 2016-06-13T14:08:18Z\r\n",
	)
	resp, err = handler.Exec(cmd)
	if err != nil || !bytes.Equal(expResp, resp) {
		t.Fatalf("Response mismatch, resp=%+v, err=%s", resp, err)
	}

	jb := *j
	jb.ID, _ = parseID([]byte("d942e06a-1bb0-49fd-9d93-29d87d75d64c"))
	jb.Created, _ = time.Parse(testTimeFormat, "2016-06-13T14:09:00Z")
	rec = job.NewRunRecord()
	rec.Job = &jb
	rec.Attempts = 1
	rec.State = job.StateLeased
	reg.Add(rec)
	qc.Add(&jb)

	expResp = []byte(
		"OK 2\r\n" +
			"61a444a0-6128-41c0-8078-cc757d3bd2d8 12\r\n" +
			"name q1\r\n" +
			"ttr 1000\r\n" +
			"ttl 60000\r\n" +
			"payload-size 1\r\n" +
			"payload a\r\n" +
			"max-attempts 3\r\n" +
			"attempts 1\r\n" +
			"max-fails 1\r\n" +
			"fails 0\r\n" +
			"priority 100\r\n" +
			"state 3\r\n" +
			"created 2016-06-13T14:08:18Z\r\n" +
			"d942e06a-1bb0-49fd-9d93-29d87d75d64c 12\r\n" +
			"name q1\r\n" +
			"ttr 1000\r\n" +
			"ttl 60000\r\n" +
			"payload-size 1\r\n" +
			"payload a\r\n" +
			"max-attempts 3\r\n" +
			"attempts 1\r\n" +
			"max-fails 1\r\n" +
			"fails 0\r\n" +
			"priority 100\r\n" +
			"state 4\r\n" +
			"created 2016-06-13T14:09:00Z\r\n",
	)
	resp, err = handler.Exec(cmd)
	if err != nil || !bytes.Equal(expResp, resp) {
		t.Fatalf("Response mismatch, resp=%s, err=%s", resp, err)
	}

	// 3rd job is unreachable due to limit
	jc := *j
	jc.ID, _ = parseID([]byte("13a0e776-c167-4854-b927-9e8aed5228f9"))
	jc.Created, _ = time.Parse(testTimeFormat, "2016-06-13T14:10:00Z")
	rec = job.NewRunRecord()
	rec.Job = &jc
	rec.Attempts = 1
	rec.State = job.StateLeased
	reg.Add(rec)
	ok := qc.Add(&jc)
	if !ok {
		t.Fatalf("Unable to add job")
	}

	resp, err = handler.Exec(cmd)
	if err != nil || !bytes.Equal(expResp, resp) {
		t.Fatalf("Response mismatch, resp=%s, err=%s", resp, err)
	}
}

func TestInspectJobsScheduled(t *testing.T) {
	reg := job.NewRegistry()
	qc := job.NewQueueController()
	handler := NewInspectJobsHandler(reg, qc)

	cmd := prot.NewCmd(
		"inspect",
		[][]byte{
			[]byte("scheduled-jobs"),
			[]byte("q1"),
			[]byte("0"),
			[]byte("2"),
		},
		prot.CmdFlags{},
	)
	expErr := prot.ErrNotFound
	resp, err := handler.Exec(cmd)
	if err != expErr || resp != nil {
		t.Fatalf("Response mismatch, resp=%s, err=%s", resp, err)
	}

	// Generates an empty queue
	qc.Queue("q1")

	expResp := []byte("OK 0\r\n")
	resp, err = handler.Exec(cmd)
	if err != nil || !bytes.Equal(expResp, resp) {
		t.Fatalf("Response mismatch, resp=%s, err=%s", resp, err)
	}

	j := &job.Job{}
	j.ID, _ = parseID([]byte("61a444a0-6128-41c0-8078-cc757d3bd2d8"))
	j.Name = "q1"
	j.TTR = 1000
	j.TTL = 60000
	j.Payload = []byte("a")
	j.MaxAttempts = 3
	j.MaxFails = 1
	j.Priority = 100
	j.Created, _ = time.Parse(testTimeFormat, "2016-06-13T14:08:18Z")
	j.Time, _ = time.Parse(testTimeFormat, "2016-06-13T15:08:18Z")

	rec := job.NewRunRecord()
	rec.Job = j
	reg.Add(rec)
	qc.Schedule(j)

	expResp = []byte(
		"OK 1\r\n" +
			"61a444a0-6128-41c0-8078-cc757d3bd2d8 13\r\n" +
			"name q1\r\n" +
			"ttr 1000\r\n" +
			"ttl 60000\r\n" +
			"payload-size 1\r\n" +
			"payload a\r\n" +
			"max-attempts 3\r\n" +
			"attempts 0\r\n" +
			"max-fails 1\r\n" +
			"fails 0\r\n" +
			"priority 100\r\n" +
			"state 0\r\n" +
			"created 2016-06-13T14:08:18Z\r\n" +
			"time 2016-06-13T15:08:18Z\r\n",
	)
	resp, err = handler.Exec(cmd)
	if err != nil || !bytes.Equal(expResp, resp) {
		t.Fatalf("Response mismatch, resp=%s, err=%s", resp, err)
	}

	jb := *j
	jb.ID, _ = parseID([]byte("61a444a0-6128-41c0-8078-cc757d3bd2d8"))
	jb.Created, _ = time.Parse(testTimeFormat, "2016-06-13T14:09:00Z")
	jb.Time, _ = time.Parse(testTimeFormat, "2016-06-13T15:09:00Z")
	rec = job.NewRunRecord()
	rec.Job = &jb
	reg.Add(rec)
	qc.Schedule(&jb)

	expResp = []byte(
		"OK 2\r\n" +
			"61a444a0-6128-41c0-8078-cc757d3bd2d8 13\r\n" +
			"name q1\r\n" +
			"ttr 1000\r\n" +
			"ttl 60000\r\n" +
			"payload-size 1\r\n" +
			"payload a\r\n" +
			"max-attempts 3\r\n" +
			"attempts 0\r\n" +
			"max-fails 1\r\n" +
			"fails 0\r\n" +
			"priority 100\r\n" +
			"state 0\r\n" +
			"created 2016-06-13T14:08:18Z\r\n" +
			"time 2016-06-13T15:08:18Z\r\n" +
			"61a444a0-6128-41c0-8078-cc757d3bd2d8 13\r\n" +
			"name q1\r\n" +
			"ttr 1000\r\n" +
			"ttl 60000\r\n" +
			"payload-size 1\r\n" +
			"payload a\r\n" +
			"max-attempts 3\r\n" +
			"attempts 0\r\n" +
			"max-fails 1\r\n" +
			"fails 0\r\n" +
			"priority 100\r\n" +
			"state 0\r\n" +
			"created 2016-06-13T14:09:00Z\r\n" +
			"time 2016-06-13T15:09:00Z\r\n",
	)
	resp, err = handler.Exec(cmd)
	if err != nil || !bytes.Equal(expResp, resp) {
		t.Fatalf("Response mismatch, resp=%s, err=%s", resp, err)
	}

	// 3rd job is unreachable due to limit
	jc := *j
	jc.ID, _ = parseID([]byte("6ba7b810-9dad-11d1-80b4-00c04fd430c6"))
	jc.Created, _ = time.Parse(testTimeFormat, "2016-06-13T14:10:00Z")
	jc.Time, _ = time.Parse(testTimeFormat, "2016-06-13T15:10:00Z")
	rec = job.NewRunRecord()
	rec.Job = &jc
	reg.Add(rec)
	ok := qc.Schedule(&jc)
	if !ok {
		t.Fatalf("Unable to add job")
	}

	resp, err = handler.Exec(cmd)
	if err != nil || !bytes.Equal(expResp, resp) {
		t.Fatalf("Response mismatch, resp=%s, err=%s", resp, err)
	}
}

func TestInspectJobsOutOfRangeSeek(t *testing.T) {
	reg := job.NewRegistry()
	qc := job.NewQueueController()
	handler := NewInspectJobsHandler(reg, qc)

	// Generate empty queue
	qc.Queue("q1")

	cmd := prot.NewCmd(
		"inspect",
		[][]byte{
			[]byte("jobs"),
			[]byte("q1"),
			[]byte("1"),
			[]byte("2"),
		},
		prot.CmdFlags{},
	)
	expResp := []byte("OK 0\r\n")
	resp, err := handler.Exec(cmd)
	if err != nil || !bytes.Equal(expResp, resp) {
		t.Fatalf("Response mismatch, resp=%s, err=%s", resp, err)
	}
}

// When job record is missing while scanning.
// Expected if a job is deleted or expired while scanning.
func TestInspectJobsOutOfSyncQueue(t *testing.T) {
	reg := job.NewRegistry()
	qc := job.NewQueueController()
	handler := NewInspectJobsHandler(reg, qc)

	// Generate empty queue
	j := job.NewEmptyJob()
	j.ID, _ = parseID([]byte("61a444a0-6128-41c0-8078-cc757d3bd2d8"))
	j.Name = "q1"
	qc.Add(j)

	cmd := prot.NewCmd(
		"inspect",
		[][]byte{
			[]byte("jobs"),
			[]byte("q1"),
			[]byte("0"),
			[]byte("1"),
		},
		prot.CmdFlags{},
	)
	expResp := []byte("OK 0\r\n")
	resp, err := handler.Exec(cmd)
	if err != nil || !bytes.Equal(expResp, resp) {
		t.Fatalf("Response mismatch, resp=%s, err=%s", resp, err)
	}
}

func TestInspectJobsInvalidArgs(t *testing.T) {
	reg := job.NewRegistry()
	qc := job.NewQueueController()
	handler := NewInspectJobsHandler(reg, qc)
	tests := []struct {
		cmd  *prot.Cmd
		resp []byte
		err  error
	}{
		{
			prot.NewCmd(
				"inspect",
				[][]byte{},
				prot.CmdFlags{},
			),
			nil,
			prot.ErrInvalidCmdArgs,
		},
		{
			prot.NewCmd(
				"inspect",
				[][]byte{[]byte("")},
				prot.CmdFlags{},
			),
			nil,
			prot.ErrInvalidCmdArgs,
		},
		{
			prot.NewCmd(
				"inspect",
				[][]byte{[]byte("jobs")},
				prot.CmdFlags{},
			),
			nil,
			prot.ErrInvalidCmdArgs,
		},
		{
			prot.NewCmd(
				"inspect",
				[][]byte{
					[]byte("jobs"),
					[]byte("q1"),
				},
				prot.CmdFlags{},
			),
			nil,
			prot.ErrInvalidCmdArgs,
		},
		{
			prot.NewCmd(
				"inspect",
				[][]byte{
					[]byte("jobs"),
					[]byte("q1"),
					[]byte("0"),
				},
				prot.CmdFlags{},
			),
			nil,
			prot.ErrInvalidCmdArgs,
		},
		{
			prot.NewCmd(
				"inspect",
				[][]byte{
					[]byte("jobs"),
					[]byte("q1"),
					[]byte("0"),
					[]byte("1"),
				},
				prot.CmdFlags{"test": []byte("test")},
			),
			nil,
			prot.ErrInvalidCmdArgs,
		},
		{
			prot.NewCmd(
				"inspect",
				[][]byte{
					[]byte("jobs"),
					[]byte("*"),
					[]byte("0"),
					[]byte("1"),
				},
				prot.CmdFlags{},
			),
			nil,
			prot.NewClientErr(job.ErrInvalidName.Error()),
		},
		{
			prot.NewCmd(
				"inspect",
				[][]byte{
					[]byte("jobs"),
					[]byte("q1"),
					[]byte("-1"),
					[]byte("1"),
				},
				prot.CmdFlags{},
			),
			nil,
			ErrInvalidCursorOffset,
		},
		{
			prot.NewCmd(
				"inspect",
				[][]byte{
					[]byte("jobs"),
					[]byte("q1"),
					[]byte("0"),
					[]byte("-1"),
				},
				prot.CmdFlags{},
			),
			nil,
			ErrInvalidLimit,
		},
		// WRONG CMD
		{
			prot.NewCmd(
				"inspect",
				[][]byte{
					[]byte("not-jobs"),
					[]byte("q1"),
					[]byte("0"),
					[]byte("1"),
				},
				prot.CmdFlags{},
			),
			nil,
			prot.ErrInvalidCmdArgs,
		},
		{
			prot.NewCmd(
				"insp",
				[][]byte{
					[]byte("jobs"),
					[]byte("q1"),
					[]byte("0"),
					[]byte("1"),
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
			t.Fatalf("Response mismatch, expResp=%s, actResp=%s", tt.resp, resp)
		}

		if err.Error() != tt.err.Error() {
			t.Fatalf("Err mismatch, cmd=%s, expErr=%s, actErr=%s", tt.cmd.Args, tt.err, err)
		}
	}
}
