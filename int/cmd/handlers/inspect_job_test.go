package handlers

import (
	"bytes"
	"testing"
	"time"

	"github.com/iamduo/workq/int/job"
	"github.com/iamduo/workq/int/prot"
)

func TestInspectJob(t *testing.T) {
	reg := job.NewRegistry()
	handler := NewInspectJobHandler(reg)

	cmd := prot.NewCmd(
		"inspect",
		[][]byte{
			[]byte("job"),
			[]byte("6ba7b810-9dad-11d1-80b4-00c04fd430c4"),
		},
		prot.CmdFlags{},
	)

	expErr := prot.ErrNotFound
	resp, err := handler.Exec(cmd)
	if err != expErr || resp != nil {
		t.Fatalf("Response mismatch, resp=%s, err=%s", resp, err)
	}

	j := &job.Job{}
	j.Name = "q1"
	j.ID, _ = job.IDFromBytes(cmd.Args[1])
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

	expResp := []byte(
		"OK 1\r\n" +
			"6ba7b810-9dad-11d1-80b4-00c04fd430c4 12\r\n" +
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
		t.Fatalf("Response mismatch, resp=%s, err=%s", resp, err)
	}
}

func TestInspectJobInvalidArgs(t *testing.T) {
	reg := job.NewRegistry()
	handler := NewInspectJobHandler(reg)
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
				[][]byte{[]byte("job")},
				prot.CmdFlags{},
			),
			nil,
			prot.ErrInvalidCmdArgs,
		},
		{
			prot.NewCmd(
				"inspect",
				[][]byte{
					[]byte("job"),
					[]byte("*"),
				},
				prot.CmdFlags{},
			),
			nil,
			prot.NewClientErr(job.ErrInvalidID.Error()),
		},
		{
			prot.NewCmd(
				"inspect",
				[][]byte{
					[]byte("job"),
					[]byte("6ba7b810-9dad-11d1-80b4-00c04fd430c4"),
				},
				prot.CmdFlags{"test": []byte("test")},
			),
			nil,
			prot.ErrInvalidCmdArgs,
		},
		// WRONG CMD
		{
			prot.NewCmd(
				"inspect",
				[][]byte{
					[]byte("not-job"),
					[]byte("6ba7b810-9dad-11d1-80b4-00c04fd430c4"),
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
					[]byte("job"),
					[]byte("6ba7b810-9dad-11d1-80b4-00c04fd430c4"),
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
