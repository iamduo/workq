package handlers

import (
	"bytes"
	"testing"

	"github.com/iamduo/workq/int/job"
	"github.com/iamduo/workq/int/prot"
	"github.com/iamduo/workq/int/testutil"
)

func TestInspectQueue(t *testing.T) {
	qc := job.NewQueueController()
	handler := NewInspectQueueHandler(qc)

	cmd := prot.NewCmd(
		"inspect",
		[][]byte{
			[]byte("queue"),
			[]byte("q1"),
		},
		prot.CmdFlags{},
	)

	expErr := prot.ErrNotFound
	resp, err := handler.Exec(cmd)
	if err != expErr || resp != nil {
		t.Fatalf("Response mismatch, resp=%s, err=%s", resp, err)
	}

	qc.Queue("q1")

	expResp := []byte(
		"OK 1\r\n" +
			"q1 2\r\n" +
			"ready-len 0\r\n" +
			"scheduled-len 0\r\n",
	)
	resp, err = handler.Exec(cmd)
	if err != nil || !bytes.Equal(expResp, resp) {
		t.Fatalf("Response mismatch, resp=%s, err=%s", resp, err)
	}

	j := job.NewEmptyJob()
	j.ID, _ = parseID([]byte(testutil.GenIDString()))
	j.Name = "q1"
	qc.Add(j)

	expResp = []byte(
		"OK 1\r\n" +
			"q1 2\r\n" +
			"ready-len 1\r\n" +
			"scheduled-len 0\r\n",
	)
	resp, err = handler.Exec(cmd)
	if err != nil || !bytes.Equal(expResp, resp) {
		t.Fatalf("Response mismatch, resp=%s, err=%s", resp, err)
	}

	j = job.NewEmptyJob()
	j.ID, _ = parseID([]byte(testutil.GenIDString()))
	j.Name = "q1"
	qc.Schedule(j)

	expResp = []byte(
		"OK 1\r\n" +
			"q1 2\r\n" +
			"ready-len 1\r\n" +
			"scheduled-len 1\r\n",
	)
	resp, err = handler.Exec(cmd)
	if err != nil || !bytes.Equal(expResp, resp) {
		t.Fatalf("Response mismatch, resp=%s, err=%s", resp, err)
	}
}

func TestInspectQueueInvalidArgs(t *testing.T) {
	qc := job.NewQueueController()
	handler := NewInspectQueueHandler(qc)
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
				[][]byte{[]byte("queue")},
				prot.CmdFlags{},
			),
			nil,
			prot.ErrInvalidCmdArgs,
		},
		{
			prot.NewCmd(
				"inspect",
				[][]byte{[]byte("queue"), []byte("*")},
				prot.CmdFlags{},
			),
			nil,
			prot.NewClientErr(job.ErrInvalidName.Error()),
		},
		{
			prot.NewCmd(
				"inspect",
				[][]byte{[]byte("queue"), []byte("q1")},
				prot.CmdFlags{"test": []byte("test")},
			),
			nil,
			prot.ErrInvalidCmdArgs,
		},
		// WRONG CMD
		{
			prot.NewCmd(
				"inspect",
				[][]byte{[]byte("qqqueue"), []byte("q1")},
				prot.CmdFlags{},
			),
			nil,
			prot.ErrInvalidCmdArgs,
		},
		{
			prot.NewCmd(
				"insp",
				[][]byte{[]byte("queue"), []byte("q1")},
				prot.CmdFlags{"test": []byte("test")},
			),
			nil,
			prot.ErrInvalidCmdArgs,
		},
	}

	for i, tt := range tests {
		resp, err := handler.Exec(tt.cmd)
		if !bytes.Equal(tt.resp, resp) {
			t.Fatalf("Response mismatch, i=%d  expResp=%s, actResp=%s", i, tt.resp, resp)
		}

		if err.Error() != tt.err.Error() {
			t.Fatalf("Err mismatch, cmd=%s, expErr=%s, actErr=%s", tt.cmd.Args, tt.err, err)
		}
	}
}
