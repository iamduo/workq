package handlers

import (
	"bytes"
	"testing"
	"time"

	"github.com/iamduo/workq/int/job"
	"github.com/iamduo/workq/int/prot"
	"github.com/iamduo/workq/int/testutil"
)

func TestInspectQueues(t *testing.T) {
	qc := job.NewQueueController()
	handler := NewInspectQueuesHandler(qc)
	// Generates an empty queue
	qc.Queue("q1")

	j := job.NewEmptyJob()
	j.ID, _ = parseID([]byte(testutil.GenIDString()))
	j.Name = "q2"
	qc.Add(j)

	j = job.NewEmptyJob()
	j.ID, _ = parseID([]byte(testutil.GenIDString()))
	j.Name = "q3"
	j.Time = time.Now().UTC().Add(1 * time.Second)
	qc.Schedule(j)

	cmd := prot.NewCmd(
		"inspect",
		[][]byte{
			[]byte("queues"),
			[]byte("4"),
			[]byte("1"),
		},
		prot.CmdFlags{},
	)
	expResp := []byte("OK 0\r\n")
	resp, err := handler.Exec(cmd)
	if err != nil || !bytes.Equal(expResp, resp) {
		t.Fatalf("Response mismatch, resp=%s, err=%s", resp, err)
	}

	cmd = prot.NewCmd(
		"inspect",
		[][]byte{
			[]byte("queues"),
			[]byte("0"),
			[]byte("1"),
		},
		prot.CmdFlags{},
	)

	expResp = []byte(
		"OK 1\r\n" +
			"q1 2\r\n" +
			"ready-len 0\r\n" +
			"scheduled-len 0\r\n",
	)
	resp, err = handler.Exec(cmd)
	if err != nil || !bytes.Equal(expResp, resp) {
		t.Fatalf("Response mismatch, resp=%s, err=%s", resp, err)
	}

	cmd = prot.NewCmd(
		"inspect",
		[][]byte{
			[]byte("queues"),
			[]byte("1"),
			[]byte("1"),
		},
		prot.CmdFlags{},
	)

	expResp = []byte(
		"OK 1\r\n" +
			"q2 2\r\n" +
			"ready-len 1\r\n" +
			"scheduled-len 0\r\n",
	)
	resp, err = handler.Exec(cmd)
	if err != nil || !bytes.Equal(expResp, resp) {
		t.Fatalf("Response mismatch, resp=%s, err=%s", resp, err)
	}

	cmd = prot.NewCmd(
		"inspect",
		[][]byte{
			[]byte("queues"),
			[]byte("0"),
			[]byte("10"),
		},
		prot.CmdFlags{},
	)

	expResp = []byte(
		"OK 3\r\n" +
			"q1 2\r\n" +
			"ready-len 0\r\n" +
			"scheduled-len 0\r\n" +
			"q2 2\r\n" +
			"ready-len 1\r\n" +
			"scheduled-len 0\r\n" +
			"q3 2\r\n" +
			"ready-len 0\r\n" +
			"scheduled-len 1\r\n",
	)
	resp, err = handler.Exec(cmd)
	if err != nil || !bytes.Equal(expResp, resp) {
		t.Fatalf("Response mismatch, resp=%s, err=%s", resp, err)
	}
}

func TestInspectQueuesInvalidArgs(t *testing.T) {
	qc := job.NewQueueController()
	handler := NewInspectQueuesHandler(qc)
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
				[][]byte{[]byte("queues")},
				prot.CmdFlags{},
			),
			nil,
			prot.ErrInvalidCmdArgs,
		},
		{
			prot.NewCmd(
				"inspect",
				[][]byte{
					[]byte("queues"),
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
					[]byte("queues"),
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
					[]byte("queues"),
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
					[]byte("queues"),
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
					[]byte("not-queues"),
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
					[]byte("queues"),
					[]byte("0"),
					[]byte("1"),
				},
				prot.CmdFlags{},
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
