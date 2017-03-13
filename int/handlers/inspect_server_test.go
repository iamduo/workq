package handlers

import (
	"bytes"
	"testing"
	"time"

	"github.com/iamduo/workq/int/job"
	"github.com/iamduo/workq/int/prot"
	"github.com/iamduo/workq/int/server"
)

type testServerStater struct {
	stats server.Stats
}

func (s *testServerStater) Stats() server.Stats {
	return s.stats
}

type testJobStater struct {
	stats job.Stats
}

func (s *testJobStater) Stats() job.Stats {
	return s.stats
}

func TestInspectServer(t *testing.T) {
	started, _ := time.Parse(timeFormat, "2016-01-02T15:04:05Z")
	serverStats := server.Stats{
		ActiveClients: 123,
		Started:       started,
	}
	jobStats := job.Stats{
		EvictedJobs: 1,
	}
	handler := NewInspectServerHandler(
		&testServerStater{serverStats},
		&testJobStater{jobStats},
	)

	cmd := prot.NewCmd(
		"inspect",
		[][]byte{
			[]byte("server"),
		},
		prot.CmdFlags{},
	)

	expResp := []byte(
		"OK 1\r\n" +
			"server 3\r\n" +
			"active-clients 123\r\n" +
			"evicted-jobs 1\r\n" +
			"started 2016-01-02T15:04:05Z\r\n",
	)
	resp, err := handler.Exec(cmd)
	if err != nil || bytes.Equal(expResp, resp) {
		t.Fatalf("Response mismatch, resp=%s, err=%s", resp, err)
	}
}

func TestInspectServerInvalidArgs(t *testing.T) {
	handler := NewInspectServerHandler(
		&testServerStater{server.Stats{}},
		&testJobStater{job.Stats{}},
	)
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
				[][]byte{[]byte("server")},
				prot.CmdFlags{"test": []byte("1")},
			),
			nil,
			prot.ErrInvalidCmdArgs,
		},
		{
			prot.NewCmd(
				"insp",
				[][]byte{[]byte("server")},
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
