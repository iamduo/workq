package handlers

import (
	"bytes"
	"testing"
	"time"

	"github.com/iamduo/workq/int/prot"
	"github.com/iamduo/workq/int/server"
)

/*func BuildInspectHandler() *InspectHandler {
  NewInspectHandler(
    NewInspectServerHandler(serverUsage, handlerUsage),
    NewInspectQueuesHandler(controller),
    NewInspectQueueHandler(controller),
    NewInspectJobsHandler(reg, controller),
    NewInspectJobHandler(reg),
  )
}*/

func TestInspectServer(t *testing.T) {
	handlerUsage := &Usage{
		EvictedJobs: 1,
	}
	started, _ := time.Parse(timeFormat, "2016-01-02T15:04:05Z")
	serverUsage := &server.Usage{
		ActiveClients: 123,
		Started:       started,
	}
	handler := NewInspectServerHandler(serverUsage, handlerUsage)

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
	handler := NewInspectServerHandler(&server.Usage{}, &Usage{})
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
