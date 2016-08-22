package handlers

import (
	"bytes"
	"testing"

	"github.com/iamduo/workq/int/prot"
)

type TestHandler struct {
	AssertCmd func(cmd *prot.Cmd)
}

func (h *TestHandler) Exec(cmd *prot.Cmd) ([]byte, error) {
	h.AssertCmd(cmd)
	return nil, nil
}

func TestInspect(t *testing.T) {
	createAssert := func(subCommands []string) func(cmd *prot.Cmd) {
		return func(cmd *prot.Cmd) {
			if cmd.ArgC < 1 {
				t.Fatalf("Inspect cmd mismatch")
			}

			var match bool
			for _, v := range subCommands {
				if bytes.Equal(cmd.Args[inspectArgObject], []byte(v)) {
					match = true
					break
				}
			}

			if !match {
				t.Fatalf("Inspect sub cmd mismatch")
			}
		}
	}

	handler := NewInspectHandler(
		&TestHandler{createAssert([]string{"server"})},
		&TestHandler{createAssert([]string{"queues"})},
		&TestHandler{createAssert([]string{"queue"})},
		&TestHandler{createAssert([]string{"jobs", "scheduled-jobs"})},
		&TestHandler{createAssert([]string{"job"})},
	)

	cmd := prot.NewCmd("inspect", [][]byte{[]byte("server")}, prot.CmdFlags{})
	resp, err := handler.Exec(cmd)
	if resp == nil && err == prot.ErrUnknownCmd {
		t.Fatalf("Inspect server did not match")
	}

	cmd = prot.NewCmd("inspect", [][]byte{[]byte("queues")}, prot.CmdFlags{})
	resp, err = handler.Exec(cmd)
	if resp == nil && err == prot.ErrUnknownCmd {
		t.Fatalf("Inspect queues did not match")
	}

	cmd = prot.NewCmd("inspect", [][]byte{[]byte("queue")}, prot.CmdFlags{})
	resp, err = handler.Exec(cmd)
	if resp == nil && err == prot.ErrUnknownCmd {
		t.Fatalf("Inspect queue did not match")
	}

	cmd = prot.NewCmd("inspect", [][]byte{[]byte("jobs")}, prot.CmdFlags{})
	resp, err = handler.Exec(cmd)
	if resp == nil && err == prot.ErrUnknownCmd {
		t.Fatalf("Inspect jobs did not match")
	}

	cmd = prot.NewCmd("inspect", [][]byte{[]byte("scheduled-jobs")}, prot.CmdFlags{})
	resp, err = handler.Exec(cmd)
	if resp == nil && err == prot.ErrUnknownCmd {
		t.Fatalf("Inspect scheduled-jobs did not match")
	}

	cmd = prot.NewCmd("inspect", [][]byte{[]byte("job")}, prot.CmdFlags{})
	resp, err = handler.Exec(cmd)
	if resp == nil && err == prot.ErrUnknownCmd {
		t.Fatalf("Inspect job did not match")
	}
}

func TestInspectNotFound(t *testing.T) {
	handler := &InspectHandler{}
	cmd := prot.NewCmd("inspect", [][]byte{[]byte("bad")}, prot.CmdFlags{})
	resp, err := handler.Exec(cmd)
	if resp != nil || err != prot.ErrUnknownCmd {
		t.Fatalf("Inspect not found mismatch, resp=%s, err=%s", resp, err)
	}
}

func TestInspectInvalidArgs(t *testing.T) {
	handler := &InspectHandler{}
	cmd := prot.NewCmd("insp", [][]byte{[]byte("jobs")}, prot.CmdFlags{})
	resp, err := handler.Exec(cmd)
	if resp != nil || err != prot.ErrInvalidCmdArgs {
		t.Fatalf("Inspect not found mismatch, resp=%s, err=%s", resp, err)
	}
}
