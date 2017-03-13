package handlers

import (
	"testing"

	"github.com/iamduo/workq/int/prot"
)

func TestUnknownHandler(t *testing.T) {
	handler := &UnknownHandler{}
	resp, err := handler.Exec(prot.NewCmd("unknown", [][]byte{}, prot.CmdFlags{}))
	if resp != nil && err != prot.ErrUnknownCmd {
		t.Fatalf("UnknownHandler response mismatch, resp=%s, err=%s", resp, err)
	}
}
