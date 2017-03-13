package handlers

import "github.com/iamduo/workq/int/prot"

type UnknownHandler struct{}

func (h *UnknownHandler) Exec(cmd *prot.Cmd) ([]byte, error) {
	return nil, prot.ErrUnknownCmd
}
