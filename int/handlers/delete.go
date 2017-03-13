package handlers

import (
	"github.com/iamduo/workq/int/job"
	"github.com/iamduo/workq/int/prot"
)

const (
	// delete cmd args index
	deleteArgID = 0
)

type DeleteHandler struct {
	jc job.Deleter
}

func NewDeleteHandler(jc job.Deleter) *DeleteHandler {
	return &DeleteHandler{
		jc: jc,
	}
}

// `delete` <id>
//
// Delete a job by id.
//
// Returns:
// CLIENT-ERROR on invalid input
// NOT-FOUND if job does not exist
// OK if successful
func (h *DeleteHandler) Exec(cmd *prot.Cmd) ([]byte, error) {
	if cmd.Name != "delete" || cmd.ArgC != 1 || cmd.FlagC > 0 {
		return nil, prot.ErrInvalidCmdArgs
	}

	id, err := parseID(cmd.Args[deleteArgID])
	if err != nil {
		return nil, prot.NewClientErr(err.Error())
	}

	err = h.jc.Delete(id)
	if err == job.ErrNotFound {
		return nil, prot.ErrNotFound
	}

	return prot.OkResp(), nil
}
