package handlers

import (
	"github.com/iamduo/workq/int/job"
	"github.com/iamduo/workq/int/prot"
	"github.com/iamduo/workq/int/wqueue"
)

const (
	// delete cmd args index
	deleteArgID = 0
)

type DeleteHandler struct {
	registry *job.Registry
	qc       wqueue.ControllerInterface
}

func NewDeleteHandler(reg *job.Registry, qc wqueue.ControllerInterface) *DeleteHandler {
	return &DeleteHandler{
		registry: reg,
		qc:       qc,
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

	id, err := job.IDFromBytes(cmd.Args[deleteArgID])
	if err != nil {
		return nil, prot.NewClientErr(err.Error())
	}

	rec, ok := h.registry.Record(id)
	if !ok {
		// Job expired or never existed
		// NOOP
		return nil, prot.ErrNotFound
	}

	h.registry.Delete(id)
	h.qc.Delete(rec.Job)

	// TODO: Command Log
	return prot.OkResp(), nil
}
