package handlers

import (
	"github.com/iamduo/workq/int/job"
	"github.com/iamduo/workq/int/prot"
	"github.com/iamduo/workq/int/wqueue"
)

const (
	// complete cmd args index
	completeArgID         = 0
	completeArgResultSize = 1
	completeArgResult     = 2
)

type CompleteHandler struct {
	registry *job.Registry
	qc       wqueue.ControllerInterface
}

func NewCompleteHandler(reg *job.Registry, qc wqueue.ControllerInterface) *CompleteHandler {
	return &CompleteHandler{
		registry: reg,
		qc:       qc,
	}
}

// complete <id> <result-size> <result>
//
// Sucessfully complete a job with an optional result.
// Stops any further TTR timer.
//
// Returns:
// CLIENT-ERROR * on invalid input
// NOT-FOUND if job does not exist
// OK on success
func (h *CompleteHandler) Exec(cmd *prot.Cmd) ([]byte, error) {
	if cmd.Name != "complete" || cmd.ArgC != 3 || cmd.FlagC != 0 {
		return nil, prot.ErrInvalidCmdArgs
	}

	id, err := job.IDFromBytes(cmd.Args[completeArgID])
	if err != nil {
		return nil, prot.NewClientErr(err.Error())
	}

	result, err := job.ResultFromBytes(cmd.Args[completeArgResultSize], cmd.Args[completeArgResult])
	if err != nil {
		return nil, prot.NewClientErr(err.Error())
	}

	rec, ok := h.registry.Record(id)
	if !ok {
		// Job expired or never existed
		// NOOP
		return nil, prot.ErrNotFound
	}

	rec.Mu.Lock()
	if rec.Processed() || rec.Result != nil {
		rec.Mu.Unlock()
		return nil, ErrDuplicateResult
	}

	rec.State = job.StateCompleted
	rec.WriteResult(result, true)
	if rec.Timers[job.RunRecTTRTimerIdx] != nil {
		rec.Timers[job.RunRecTTRTimerIdx].Cancel()
	}
	rec.Mu.Unlock()

	// TODO: sync job
	h.qc.Delete(rec.Job)

	// TODO: Command Log
	return prot.OkResp(), nil
}
