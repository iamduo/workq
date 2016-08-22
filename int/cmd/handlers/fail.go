package handlers

import (
	"github.com/iamduo/workq/int/job"
	"github.com/iamduo/workq/int/prot"
	"github.com/iamduo/workq/int/wqueue"
)

const (
	// fail cmd arg indexes
	failArgID         = 0
	failArgResultSize = 1
	failArgResult     = 2
)

type FailHandler struct {
	registry *job.Registry
	qc       wqueue.ControllerInterface
}

func NewFailHandler(reg *job.Registry, qc wqueue.ControllerInterface) *FailHandler {
	return &FailHandler{
		registry: reg,
		qc:       qc,
	}
}

// fail <id> <result-size> <result>
//
// Fails a job with an optional result.
// Stops any further TTR timer.
//
// Returns:
// CLIENT-ERROR * on invalid input
// NOT-FOUND if job does not exist
// OK on success
func (h *FailHandler) Exec(cmd *prot.Cmd) ([]byte, error) {
	if cmd.Name != "fail" || cmd.ArgC != 3 || cmd.FlagC != 0 {
		return nil, prot.ErrInvalidCmdArgs
	}

	id, err := job.IDFromBytes(cmd.Args[failArgID])
	if err != nil {
		return nil, prot.NewClientErr(err.Error())
	}

	result, err := job.ResultFromBytes(cmd.Args[failArgResultSize], cmd.Args[failArgResult])
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

	rec.State = job.StateFailed
	rec.WriteResult(result, false)
	if rec.Timers[job.RunRecTTRTimerIdx] != nil {
		rec.Timers[job.RunRecTTRTimerIdx].Cancel()
	}
	rec.Mu.Unlock()

	h.qc.Delete(rec.Job)

	// TODO: Command Log
	return prot.OkResp(), nil
}
