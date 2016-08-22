package handlers

import (
	"time"

	"github.com/iamduo/workq/int/job"
	"github.com/iamduo/workq/int/prot"
	"github.com/iamduo/workq/int/wqueue"
)

const (
	// Result cmd args index
	resultArgID          = 0
	resultArgWaitTimeout = 1
)

type ResultHandler struct {
	registry *job.Registry
	qc       wqueue.ControllerInterface
}

func NewResultHandler(reg *job.Registry, qc wqueue.ControllerInterface) *ResultHandler {
	return &ResultHandler{
		registry: reg,
		qc:       qc,
	}
}

// `result` <id>...
//
// Job result by id, blocking until wait-timeout.
//
// Returns:
// CLIENT-ERROR on invalid input
// TIMED-OUT if no job results are available
// OK if successful with result response
func (h *ResultHandler) Exec(cmd *prot.Cmd) ([]byte, error) {
	if cmd.Name != "result" || cmd.ArgC != 2 || cmd.FlagC > 0 {
		return nil, prot.ErrInvalidCmdArgs
	}

	id, err := job.IDFromBytes(cmd.Args[resultArgID])
	if err != nil {
		return nil, prot.NewClientErr(err.Error())
	}

	timeout, err := waitTimeoutFromBytes(cmd.Args[resultArgWaitTimeout])
	if err != nil {
		return nil, err
	}

	rec, ok := h.registry.Record(id)
	if !ok {
		// Job expired or never existed
		// NOOP
		return nil, prot.ErrNotFound
	}

	timer := time.NewTimer(time.Duration(timeout) * time.Millisecond)
	rec.Mu.RLock()
	if !rec.Processed() {
		rec.Mu.RUnlock()
		select {
		case <-timer.C:
			return nil, prot.ErrTimedOut
		case result := <-rec.Wait:
			return prot.OkResultResp(id, result.Success, result.Result), nil
		}
	}

	resp := prot.OkResultResp(id, rec.Success(), rec.Result)
	rec.Mu.RUnlock()

	return resp, nil
}
