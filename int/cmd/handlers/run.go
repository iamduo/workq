package handlers

import (
	"time"

	"github.com/iamduo/workq/int/job"
	"github.com/iamduo/workq/int/prot"
	"github.com/iamduo/workq/int/wqueue"
)

const (
	runArgID          = 0
	runArgName        = 1
	runArgTTR         = 2
	runArgWaitTimeout = 3
	runArgPSize       = 4
)

type RunHandler struct {
	registry *job.Registry
	qc       wqueue.ControllerInterface
	usage    *Usage
}

func NewRunHandler(reg *job.Registry, qc wqueue.ControllerInterface, usage *Usage) *RunHandler {
	return &RunHandler{
		registry: reg,
		qc:       qc,
		usage:    usage,
	}
}

func (h *RunHandler) Exec(cmd *prot.Cmd) ([]byte, error) {
	j, err := buildJobFromRunCmd(cmd)
	if err != nil {
		return nil, err
	}

	timeout, err := waitTimeoutFromBytes(cmd.Args[runArgWaitTimeout])
	if err != nil {
		return nil, err
	}

	rec := job.NewRunRecord()
	wait := rec.Wait
	rec.Job = j
	if !h.registry.Add(rec) {
		return nil, ErrDuplicateJob
	}

	// Severe, registry and queue out of sync
	if !h.qc.Run(j) {
		return nil, prot.NewServerErr("Unable to run")
	}

	timer := time.NewTimer(time.Duration(timeout) * time.Millisecond)
	timeoutRun := func() {
		h.registry.Delete(j.ID)
		h.qc.Delete(j)
		expireJob(h.registry, h.qc, h.usage, j.ID)
	}

	for {
		// Pass 1
		// Look for either a result or <wait-timeout>
		select {
		case <-timer.C:
			rec, ok := h.registry.Record(j.ID)
			rec.Mu.RLock()
			// Job is not running during <wait-timeout>
			// AOK to fully clean up
			if !ok || !rec.Running() {
				rec.Mu.RUnlock()
				timeoutRun()
				return nil, prot.ErrTimedOut
			}

			// Pass 2
			// Job is running after <wait-timeout>
			// Wait at maximum + <ttr>
			// Grab result or timeout after <ttr>
			if rec.Timers[job.RunRecTTRTimerIdx] == nil {
				rec.Mu.RUnlock()
				return nil, prot.ErrTimedOut
			}

			deadline := rec.Timers[job.RunRecTTRTimerIdx].Deadline
			rec.Mu.RUnlock()

			ttrTimer := time.NewTimer(deadline.Sub(time.Now().UTC()))
			select {
			case <-ttrTimer.C:
				timeoutRun()
				return nil, prot.ErrTimedOut
			case result := <-wait:
				return prot.OkResultResp(j.ID, result.Success, result.Result), nil
			}
		case result := <-wait:
			return prot.OkResultResp(j.ID, result.Success, result.Result), nil
		}
	}
}

func buildJobFromRunCmd(cmd *prot.Cmd) (*job.Job, error) {
	var err error
	if cmd.ArgC < 6 || cmd.FlagC > 1 {
		return nil, prot.ErrInvalidCmdArgs
	}

	j := job.NewEmptyJob()
	j.ID, err = job.IDFromBytes(cmd.Args[runArgID])
	if err != nil {
		return nil, prot.NewClientErr(err.Error())
	}

	j.Name, err = job.NameFromBytes(cmd.Args[runArgName])
	if err != nil {
		return nil, prot.NewClientErr(err.Error())
	}

	j.TTR, err = job.TTRFromBytes(cmd.Args[runArgTTR])
	if err != nil {
		return nil, prot.NewClientErr(err.Error())
	}

	j.Payload, err = job.PayloadFromBytes(
		cmd.Args[runArgPSize],
		cmd.Args[cmd.ArgC-1],
	)
	if err != nil {
		return nil, prot.NewClientErr(err.Error())
	}

	priority, ok := cmd.Flags["priority"]
	if ok {
		j.Priority, err = job.PriorityFromBytes(priority)
		if err != nil {
			return nil, prot.NewClientErr(err.Error())
		}
	}

	return j, nil
}
