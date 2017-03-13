package handlers

import (
	"github.com/iamduo/workq/int/job"
	"github.com/iamduo/workq/int/prot"
)

const (
	runArgID          = 0
	runArgName        = 1
	runArgTTR         = 2
	runArgWaitTimeout = 3
	runArgPSize       = 4
)

type RunHandler struct {
	jc job.Runner
}

func NewRunHandler(jc job.Runner) *RunHandler {
	return &RunHandler{
		jc: jc,
	}
}

// run <id> <name> <ttr> <wait-timeout> <payload-size> [-priority=<value>]
// <payload-bytes>
//
// Run a job, blocking until wait-timeout if no workers are available, or until TTR
// if a worker is processing.
//
// Returns:
//
// CLIENT-ERROR on invalid input
// TIMEOUT if no workers were available within <wait-timeout>
// OK if successful with result response of executed job.
func (h *RunHandler) Exec(cmd *prot.Cmd) ([]byte, error) {
	j, err := buildJobFromRunCmd(cmd)
	if err != nil {
		return nil, err
	}

	timeout, err := parseTimeout(cmd.Args[runArgWaitTimeout])
	if err != nil {
		return nil, err
	}

	r, err := h.jc.Run(j, timeout)
	if err == job.ErrQueueOutOfSync {
		return nil, prot.NewServerErr(err.Error())
	}
	if err == job.ErrTimeout {
		return nil, prot.ErrTimeout
	}
	if err != nil {
		return nil, prot.NewClientErr(err.Error())
	}

	return prot.OkResultResp(j.ID, r.Success, r.Result), nil
}

func buildJobFromRunCmd(cmd *prot.Cmd) (*job.Job, error) {
	var err error
	if cmd.ArgC < 6 || cmd.FlagC > 1 {
		return nil, prot.ErrInvalidCmdArgs
	}

	j := job.NewEmptyJob()
	j.ID, err = parseID(cmd.Args[runArgID])
	if err != nil {
		return nil, prot.NewClientErr(err.Error())
	}

	j.Name, err = parseName(cmd.Args[runArgName])
	if err != nil {
		return nil, prot.NewClientErr(err.Error())
	}

	j.TTR, err = parseTTR(cmd.Args[runArgTTR])
	if err != nil {
		return nil, prot.NewClientErr(err.Error())
	}

	j.Payload, err = parsePayload(
		cmd.Args[runArgPSize],
		cmd.Args[cmd.ArgC-1],
	)
	if err != nil {
		return nil, prot.NewClientErr(err.Error())
	}

	priority, ok := cmd.Flags["priority"]
	if ok {
		j.Priority, err = parsePriority(priority)
		if err != nil {
			return nil, prot.NewClientErr(err.Error())
		}
	}

	return j, nil
}
