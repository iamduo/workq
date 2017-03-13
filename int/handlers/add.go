package handlers

import (
	"time"

	"github.com/iamduo/workq/int/job"
	"github.com/iamduo/workq/int/prot"
)

const (
	// Index of add cmd args
	addArgID    = 0
	addArgName  = 1
	addArgTTR   = 2
	addArgTTL   = 3
	addArgPSize = 4
)

type AddHandler struct {
	jc job.Adder
}

func NewAddHandler(jc job.Adder) *AddHandler {
	return &AddHandler{
		jc: jc,
	}
}

// add <id> <name> <ttr> <ttl> <payload-size> [max-attempts=<value>]
// [max-fails=<value>] [-priority=<value>] <payload>
//
// Adds a job to its named work queue with respect for <priority>
// TTL timer starts immmediately
//
// Returns:
// CLIENT-ERROR on invalid input
// OK on success enqueue
func (h *AddHandler) Exec(cmd *prot.Cmd) ([]byte, error) {
	j, err := buildJobFromAddCmd(cmd)
	if err != nil {
		return nil, err
	}

	err = h.jc.Add(j)
	if err != nil {
		if err == job.ErrQueueOutOfSync {
			return nil, prot.NewServerErr(err.Error())
		}

		return nil, prot.NewClientErr(err.Error())
	}

	return prot.OkResp(), nil
}

func buildJobFromAddCmd(cmd *prot.Cmd) (*job.Job, error) {
	var err error
	if cmd.Name != "add" || cmd.ArgC < 6 || cmd.FlagC > 3 {
		return nil, prot.ErrInvalidCmdArgs
	}

	j := &job.Job{Created: time.Now().UTC()}

	j.ID, err = parseID(cmd.Args[addArgID])
	if err != nil {
		return nil, prot.NewClientErr(err.Error())
	}

	j.Name, err = parseName(cmd.Args[addArgName])
	if err != nil {
		return nil, prot.NewClientErr(err.Error())
	}

	j.Payload, err = parsePayload(
		cmd.Args[addArgPSize],
		cmd.Args[cmd.ArgC-1],
	)
	if err != nil {
		return nil, prot.NewClientErr(err.Error())
	}

	j.TTR, err = parseTTR(cmd.Args[addArgTTR])
	if err != nil {
		return nil, prot.NewClientErr(err.Error())
	}

	j.TTL, err = parseTTL(cmd.Args[addArgTTL])
	if err != nil {
		return nil, prot.NewClientErr(err.Error())
	}

	maxAtt, ok := cmd.Flags["max-attempts"]
	if ok {
		j.MaxAttempts, err = parseMaxAttempts(maxAtt)
		if err != nil {
			return nil, prot.NewClientErr(err.Error())
		}
	}

	maxFails, ok := cmd.Flags["max-fails"]
	if ok {
		j.MaxFails, err = parseMaxFails(maxFails)
		if err != nil {
			return nil, prot.NewClientErr(err.Error())
		}
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
