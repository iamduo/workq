package handlers

import (
	"time"

	"github.com/iamduo/workq/int/job"
	"github.com/iamduo/workq/int/prot"
	"github.com/iamduo/workq/int/wqueue"
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
	registry *job.Registry
	qc       wqueue.ControllerInterface
	usage    *Usage
}

func NewAddHandler(reg *job.Registry, qc wqueue.ControllerInterface, usage *Usage) *AddHandler {
	return &AddHandler{
		registry: reg,
		qc:       qc,
		usage:    usage,
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

	rec := job.NewRunRecord()
	rec.Job = j

	if !h.registry.Add(rec) {
		return nil, ErrDuplicateJob
	}

	// More severe, queue is always in sync with registry
	if !h.qc.Add(j) {
		h.registry.Delete(j.ID)
		return nil, prot.NewServerErr("Unable to add job")
	}

	timer := job.NewTimer(time.Duration(rec.Job.TTL) * time.Millisecond)
	rec.Mu.Lock()
	rec.Timers[job.RunRecTTLTimerIdx] = timer
	rec.Mu.Unlock()
	go func() {
		select {
		case <-timer.C:
			expireJob(h.registry, h.qc, h.usage, j.ID)
		case <-timer.Cancellation:
			return
		}
	}()

	return prot.OkResp(), nil
}

func buildJobFromAddCmd(cmd *prot.Cmd) (*job.Job, error) {
	var err error
	if cmd.Name != "add" || cmd.ArgC < 6 || cmd.FlagC > 3 {
		return nil, prot.ErrInvalidCmdArgs
	}

	j := &job.Job{Created: time.Now().UTC()}

	j.ID, err = job.IDFromBytes(cmd.Args[addArgID])
	if err != nil {
		return nil, prot.NewClientErr(err.Error())
	}

	j.Name, err = job.NameFromBytes(cmd.Args[addArgName])
	if err != nil {
		return nil, prot.NewClientErr(err.Error())
	}

	j.Payload, err = job.PayloadFromBytes(
		cmd.Args[addArgPSize],
		cmd.Args[cmd.ArgC-1],
	)
	if err != nil {
		return nil, prot.NewClientErr(err.Error())
	}

	j.TTR, err = job.TTRFromBytes(cmd.Args[addArgTTR])
	if err != nil {
		return nil, prot.NewClientErr(err.Error())
	}

	j.TTL, err = job.TTLFromBytes(cmd.Args[addArgTTL])
	if err != nil {
		return nil, prot.NewClientErr(err.Error())
	}

	maxAtt, ok := cmd.Flags["max-attempts"]
	if ok {
		j.MaxAttempts, err = job.MaxAttemptsFromBytes(maxAtt)
		if err != nil {
			return nil, prot.NewClientErr(err.Error())
		}
	}

	maxFails, ok := cmd.Flags["max-fails"]
	if ok {
		j.MaxFails, err = job.MaxFailsFromBytes(maxFails)
		if err != nil {
			return nil, prot.NewClientErr(err.Error())
		}
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
