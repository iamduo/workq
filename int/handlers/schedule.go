package handlers

import (
	"github.com/iamduo/workq/int/job"
	"github.com/iamduo/workq/int/prot"
)

const (
	schedArgID    = 0
	schedArgName  = 1
	schedArgTTR   = 2
	schedArgTTL   = 3
	schedArgTime  = 4
	schedArgPSize = 5
)

type ScheduleHandler struct {
	jc job.Scheduler
}

func NewScheduleHandler(jc job.Scheduler) *ScheduleHandler {
	return &ScheduleHandler{
		jc: jc,
	}
}

// schedule <id> <name> <ttr> <ttl> <time> <payload-size> [max-attempts=<value>]
// [max-fails=<value>] [-priority=<value>] <payload>
//
// Schedules a job to run at a UTC time with respect for <priority>
// TTL timer starts when scheduled time is met.
//
// Returns:
// CLIENT-ERROR on invalid input
// OK on success enqueue
func (h *ScheduleHandler) Exec(cmd *prot.Cmd) ([]byte, error) {
	j, err := buildJobFromSchedCmd(cmd)
	if err != nil {
		return nil, err
	}

	err = h.jc.Schedule(j)
	if err == job.ErrQueueOutOfSync {
		return nil, prot.NewServerErr(err.Error())
	}
	if err != nil {
		return nil, prot.NewClientErr(err.Error())
	}

	return prot.OkResp(), nil
}

func buildJobFromSchedCmd(cmd *prot.Cmd) (*job.Job, error) {
	var err error
	if cmd.Name != "schedule" || cmd.ArgC < 7 || cmd.FlagC > 3 {
		return nil, prot.ErrInvalidCmdArgs
	}

	j := job.NewEmptyJob()

	j.ID, err = parseID(cmd.Args[schedArgID])
	if err != nil {
		return nil, prot.NewClientErr(err.Error())
	}

	j.Name, err = parseName(cmd.Args[schedArgName])
	if err != nil {
		return nil, prot.NewClientErr(err.Error())
	}

	j.TTR, err = parseTTR(cmd.Args[schedArgTTR])
	if err != nil {
		return nil, prot.NewClientErr(err.Error())
	}

	j.TTL, err = parseTTL(cmd.Args[schedArgTTL])
	if err != nil {
		return nil, prot.NewClientErr(err.Error())
	}

	j.Time, err = parseTime(cmd.Args[schedArgTime])
	if err != nil {
		return nil, prot.NewClientErr(err.Error())
	}

	j.Payload, err = parsePayload(
		cmd.Args[schedArgPSize],
		cmd.Args[cmd.ArgC-1],
	)
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
