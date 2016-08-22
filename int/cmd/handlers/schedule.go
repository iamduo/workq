package handlers

import (
	"time"

	"github.com/iamduo/workq/int/job"
	"github.com/iamduo/workq/int/prot"
	"github.com/iamduo/workq/int/wqueue"
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
	registry *job.Registry
	qc       wqueue.ControllerInterface
	usage    *Usage
}

func NewScheduleHandler(reg *job.Registry, qc wqueue.ControllerInterface, usage *Usage) *ScheduleHandler {
	return &ScheduleHandler{
		registry: reg,
		qc:       qc,
		usage:    usage,
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

	rec := job.NewRunRecord()
	rec.Job = j
	if !h.registry.Add(rec) {
		return nil, ErrDuplicateJob
	}

	// Severe, registry and queue out of sync
	if !h.qc.Schedule(j) {
		h.registry.Delete(j.ID)
		return nil, prot.NewServerErr("Unable to schedule job")
	}

	timer := job.NewTimer(j.Time.Sub(j.Created))
	rec.Mu.Lock()
	rec.Timers[job.RunRecSchedTimerIdx] = timer
	rec.Mu.Unlock()

	go func() {
		select {
		case <-timer.C:
			h.runScheduled(j.ID)
		case <-timer.Cancellation:
			return
		}
	}()

	return prot.OkResp(), nil
}

func buildJobFromSchedCmd(cmd *prot.Cmd) (*job.Job, error) {
	var err error
	if cmd.Name != "schedule" || cmd.ArgC < 7 || cmd.FlagC > 3 {
		return nil, prot.ErrInvalidCmdArgs
	}

	j := job.NewEmptyJob()

	j.ID, err = job.IDFromBytes(cmd.Args[schedArgID])
	if err != nil {
		return nil, prot.NewClientErr(err.Error())
	}

	j.Name, err = job.NameFromBytes(cmd.Args[schedArgName])
	if err != nil {
		return nil, prot.NewClientErr(err.Error())
	}

	j.TTR, err = job.TTRFromBytes(cmd.Args[schedArgTTR])
	if err != nil {
		return nil, prot.NewClientErr(err.Error())
	}

	j.TTL, err = job.TTLFromBytes(cmd.Args[schedArgTTL])
	if err != nil {
		return nil, prot.NewClientErr(err.Error())
	}

	j.Time, err = job.TimeFromBytes(cmd.Args[schedArgTime])
	if err != nil {
		return nil, prot.NewClientErr(err.Error())
	}

	if !job.IsTimeRelevant(j.Time) {
		return nil, prot.NewClientErr(job.ErrInvalidTime.Error())
	}

	j.Payload, err = job.PayloadFromBytes(
		cmd.Args[schedArgPSize],
		cmd.Args[cmd.ArgC-1],
	)
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

func (h *ScheduleHandler) runScheduled(id job.ID) {
	rec, ok := h.registry.Record(id)
	if !ok {
		return
	}

	timer := job.NewTimer(time.Duration(rec.Job.TTL) * time.Millisecond)
	rec.Mu.Lock()
	rec.Timers[job.RunRecTTLTimerIdx] = timer
	rec.Mu.Unlock()

	// NOOP if Awake returns false, job already gone.
	// expireJob will finalize clean if neccessary.
	h.qc.Awake(rec.Job)

	go func() {
		select {
		case <-timer.C:
			expireJob(h.registry, h.qc, h.usage, id)
		case <-timer.Cancellation:
			return
		}
	}()

}
