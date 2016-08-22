package handlers

import (
	"time"

	"github.com/iamduo/workq/int/job"
	"github.com/iamduo/workq/int/prot"
	"github.com/iamduo/workq/int/wqueue"
)

type LeaseHandler struct {
	registry *job.Registry
	qc       wqueue.ControllerInterface
}

func NewLeaseHandler(reg *job.Registry, qc wqueue.ControllerInterface) *LeaseHandler {
	return &LeaseHandler{
		registry: reg,
		qc:       qc,
	}
}

func (h *LeaseHandler) Exec(cmd *prot.Cmd) ([]byte, error) {
	var err error
	// No flags needed, don't allow garbage
	if cmd.Name != "lease" || cmd.ArgC < 2 || cmd.FlagC != 0 {
		return nil, prot.ErrInvalidCmdArgs
	}

	// Slice names from lease name1 name2 name3 <wait-timeout>
	// -1 for wait-timeout
	nameC := cmd.ArgC - 1
	names := make([]string, nameC)
	for i := 0; i < nameC; i++ {
		names[i], err = job.NameFromBytes(cmd.Args[i])
		if err != nil {
			return nil, prot.NewClientErr(err.Error())
		}
	}

	timeout, err := waitTimeoutFromBytes(cmd.Args[cmd.ArgC-1])
	if err != nil {
		return nil, err
	}

	cs := make([]<-chan wqueue.JobProxy, nameC)
	for i, name := range names {
		cs[i] = h.qc.Lease(name)
	}

	timer := time.NewTimer(time.Duration(timeout) * time.Millisecond)
	j, err := firstJob(cs, timer.C)
	if err != nil {
		return nil, err
	}

	// Track Job for Requeue
	err = h.startAttempt(j.ID)
	if err != nil {
		return nil, err
	}

	return prot.OkJobResp(j.ID, j.Name, j.Payload), nil
}

func (h *LeaseHandler) startAttempt(id job.ID) error {
	rec, ok := h.registry.Record(id)
	if !ok {
		// Job may have been forced deleted or TTL expired
		return prot.NewServerErr("Job record unavailable")
	}

	rec.Mu.Lock()
	if rec.Running() {
		rec.Mu.Unlock()
		// Job is within another attempt.
		return prot.NewClientErr("Job already leased")
	}

	if rec.Processed() {
		rec.Mu.Unlock()
		return prot.NewClientErr("Job already processed")
	}

	if (rec.Job.MaxAttempts != 0 &&
		rec.Attempts >= uint64(rec.Job.MaxAttempts) ||
		rec.Attempts >= job.MaxHardAttempts) ||
		rec.Attempts == job.MaxRunRecAttempts {

		rec.State = job.StateFailed
		rec.Mu.Unlock()
		return prot.NewClientErr("Attempts max reached")
	}

	rec.State = job.StateLeased
	rec.Attempts++

	ttr := time.Duration(rec.Job.TTR) * time.Millisecond
	timer := job.NewTimer(ttr)
	rec.Timers[job.RunRecTTRTimerIdx] = timer
	rec.Mu.Unlock()

	go func() {
		select {
		case <-timer.C:
			h.timeoutAttempt(id)
		case <-timer.Cancellation:
			return
		}
	}()

	return nil
}

func (h *LeaseHandler) timeoutAttempt(id job.ID) {
	rec, ok := h.registry.Record(id)
	if !ok {
		return
	}

	rec.Mu.Lock()
	if rec.Processed() {
		rec.Mu.Unlock()
		// NOOP
		return
	}

	if rec.Timers[job.RunRecTTRTimerIdx] != nil {
		rec.Timers[job.RunRecTTRTimerIdx].Cancel()
	}

	if rec.Job.MaxAttempts > 0 && rec.Attempts >= uint64(rec.Job.MaxAttempts) {
		rec.State = job.StateFailed
		rec.Mu.Unlock()
		return
	}

	rec.State = job.StatePending
	rec.Mu.Unlock()
	// NOOP if add returns false, already requeued.
	h.qc.Add(rec.Job)
}

func firstJob(cs []<-chan wqueue.JobProxy, timeoutCh <-chan time.Time) (*job.Job, error) {
	out := make(chan wqueue.JobProxy, 1)
	done := make(chan struct{})

	// Listen on a wqueue lease
	// Loop until done chan is closed from the first successful job lease
	receive := func(c <-chan wqueue.JobProxy) {
		for {
			select {
			case proxy := <-c:
				select {
				case out <- proxy:
				case <-done:
					return
				}
			case <-done:
				return
			}
		}
	}

	// Ensures there is a valid job from the proxy
	// Cleans up the other competing receive() goroutines via done chan
	jobFromProxy := func(proxy wqueue.JobProxy) (*job.Job, bool) {
		if j, ok := proxy(); ok {
			// Signal any outstanding receive() goroutines to stop
			close(done)
			return j, true
		}

		return nil, false
	}

	for _, ch := range cs {
		go receive(ch)
	}

	for {
		// It is possible for a job proxy to return nil if queues are empty at the
		// time a client receives a job proxy.
		// Case #1 - Leasing on a queue that is already empty
		// Case #2 - Another client has consumed the job
		// . Loop and try again.
		select {
		case <-timeoutCh:
			// Signal any outstanding receive() goroutines to stop
			close(done)
			return nil, prot.ErrTimedOut
		case proxy := <-out:
			j, ok := jobFromProxy(proxy)
			if ok {
				return j, nil
			}
		}
	}
}
