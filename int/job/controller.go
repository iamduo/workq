package job

import (
	"errors"
	"sync"
	"time"
)

var (
	ErrDuplicateJob       = errors.New("Duplicate job")
	ErrDuplicateResult    = errors.New("Duplicate result")
	ErrLeaseExpired       = errors.New("Lease expired")
	ErrMaxAttemptsReached = errors.New("Max attempts reached")
	ErrAlreadyLeased      = errors.New("Job already leased")
	ErrAlreadyProcessed   = errors.New("Job already processed")
	ErrEnqueue            = errors.New("Unable to enqueue")
	ErrNotFound           = errors.New("Not found")
	ErrTimeout            = errors.New("Timeout")
	ErrQueueOutOfSync     = errors.New("Queue out of sync")
)

type Controller struct {
	reg                *Registry
	qc                 QueueControllerInterface
	stats              Stats
	statlock           sync.Mutex
	expireFunc         func(id ID)
	timeoutAttemptFunc func(id ID)
}

type ControllerInterface interface {
	Adder
	Completer
	Deleter
	Failer
	Runner
	Scheduler
	Leaser
}

type Adder interface {
	Add(j *Job) error
	Expire(id ID)
	HandleExpire(func(ID))
	ExpireFunc() func(ID)
}

type Completer interface {
	Complete(id ID, result []byte) error
}

type Deleter interface {
	Delete(id ID) error
}

type Failer interface {
	Fail(id ID, result []byte) error
}

type Runner interface {
	Run(j *Job, timeout uint32) (*RunResult, error)
}

type Scheduler interface {
	Schedule(j *Job) error
}

type Leaser interface {
	Lease(names []string, timeout uint32) (*Job, error)
	StartAttempt(id ID) error
	TimeoutAttempt(id ID)
	HandleTimeoutAttempt(func(ID))
	TimeoutAttemptFunc() func(ID)
}

func ValidateAddJob(j *Job) error {
	var err error
	if err = ValidateID(j.ID); err != nil {
		return err
	}

	if err = ValidateName(j.Name); err != nil {
		return err
	}

	if err = ValidateTTR(j.TTR); err != nil {
		return err
	}

	if err = ValidateTTL(j.TTL); err != nil {
		return err
	}

	// Max Attempts + Max Fails is valid within uint8
	// Priority is valid within uint32

	if err = ValidateTime(j.Expiration()); err != nil {
		return err
	}

	if err = ValidatePayload(j.Payload); err != nil {
		return err
	}

	return nil
}

func ValidateScheduleJob(j *Job) error {
	return ValidateAddJob(j)
}

func ValidateRunJob(j *Job) error {
	var err error

	if err = ValidateID(j.ID); err != nil {
		return err
	}

	if err = ValidateName(j.Name); err != nil {
		return err
	}

	if err = ValidateTTR(j.TTR); err != nil {
		return err
	}

	if err = ValidatePayload(j.Payload); err != nil {
		return err
	}

	// Priority is always valid within uint32
	return nil
}

func NewController(reg *Registry, qc QueueControllerInterface) *Controller {
	c := &Controller{
		reg: reg,
		qc:  qc,
	}
	c.HandleExpire(c.expire)
	c.HandleTimeoutAttempt(c.timeoutAttempt)
	return c
}

// Adds a job to its named work queue with respect for <priority>.
// TTL timer starts immmediately.
func (c *Controller) Add(j *Job) error {
	if err := ValidateAddJob(j); err != nil {
		return err
	}

	rec := NewRunRecord()
	rec.Job = j
	if !c.reg.Add(rec) {
		return ErrDuplicateJob
	}

	// Severe, queue is always in sync with registry
	if !c.qc.Add(j) {
		return ErrQueueOutOfSync
	}

	timer := NewTimer(time.Duration(rec.Job.TTL) * time.Millisecond)

	rec.Mu.Lock()
	rec.Timers[RunRecTTLTimerIdx] = timer
	rec.Mu.Unlock()

	go func() {
		select {
		case <-timer.C:
			c.Expire(j.ID)
		case <-timer.Cancellation:
			return
		}
	}()

	return nil
}

// Sucessfully complete a job with an optional result.
// Stops TTR timer.
func (c *Controller) Complete(id ID, result []byte) error {
	var err error
	if err = ValidateID(id); err != nil {
		return err
	}
	if err = ValidateResult(result); err != nil {
		return err
	}

	rec, ok := c.reg.Record(id)
	if !ok {
		// Job expired or never existed
		// NO-OP
		return ErrNotFound
	}

	rec.Mu.Lock()
	if rec.Processed() || rec.Result != nil {
		rec.Mu.Unlock()
		return ErrDuplicateResult
	}

	rec.State = StateCompleted
	rec.WriteResult(result, true)
	if rec.Timers[RunRecTTRTimerIdx] != nil {
		rec.Timers[RunRecTTRTimerIdx].Cancel()
	}
	rec.Mu.Unlock()

	c.qc.Delete(rec.Job)

	return nil
}

// Delete a job by ID regardless of existing state.
func (c *Controller) Delete(id ID) error {
	var err error
	if err = ValidateID(id); err != nil {
		return err
	}

	rec, ok := c.reg.Record(id)
	if !ok {
		// Job expired or never existed
		// NO-OP
		return ErrNotFound
	}

	c.reg.Delete(id)

	rec.Mu.RLock()
	c.qc.Delete(rec.Job)
	rec.Mu.RUnlock()
	return nil
}

// Fail a job with a result.
// Stops TTR timer.
func (c *Controller) Fail(id ID, result []byte) error {
	var err error
	if err = ValidateID(id); err != nil {
		return err
	}
	if err = ValidateResult(result); err != nil {
		return err
	}

	rec, ok := c.reg.Record(id)
	if !ok {
		// Job expired or never existed
		// NO-OP
		return ErrNotFound
	}

	rec.Mu.Lock()
	defer rec.Mu.Unlock()
	if rec.Processed() || rec.Result != nil {
		return ErrDuplicateResult
	}

	rec.State = StateFailed
	rec.WriteResult(result, false)
	if rec.Timers[RunRecTTRTimerIdx] != nil {
		rec.Timers[RunRecTTRTimerIdx].Cancel()
	}

	c.qc.Delete(rec.Job)
	return nil
}

// Lease a job by name blocking until <wait-timeout>. Multiple job names can be
// specified and they will be processed uniformly by random selection.
//
// Returns a leased job on success within <wait-timeout> or a timeout error.
// TTR timer starts immediately on success.
func (c *Controller) Lease(names []string, timeout uint32) (*Job, error) {
	var err error
	if len(names) == 0 {
		return nil, ErrInvalidName
	}

	cs := make([]<-chan JobProxy, len(names))
	for i, name := range names {
		if err = ValidateName(name); err != nil {
			return nil, err
		}

		cs[i] = c.qc.Lease(name)
	}

	if err = ValidateTimeout(timeout); err != nil {
		return nil, err
	}

	timer := time.NewTimer(time.Duration(timeout) * time.Millisecond)
	var j *Job
	for {
		j, err = firstJob(cs, timer.C)
		if err != nil {
			return nil, err
		}

		// Track Job for Requeue
		err = c.StartAttempt(j.ID)
		if err != nil {
			// Skip job if job is no longer available for execution.
			// Try for another job.
			continue
		}

		return j, nil
	}
}

// Get the first job from a list of job lease channels.
func firstJob(cs []<-chan JobProxy, timeoutCh <-chan time.Time) (*Job, error) {
	out := make(chan JobProxy, 1)
	done := make(chan struct{})

	// Listen on a wqueue lease
	// Loop until done chan is closed from the first successful job lease
	receive := func(c <-chan JobProxy) {
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
	jobFromProxy := func(proxy JobProxy) (*Job, bool) {
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

JobLoop:
	for {
		// It is possible for a job proxy to return nil if queues are empty at the
		// time a client receives a job proxy.
		// Case #1 - Leasing on a queue that is already empty
		// Case #2 - Another client has consumed the job.
		// Loop and try again.
		select {
		case proxy := <-out:
			j, ok := jobFromProxy(proxy)
			if ok {
				return j, nil
			}

			continue JobLoop

		default:
			// Priority Nested Select
			// Timeout is given as a second priority to allow low timeouts to work
			// more naturaly (e.g., "10" ms timeout for an almost no-wait).
			// If there is a job, it takes priority.
			select {
			case <-timeoutCh:
				// Signal any outstanding receive() goroutines to stop
				close(done)
				return nil, ErrTimeout
			case proxy := <-out:
				j, ok := jobFromProxy(proxy)
				if ok {
					return j, nil
				}

				continue JobLoop
			}
		}
	}
}

// Run a job, blocking until wait-timeout if no workers are available, or until TTR
// if a worker is processing.
//
// Returns the job result on successful completion.
// TIMEOUT error is returned if no workers were available within <wait-timeout>
// or if the job failed to complete within TTR.
//
// This is the syncronous form of "add job".
// All job related data is deleted
func (c *Controller) Run(j *Job, timeout uint32) (*RunResult, error) {
	var err error

	if err = ValidateRunJob(j); err != nil {
		return nil, err
	}

	if err = ValidateTimeout(timeout); err != nil {
		return nil, err
	}

	rec := NewRunRecord()
	wait := rec.Wait
	rec.Job = j
	if !c.reg.Add(rec) {
		return nil, ErrDuplicateJob
	}

	// Severe, registry and queue out of sync
	if !c.qc.Run(j) {
		return nil, ErrQueueOutOfSync
	}

	timer := time.NewTimer(time.Duration(timeout) * time.Millisecond)
	timeoutRun := func() {
		c.reg.Delete(j.ID)
		c.qc.Delete(j)
		c.Expire(j.ID)
	}
	defer timeoutRun()

	for {
		// Pass 1
		// Look for either a result or <wait-timeout>
		select {
		case <-timer.C:
			rec, ok := c.reg.Record(j.ID)
			rec.Mu.RLock()
			// Job is not running during <wait-timeout>
			// AOK to fully clean up
			if !ok || (!rec.Running() && !rec.Processed()) {
				rec.Mu.RUnlock()
				return nil, ErrTimeout
			}

			// Pass 2
			// Job is running after <wait-timeout>
			// Wait at maximum + <ttr>
			// Grab result or timeout after <ttr>
			if rec.Timers[RunRecTTRTimerIdx] == nil {
				rec.Mu.RUnlock()
				return nil, ErrTimeout
			}

			deadline := rec.Timers[RunRecTTRTimerIdx].Deadline
			rec.Mu.RUnlock()

			ttrTimer := time.NewTimer(deadline.Sub(time.Now().UTC()))
			select {
			case <-ttrTimer.C:
				return nil, ErrTimeout
			case result := <-wait:
				return result, nil
			}
		case result := <-wait:
			return result, nil
		}
	}
}

// Schedules a job to run at a UTC time with respect for <priority>
// TTL timer starts when scheduled time is met.
func (c *Controller) Schedule(j *Job) error {
	var err error
	if err = ValidateScheduleJob(j); err != nil {
		return err
	}

	rec := NewRunRecord()
	rec.Job = j
	if !c.reg.Add(rec) {
		return ErrDuplicateJob
	}

	// Severe, registry and queue out of sync
	if !c.qc.Schedule(j) {
		return ErrQueueOutOfSync
	}

	timer := NewTimer(j.Time.Sub(time.Now()))

	rec.Mu.Lock()
	rec.Timers[RunRecSchedTimerIdx] = timer
	rec.Mu.Unlock()

	go func() {
		select {
		case <-timer.C:
			c.awake(j.ID)
		case <-timer.Cancellation:
			return
		}
	}()

	return nil
}

// Expire job by ID.
// Invoked by TTL timers and removes job regardless of state.
// See the expire() method for the implementation.
func (c *Controller) Expire(id ID) {
	c.expireFunc(id)
}

func (c *Controller) expire(id ID) {
	rec, ok := c.reg.Record(id)
	if !ok {
		return
	}

	rec.Mu.RLock()
	processed := rec.Processed()
	rec.Mu.RUnlock()
	if !processed {
		c.statlock.Lock()
		c.stats.EvictedJobs++
		c.statlock.Unlock()
	}

	c.reg.Delete(id)
	c.qc.Delete(rec.Job)
}

// HandleExpire sets an the func to be used for the Expire() method.
// This is primarily used to allow the proxying of a job controller in full as the
// expire method is not invoked directly, but rather as a secondary call from "add"
// or "schedule".
// Example can be found in the cmdlog package.
func (c *Controller) HandleExpire(f func(ID)) {
	c.expireFunc = f
}

// Return the current Expire callback.
func (c *Controller) ExpireFunc() func(ID) {
	return c.expireFunc
}

// Timeout job attempt by ID.
// See the "timeoutAttempt()" method for the implementation.
func (c *Controller) TimeoutAttempt(id ID) {
	c.timeoutAttemptFunc(id)
}

func (c *Controller) timeoutAttempt(id ID) {
	rec, ok := c.reg.Record(id)
	if !ok {
		return
	}

	rec.Mu.Lock()
	defer rec.Mu.Unlock()

	if rec.Timers[RunRecTTRTimerIdx] != nil {
		rec.Timers[RunRecTTRTimerIdx].Cancel()
	}

	if rec.Processed() {
		// NO-OP
		return
	}

	if rec.Job.MaxAttempts > 0 && rec.Attempts >= uint64(rec.Job.MaxAttempts) {
		rec.State = StateFailed
		rec.WriteResult([]byte{}, false)
		return
	}

	rec.State = StatePending
	// NO-OP if add returns false, already requeued.
	c.qc.Add(rec.Job)
}

// HandleTimeoutAttempt sets the func to be used for the TimeoutAttempt() method.
// This is primarily used to allow the proxying of a job controller in full as the
// TimeoutAttempt method is not invoked directly, but rather as a background call from "lease".
// Example can be found in the cmdlog package.
func (c *Controller) HandleTimeoutAttempt(f func(ID)) {
	c.timeoutAttemptFunc = f
}

// Return TimeoutAttempt callback.
func (c *Controller) TimeoutAttemptFunc() func(ID) {
	return c.timeoutAttemptFunc
}

// Start an attempt by ID.
// Ensures jobs respect TTR & max attempts policy.
func (c *Controller) StartAttempt(id ID) error {
	rec, ok := c.reg.Record(id)
	if !ok {
		// Job may have been forced deleted or TTL expired
		return ErrNotFound
	}

	rec.Mu.Lock()
	defer rec.Mu.Unlock()
	if rec.Running() {
		// Job is within another attempt.
		return ErrAlreadyLeased
	}

	if rec.Processed() {
		return ErrAlreadyProcessed
	}

	// Ensure the job is dequeued regardless of where StartAttempt is invoked from.
	// This allows StartAttempt to be idempotent.
	c.qc.Delete(rec.Job)

	if (rec.Job.MaxAttempts != 0 &&
		rec.Attempts >= uint64(rec.Job.MaxAttempts) ||
		rec.Attempts >= MaxHardAttempts) ||
		rec.Attempts == MaxRunRecAttempts {

		rec.State = StateFailed
		return ErrMaxAttemptsReached
	}

	rec.State = StateLeased
	rec.Attempts++

	ttr := time.Duration(rec.Job.TTR) * time.Millisecond
	timer := NewTimer(ttr)
	rec.Timers[RunRecTTRTimerIdx] = timer

	go func() {
		select {
		case <-timer.C:
			c.TimeoutAttempt(id)
		case <-timer.Cancellation:
			return
		}
	}()

	return nil
}

// Awake a job by ID.
// Used by scheduled job timers.
func (c *Controller) awake(id ID) {
	rec, ok := c.reg.Record(id)
	if !ok {
		return
	}

	rec.Mu.Lock()
	j := rec.Job
	timer := NewTimer(j.Expiration().Sub(time.Now()))
	rec.Timers[RunRecTTLTimerIdx] = timer
	rec.Mu.Unlock()

	// NO-OP if Awake returns false, job already gone.
	// Expire will finalize if neccessary.
	c.qc.Awake(j)

	go func() {
		select {
		case <-timer.C:
			c.Expire(id)
		case <-timer.Cancellation:
			return
		}
	}()
}

// Stats returns job controllers stats at the current time.
func (c *Controller) Stats() Stats {
	c.statlock.Lock()
	defer c.statlock.Unlock()
	return c.stats
}

// Stats represents job controller specific stats.
type Stats struct {
	// Number of incomplete jobs expired by TTL.
	EvictedJobs uint64
}
