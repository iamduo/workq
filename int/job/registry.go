package job

import (
	"errors"
	"sync"
	"time"
)

const (
	StateNew       = 0
	StateCompleted = 1
	StateFailed    = 2
	StatePending   = 3
	StateLeased    = 4

	MaxRunRecAttempts = 1<<64 - 1

	RunRecTTRTimerIdx   = 0
	RunRecTTLTimerIdx   = 1
	RunRecSchedTimerIdx = 2
)

var (
	ErrDuplicate = errors.New("Duplicate")
)

// Registry holds all Job Run Records
type Registry struct {
	all map[ID]*RunRecord
	mu  sync.RWMutex
}

func NewRegistry() *Registry {
	return &Registry{
		all: make(map[ID]*RunRecord),
	}
}

// Add a RunRecord indexed by attached job id.
// Returns false on duplicate or invalid job id.
func (r *Registry) Add(rec *RunRecord) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	if rec.Job == nil || rec.Job.ID == (ID{}) {
		return false
	}

	if _, ok := r.all[rec.Job.ID]; ok {
		return false
	}

	r.all[rec.Job.ID] = rec
	return true
}

// Delete a run record by job ID.
// Cancels all attached timers.
// Return false if record does not exist.
func (r *Registry) Delete(id ID) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	rec, ok := r.all[id]
	if !ok {
		return false
	}

	rec.Mu.RLock()
	for _, timer := range rec.Timers {
		if timer != nil {
			timer.Cancel()
		}
	}
	rec.Mu.RUnlock()

	delete(r.all, id)
	return true
}

// Look up run record by ID
// Follows comma ok idiom
func (r *Registry) Record(jid ID) (*RunRecord, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	if rec, ok := r.all[jid]; ok {
		return rec, true
	}

	return nil, false
}

// Return length of registry
func (r *Registry) Len() int {
	r.mu.RLock()
	c := len(r.all)
	r.mu.RUnlock()
	return c
}

// Run Record encapsulates the meta for the execution of jobs.
// This detached run record allows Job objects to stay immutable after creation.
type RunRecord struct {
	Attempts uint64    // Number of attempts made
	State    uint8     // One of state constants
	Fails    uint8     // Number of explicit failures
	Job      *Job      // attached job object
	Result   Result    // Result of job
	Wait     Wait      // Wait channel for job result
	Timers   [3]*Timer // Timer container for scheduled time, TTR, TTL
	Mu       sync.RWMutex
}

// Returns an empty but intiialized run record
func NewRunRecord() *RunRecord {
	return &RunRecord{
		Wait: make(Wait, 1),
	}
}

// Returns if the job was successful
func (r *RunRecord) Success() bool {
	return r.State == StateCompleted
}

// Returns if the job is currently executing.
func (r *RunRecord) Running() bool {
	return r.State == StateLeased
}

// Returns if the job is fully processed.
func (r *RunRecord) Processed() bool {
	return r.State == StateCompleted || r.State == StateFailed
}

// Records a job result and sends result over the "wait" channel.
// Requires locking.
// Returns false if result has already been written.
func (r *RunRecord) WriteResult(result Result, success bool) bool {
	if r.Result != nil {
		return false
	}

	r.Result = result
	waitResult := &RunResult{Result: result, Success: success}
	r.Wait <- waitResult
	return true
}

type Result []byte

// RunResult is sent over the Wait channel.
// Allows readers to wait for a result and determine the success.
type RunResult struct {
	Success bool
	Result  Result
}

// Wait channel for active connected readers.
type Wait chan *RunResult

// Timer container encapsulates the original time.Timer with support for
// Cancellation.
type Timer struct {
	timer        *time.Timer
	C            <-chan time.Time
	Cancellation chan struct{}
	// Read only timer deadline, useful for approximations.
	// Slightly behind actual timer as the calculation is:
	// NOW + Duration.
	Deadline time.Time
}

func NewTimer(d time.Duration) *Timer {
	t := time.NewTimer(d)
	return &Timer{
		timer:        t,
		C:            t.C,
		Cancellation: make(chan struct{}, 1),
		Deadline:     time.Now().UTC().Add(d),
	}
}

// Cancel a timer and signal readers on cancellation channel.
func (t *Timer) Cancel() {
	t.timer.Stop()
	select {
	case t.Cancellation <- struct{}{}:
	default:
	}
}
