package wqueue

import (
	"bytes"
	"sync"

	"github.com/iamduo/workq/int/job"
	"github.com/iamduo/workq/int/skiplist"
)

type QueueInterface interface {
	Add(j *job.Job) bool
	Schedule(j *job.Job) bool
	Lease() <-chan JobProxy
	Delete(j *job.Job) bool
	Exists(j *job.Job) bool
	Awake(j *job.Job) bool
	Len() int
}

type Iterator struct {
	*skiplist.Iterator
}

// WorkQueue encapsulates 3 separate queues for a single type of job.
// Queues implement skiplists with different compare functions.
type WorkQueue struct {
	// Worked enqueued ready for processing.
	// Ordered by priority,created time
	ready *skiplist.List

	// Schedueled queue (sched cmd)
	// Ordered by priority,scheduled time
	// Only processed when current time >= scheduled time
	scheduled *skiplist.List

	// Lease channel allows for workers to block and wait for work.
	// A JobProxy is a signaling device sent on every WorkQueue change operation.
	// Instead of sending the actual Queue item, a Queue Item "job" proxy is sent
	// to notify only, allowing callers to decide to commit to popping the job.
	// This is useful if you are leasing on multiple jobs, but only require a single
	// job.
	lease chan JobProxy
	mu    sync.RWMutex
}

// NewWorkQueue returns an initialized WorkQueue
func NewWorkQueue() *WorkQueue {
	w := &WorkQueue{
		lease:     make(chan JobProxy, 1),
		ready:     skiplist.New(compare),
		scheduled: skiplist.New(compare),
	}

	return w
}

// Add a job
// Returns false on duplicate job
// Returns true on success and sends a job proxy to signal leasers.
func (w *WorkQueue) Add(j *job.Job) bool {
	if !w.ready.Insert(j) {
		return false
	}

	go w.sendJobProxy()
	return true
}

// Schedule a jobs on scheduled queue
// Returns false on duplicate job
// Returns true on success and sends a job proxy to signal leasers.
func (w *WorkQueue) Schedule(j *job.Job) bool {
	if !w.scheduled.Insert(j) {
		return false
	}

	return true
}

// Lease returns a channel to listen on with a JobProxy callback.
func (w *WorkQueue) Lease() <-chan JobProxy {
	w.sendJobProxy()
	return w.lease
}

// Delete a job by value
// Returns false if job was not found
func (w *WorkQueue) Delete(j *job.Job) bool {
	if w.ready.Delete(j) {
		return true
	}

	return w.scheduled.Delete(j)
}

// Awake a scheduled job by swapping a job from scheduled -> ready queue
func (w *WorkQueue) Awake(j *job.Job) bool {
	w.mu.Lock()
	defer w.mu.Unlock()

	if !w.scheduled.Delete(j) {
		return false
	}

	// NOOP, Insert can only return false on duplicate
	w.ready.Insert(j)
	w.sendJobProxy()
	return true
}

// Verify if the job already exists
func (w *WorkQueue) Exists(j *job.Job) bool {
	if w.ready.Exists(j) {
		return true
	}

	return w.scheduled.Exists(j)
}

// Get the length of the combined queues.
func (w *WorkQueue) Len() int {
	return w.ready.Len() + w.scheduled.Len()
}

// Pump a pop method which implements JobProxy to lease channel
func (w *WorkQueue) sendJobProxy() {
	select {
	case w.lease <- w.pop:
	default:
		// No need to block, only need to signal a job is ready
	}
}

// JobProxy implementation
// Follows comma ok idiom
func (w *WorkQueue) pop() (*job.Job, bool) {
	if j, ok := w.popReady(); ok {
		return j, true
	}

	return nil, false
}

func (w *WorkQueue) popReady() (*job.Job, bool) {
	if item, ok := w.ready.Pop(); ok {
		j := item.(*job.Job)
		return j, true
	}

	return nil, false
}

// JobProxy interface
// A function that returns a job when invoked following comma ok idiom
// Used in WorkQueue leases
type JobProxy func() (*job.Job, bool)

// Skiplist compare based on priority,scheduled time,created time
func compare(a interface{}, b interface{}) int {
	aj := a.(*job.Job)
	bj := b.(*job.Job)

	// Priority comparisons are reversed
	// Skiplist Pop should return highest priority first
	if aj.Priority < bj.Priority {
		return 1
	}

	if aj.Priority > bj.Priority {
		return -1
	}

	if aj.Time.UnixNano() < bj.Time.UnixNano() {
		return -1
	}

	// By earliest scheduled time
	if aj.Time.UnixNano() > bj.Time.UnixNano() {
		return 1
	}

	if aj.Created.UnixNano() < bj.Created.UnixNano() {
		return -1
	}

	// By earliest created time
	if aj.Created.UnixNano() > bj.Created.UnixNano() {
		return 1
	}

	// Compare ID
	return bytes.Compare(aj.ID[:], bj.ID[:])
}

// Inspector allows for deeper inspection of a WorkQueue.
type Inspector struct {
	wqueue *WorkQueue
}

func NewInspector(wqueue *WorkQueue) *Inspector {
	return &Inspector{wqueue: wqueue}
}

// Length of individual queues
func (i *Inspector) Lens() (int, int) {
	return i.wqueue.ready.Len(), i.wqueue.scheduled.Len()
}

// Iterators for individual queues
func (i *Inspector) Iterators() (*Iterator, *Iterator) {
	return &Iterator{i.wqueue.ready.Iterator()}, &Iterator{i.wqueue.scheduled.Iterator()}
}
