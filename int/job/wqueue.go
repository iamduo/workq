package job

import (
	"bytes"
	"sync"

	"github.com/iamduo/workq/int/skiplist"
)

type QueueInterface interface {
	Add(j *Job) bool
	Schedule(j *Job) bool
	Lease() <-chan JobProxy
	Delete(j *Job) bool
	Exists(j *Job) bool
	Awake(j *Job) bool
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
func (w *WorkQueue) Add(j *Job) bool {
	if !w.ready.Insert(j) {
		return false
	}

	w.sendJobProxy()
	return true
}

// Schedule a jobs on scheduled queue
// Returns false on duplicate job
// Returns true on success and sends a job proxy to signal leasers.
func (w *WorkQueue) Schedule(j *Job) bool {
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
func (w *WorkQueue) Delete(j *Job) bool {
	if w.ready.Delete(j) {
		return true
	}

	return w.scheduled.Delete(j)
}

// Awake a scheduled job by swapping a job from scheduled -> ready queue
func (w *WorkQueue) Awake(j *Job) bool {
	w.mu.Lock()
	defer w.mu.Unlock()

	if !w.scheduled.Delete(j) {
		return false
	}

	// NO-OP, Insert can only return false on duplicate
	w.ready.Insert(j)
	w.sendJobProxy()
	return true
}

// Verify if the job already exists
func (w *WorkQueue) Exists(j *Job) bool {
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
func (w *WorkQueue) pop() (*Job, bool) {
	if j, ok := w.popReady(); ok {
		return j, true
	}

	return nil, false
}

func (w *WorkQueue) popReady() (*Job, bool) {
	if item, ok := w.ready.Pop(); ok {
		j := item.(*Job)
		return j, true
	}

	return nil, false
}

// JobProxy interface
// A function that returns a job when invoked following comma ok idiom
// Used in WorkQueue leases
type JobProxy func() (*Job, bool)

// Skiplist compare based on priority,scheduled time,created time
func compare(a interface{}, b interface{}) int {
	aj := a.(*Job)
	bj := b.(*Job)

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

type QueueControllerInterface interface {
	Queues() (map[string]QueueInterface, *sync.RWMutex)
	Queue(name string) QueueInterface
	Add(j *Job) bool
	Schedule(j *Job) bool
	Run(j *Job) bool
	Delete(j *Job) bool
	Awake(j *Job) bool
	Exists(j *Job) bool
	Lease(name string) <-chan JobProxy
}

// Queue controller owns/manages all queues.
// Almost all calls are proxied to the QueueInterface holder.
// Allows for management of any queue by name in a single location.
// Friendly for future queue call interception.
type QueueController struct {
	queues map[string]QueueInterface
	mu     sync.RWMutex
}

func NewQueueController() *QueueController {
	return &QueueController{queues: make(map[string]QueueInterface)}
}

// Internal factory method to allow for internals testing
var makeQueue = func() QueueInterface {
	return NewWorkQueue()
}

// Return all queues as a map with an explicit sync.RWMutex.
// All calls to the map require locking.
func (c *QueueController) Queues() (map[string]QueueInterface, *sync.RWMutex) {
	return c.queues, &c.mu
}

// Return a queue by name, creating it if it does not exist.
func (c *QueueController) Queue(name string) QueueInterface {
	c.mu.RLock()
	var q QueueInterface
	q, ok := c.queues[name]
	c.mu.RUnlock()

	if !ok {
		c.mu.Lock()
		// Create the queue
		q = makeQueue()
		c.queues[name] = q
		c.mu.Unlock()
	}
	return q
}

// Add a job
// Automatically creates the queue if it does not exist.
func (c *QueueController) Add(j *Job) bool {
	q := c.Queue(j.Name)
	return q.Add(j)
}

// Schedule a job
// Automatically creates the queue if it does not exist.
func (c *QueueController) Schedule(j *Job) bool {
	q := c.Queue(j.Name)
	return q.Schedule(j)
}

// Run a job
// @FYI When durability is added, run cmd's will not be durable.
// Automatically creates the queue if it does not exist.
func (c *QueueController) Run(j *Job) bool {
	q := c.Queue(j.Name)
	return q.Add(j)
}

// Delete a job
// Automatically creates the queue if it does not exist.
func (c *QueueController) Delete(j *Job) bool {
	q := c.Queue(j.Name)
	return q.Delete(j)
}

// Awake an existing scheduled job
func (c *QueueController) Awake(j *Job) bool {
	q := c.Queue(j.Name)
	return q.Awake(j)
}

// Verify if a job exists by object.
func (c *QueueController) Exists(j *Job) bool {
	q := c.Queue(j.Name)
	return q.Exists(j)
}

// Return a job lease by name
func (c *QueueController) Lease(name string) <-chan JobProxy {
	q := c.Queue(name)
	return q.Lease()
}
