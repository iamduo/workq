package wqueue

import (
	"sync"

	"github.com/iamduo/workq/int/job"
)

type ControllerInterface interface {
	Queues() (map[string]QueueInterface, *sync.RWMutex)
	Queue(name string) QueueInterface
	Add(j *job.Job) bool
	Schedule(j *job.Job) bool
	Run(j *job.Job) bool
	Delete(j *job.Job) bool
	Awake(j *job.Job) bool
	Exists(j *job.Job) bool
	Lease(name string) <-chan JobProxy
}

// Queue controller owns/manages all queues.
// Almost all calls are proxied to the QueueInterface holder.
// Allows for management of any queue by name in a single location.
// Friendly for future queue call interception.
type Controller struct {
	queues map[string]QueueInterface
	mu     sync.RWMutex
}

func NewController() ControllerInterface {
	return &Controller{queues: make(map[string]QueueInterface)}
}

// Internal factory method to allow for internals testing
var makeQueue = func() QueueInterface {
	return NewWorkQueue()
}

// Return all queues as a map with an explicit sync.RWMutex.
// All calls to the map require locking.
func (c *Controller) Queues() (map[string]QueueInterface, *sync.RWMutex) {
	return c.queues, &c.mu
}

// Return a queue by name, creating it if it does not exist.
func (c *Controller) Queue(name string) QueueInterface {
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
func (c *Controller) Add(j *job.Job) bool {
	q := c.Queue(j.Name)
	return q.Add(j)
}

// Schedule a job
// Automatically creates the queue if it does not exist.
func (c *Controller) Schedule(j *job.Job) bool {
	q := c.Queue(j.Name)
	return q.Schedule(j)
}

// Run a job
// @FYI When durability is added, run cmd's will not be durable.
// Automatically creates the queue if it does not exist.
func (c *Controller) Run(j *job.Job) bool {
	q := c.Queue(j.Name)
	return q.Add(j)
}

// Delete a job
// Automatically creates the queue if it does not exist.
func (c *Controller) Delete(j *job.Job) bool {
	q := c.Queue(j.Name)
	return q.Delete(j)
}

// Awake an existing scheduled job
func (c *Controller) Awake(j *job.Job) bool {
	q := c.Queue(j.Name)
	return q.Awake(j)
}

// Verify if a job exists by object.
func (c *Controller) Exists(j *job.Job) bool {
	q := c.Queue(j.Name)
	return q.Exists(j)
}

// Return a job lease by name
func (c *Controller) Lease(name string) <-chan JobProxy {
	q := c.Queue(name)
	return q.Lease()
}
