package wqueue

import (
	"testing"

	"github.com/iamduo/workq/int/job"
	"github.com/iamduo/workq/int/testutil"
)

type testCb func(interface{})
type testWorkQueue struct {
	asserts map[string]testCb
	lease   chan JobProxy
}

func newTestWorkQueue(asserts map[string]testCb) *testWorkQueue {
	return &testWorkQueue{asserts: asserts, lease: make(chan JobProxy, 1)}
}

func (q *testWorkQueue) Add(j *job.Job) bool {
	if q.asserts["Add"] != nil {
		q.asserts["Add"](j)
	}
	return true
}

func (q *testWorkQueue) Schedule(j *job.Job) bool {
	if q.asserts["Schedule"] != nil {
		q.asserts["Schedule"](j)
	}
	return true
}

func (q *testWorkQueue) Delete(j *job.Job) bool {
	if q.asserts["Delete"] != nil {
		q.asserts["Delete"](j)
	}
	return true
}

func (q *testWorkQueue) Awake(j *job.Job) bool {
	if q.asserts["Awake"] != nil {
		q.asserts["Awake"](j)
	}
	return true
}

func (q *testWorkQueue) Exists(j *job.Job) bool {
	if q.asserts["Exists"] != nil {
		q.asserts["Exists"](j)
	}
	return true
}

func (q *testWorkQueue) Lease() <-chan JobProxy {
	return q.lease
}

func (q *testWorkQueue) Len() int {
	return 0
}

func TestControllerAdd(t *testing.T) {
	called := false
	j := job.NewEmptyJob()
	j.ID, _ = job.IDFromBytes([]byte(testutil.GenId()))
	asserts := make(map[string]testCb)
	asserts["Add"] = func(ji interface{}) {
		called = true
		jb := ji.(*job.Job)
		if jb.ID != j.ID {
			t.FailNow()
		}
	}
	makeQueueOrig := makeQueue
	defer func() { makeQueue = makeQueueOrig }()
	makeQueue = func() QueueInterface {
		return newTestWorkQueue(asserts)
	}
	ctrl := NewController()
	ctrl.Add(j)

	if !called {
		t.FailNow()
	}
}

func TestControllerRun(t *testing.T) {
	called := false
	j := job.NewEmptyJob()
	j.ID, _ = job.IDFromBytes([]byte(testutil.GenId()))
	asserts := make(map[string]testCb)
	asserts["Add"] = func(ji interface{}) {
		called = true
		jb := ji.(*job.Job)
		if jb.ID != j.ID {
			t.Fatalf("ID mismatch")
		}
	}
	makeQueueOrig := makeQueue
	defer func() { makeQueue = makeQueueOrig }()
	makeQueue = func() QueueInterface {
		return newTestWorkQueue(asserts)
	}
	ctrl := NewController()
	ctrl.Run(j)

	if !called {
		t.Fatalf("Run not called")
	}
}

func TestControllerSchedule(t *testing.T) {
	called := false
	j := job.NewEmptyJob()
	j.ID, _ = job.IDFromBytes([]byte(testutil.GenId()))
	asserts := make(map[string]testCb)
	asserts["Schedule"] = func(ji interface{}) {
		called = true
		jb := ji.(*job.Job)
		if jb.ID != j.ID {
			t.FailNow()
		}
	}
	makeQueueOrig := makeQueue
	defer func() { makeQueue = makeQueueOrig }()
	makeQueue = func() QueueInterface {
		return newTestWorkQueue(asserts)
	}
	ctrl := NewController()
	ctrl.Schedule(j)

	if !called {
		t.FailNow()
	}
}

func TestControllerAwake(t *testing.T) {
	called := false
	j := job.NewEmptyJob()
	j.ID, _ = job.IDFromBytes([]byte(testutil.GenId()))
	asserts := make(map[string]testCb)
	asserts["Awake"] = func(ji interface{}) {
		called = true
		jb := ji.(*job.Job)
		if jb.ID != j.ID {
			t.FailNow()
		}
	}
	makeQueueOrig := makeQueue
	defer func() { makeQueue = makeQueueOrig }()
	makeQueue = func() QueueInterface {
		return newTestWorkQueue(asserts)
	}
	ctrl := NewController()
	ctrl.Awake(j)

	if !called {
		t.FailNow()
	}
}

func TestControllerDelete(t *testing.T) {
	called := false
	j := job.NewEmptyJob()
	j.ID, _ = job.IDFromBytes([]byte(testutil.GenId()))
	asserts := make(map[string]testCb)
	asserts["Delete"] = func(ji interface{}) {
		called = true
		jb := ji.(*job.Job)
		if jb.ID != j.ID {
			t.FailNow()
		}
	}
	makeQueueOrig := makeQueue
	defer func() { makeQueue = makeQueueOrig }()
	makeQueue = func() QueueInterface {
		return newTestWorkQueue(asserts)
	}
	ctrl := NewController()
	ctrl.Delete(j)

	if !called {
		t.FailNow()
	}
}

func TestControllerExists(t *testing.T) {
	called := false
	j := job.NewEmptyJob()
	j.ID, _ = job.IDFromBytes([]byte(testutil.GenId()))
	asserts := make(map[string]testCb)
	asserts["Exists"] = func(ji interface{}) {
		called = true
		jb := ji.(*job.Job)
		if jb.ID != j.ID {
			t.FailNow()
		}
	}
	makeQueueOrig := makeQueue
	defer func() { makeQueue = makeQueueOrig }()
	makeQueue = func() QueueInterface {
		return newTestWorkQueue(asserts)
	}
	ctrl := NewController()
	ctrl.Exists(j)

	if !called {
		t.FailNow()
	}
}

func TestControllerLease(t *testing.T) {
	name := "q1"
	expQ := newTestWorkQueue(make(map[string]testCb))

	makeQueueOrig := makeQueue
	defer func() { makeQueue = makeQueueOrig }()
	makeQueue = func() QueueInterface {
		return expQ
	}

	ctrl := NewController()
	q := ctrl.Queue(name)
	if expQ != q {
		t.Fatalf("Queue mismatch")
	}

	ch := ctrl.Lease(name)
	if expQ.Lease() != ch {
		t.Fatalf("Lease mismatch")
	}
}

func TestControllerQueue(t *testing.T) {
	ctrl := NewController()
	a := ctrl.Queue("a")
	if a != ctrl.Queue("a") {
		t.Fatalf("Queue x2 creates duplicate")
	}
}

func TestControllerQueues(t *testing.T) {
	ctrl := NewController()
	queues, mu := ctrl.Queues()

	a := ctrl.Queue("a")
	mu.RLock()
	q, ok := queues["a"]
	mu.RUnlock()
	if !ok || q != a {
		t.Fatalf("Queue mismatch")
	}
}
