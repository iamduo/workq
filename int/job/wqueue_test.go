package job

import (
	"testing"
	"time"

	"github.com/iamduo/workq/int/testutil"
)

func TestNewWorkQueue(t *testing.T) {
	q := NewWorkQueue()
	assertEmpty(t, q)
}

func TestQueueAddAndPop(t *testing.T) {
	q := NewWorkQueue()

	j := NewEmptyJob()
	if !q.Add(j) {
		t.Fatalf("Unable to add")
	}
	expected, ok := q.pop()
	if !ok || j != expected {
		t.Fatalf("Popped job=%v does not match addded job=%v", expected, j)
	}

	assertEmpty(t, q)
}

func TestQueueAddAndLease(t *testing.T) {
	q := NewWorkQueue()

	j := NewEmptyJob()
	if !q.Add(j) {
		t.Fatalf("Unable to add")
	}

	lease := q.Lease()
	select {
	case proxy := <-lease:
		expected, ok := proxy()
		if !ok || expected != j {
			t.Fatalf("Leased job=%v does not match added job=%v", expected, j)
		}
	default:
		t.Fatalf("No job leased, expected job")
	}
}

func TestQueueAddAndPopReturnsPriorityOrder(t *testing.T) {
	q := NewWorkQueue()
	assertEmpty(t, q)

	tests := []struct {
		j1  *Job
		j2  *Job
		asc bool // Ascending order
	}{
		{
			&Job{Name: "j1", Priority: 0},
			&Job{Name: "j2", Priority: 1},
			true,
		},
		// Reversed, higher priority item first
		{
			&Job{Name: "j1", Priority: 1},
			&Job{Name: "j2", Priority: 0},
			false,
		},
		// Same priority, but older by created time
		{
			&Job{Name: "j1", Priority: 0, Created: time.Now().UTC()},
			&Job{Name: "j2", Priority: 0, Created: time.Now().UTC().Add(-1 * time.Second)},
			true,
		},
		// Reversed, same priority, but older by created time first
		{
			&Job{Name: "j1", Priority: 0, Created: time.Now().UTC().Add(-1 * time.Second)},
			&Job{Name: "j2", Priority: 0, Created: time.Now().UTC()},
			false,
		},
	}

	for _, in := range tests {
		if !q.Add(in.j1) {
			t.Fatalf("Unable to add")
		}
		if !q.Add(in.j2) {
			t.Fatalf("Unable to add")
		}

		e1, ok := q.pop()
		if !ok {
			t.Fatalf("Unable to pop")
		}
		e2, ok := q.pop()
		if !ok {
			t.Fatalf("Unable to pop")
		}

		if in.asc {
			if e1 != in.j2 || e2 != in.j1 {
				t.Fatalf("Job returned in incorrect order e1=%v, e2=%v", e1, e2)
			}
		} else {
			if e1 != in.j1 || e2 != in.j2 {
				t.Fatalf("Job returned in incorrect order e1=%v, e2=%v", e1, e2)
			}
		}
	}
}

func TestQueueAddDuplicate(t *testing.T) {
	q := NewWorkQueue()
	assertEmpty(t, q)

	j := NewEmptyJob()
	if !q.Add(j) {
		t.Fatalf("Unable to add")
	}
	if q.Add(j) {
		t.Fatalf("Should not be able to add duplicate")
	}
}

func TestQueueSchedule(t *testing.T) {
	q := NewWorkQueue()

	j := NewEmptyJob()
	j.Time = time.Now().UTC()
	if !q.Schedule(j) {
		t.Fatalf("Unable to schedule")
	}

	_, ok := q.pop()
	if ok {
		t.Fatalf("Scheduled job can't be popped")
	}

	if !q.Awake(j) {
		t.Fatalf("Unable to awake job")
	}

	awakeJ, ok := q.pop()
	if awakeJ != j || !ok {
		t.Fatalf("Expected to pop awoken job")
	}

	if q.Awake(j) {
		t.Fatalf("Should not be able to wake more than once")
	}
}

func TestQueueScheduleDuplicate(t *testing.T) {
	q := NewWorkQueue()
	assertEmpty(t, q)

	j := NewEmptyJob()
	j.Time = time.Now().UTC()
	if !q.Schedule(j) {
		t.Fatalf("Unable to schedule")
	}
	if q.Schedule(j) {
		t.Fatalf("Should not be able to schedule duplicate")
	}
}

func TestQueueScheduleAndPop(t *testing.T) {
	q := NewWorkQueue()
	assertEmpty(t, q)

	timeFormat := "2006-01-02T15:04:05Z"
	parseTime := func(value string) time.Time {
		t, _ := time.Parse(timeFormat, value)
		return t
	}

	tests := []struct {
		j1  *Job
		j2  *Job
		asc bool // Ascending order
	}{
		{
			// j1=Earlier created time
			// j2=Earlier scheduled time
			&Job{Name: "j1", Time: time.Now().UTC(), Created: time.Now().UTC().Add(-1 * time.Second)},
			&Job{Name: "j2", Time: time.Now().UTC().Add(-100 * time.Second), Created: time.Now().UTC()},
			true,
		},
		// j1=Earlier scheduled time
		// j2=Earlier created time
		{
			&Job{Name: "j1", Time: time.Now().UTC().Add(-100 * time.Second), Created: time.Now().UTC()},
			&Job{Name: "j2", Time: time.Now().UTC(), Created: time.Now().UTC().Add(-1 * time.Second)},
			false,
		},
		// Same scheduled time
		// j1=Earlier created time
		// j2=Higher priority
		{
			&Job{Name: "j1", Time: parseTime("2016-01-01T00:00:00Z"), Created: time.Now().UTC().Add(-1 * time.Second)},
			&Job{Name: "j2", Time: parseTime("2016-01-01T00:00:00Z"), Priority: 1, Created: time.Now().UTC()},
			true,
		},
		// Same scheduled time
		// j1=Higher priority
		// j2=Earlier created time
		{
			&Job{Name: "j1", Time: parseTime("2016-01-01T00:00:00Z"), Priority: 1, Created: time.Now().UTC()},
			&Job{Name: "j2", Time: parseTime("2016-01-01T00:00:00Z"), Created: time.Now().UTC().Add(-1 * time.Second)},
			false,
		},
	}

	for _, in := range tests {
		in.j1.ID = ID(testutil.GenID())
		in.j2.ID = ID(testutil.GenID())

		if !q.Schedule(in.j1) {
			t.Fatalf("Unable to schedule")
		}
		if !q.Schedule(in.j2) {
			t.Fatalf("Unable to schedule")
		}

		if !q.Awake(in.j1) {
			t.Fatalf("Unable to schedule")
		}

		if !q.Awake(in.j2) {
			t.Fatalf("Unable to schedule")
		}

		e1, ok := q.pop()
		if !ok {
			t.Fatalf("Unable to pop")
		}

		e2, ok := q.pop()
		if !ok {
			t.Fatalf("Unable to pop")
		}

		if in.asc {
			if e1 != in.j2 || e2 != in.j1 {
				t.Fatalf("Job returned in incorrect order e1=%v, e2=%v", e1, e2)
			}
		} else {
			if e1 != in.j1 || e2 != in.j2 {
				t.Fatalf("Job returned in incorrect order e1=%v, e2=%v", e1, e2)
			}
		}
	}
}

func TestQueueLeaseAndWaitForJobs(t *testing.T) {
	q := NewWorkQueue()
	done := make(chan struct{}, 2)

	expectedJ := NewEmptyJob()
	expectedJ.ID = ID(testutil.GenID())

	go func(expectedJ *Job) {
		skippedOnce := false
		for {
			select {
			case proxy := <-q.Lease():
				j, ok := proxy()
				if !skippedOnce && !ok {
					skippedOnce = true
					continue
				}
				if ok && j != expectedJ {
					t.Fatalf("Lease fail, expected=%+v, job=%+v,", expectedJ, j)
				}

				func() { done <- struct{}{} }()
			}
		}
	}(expectedJ)

	if !q.Add(expectedJ) {
		t.Fatalf("Unable to add")
	}
	<-done

	expectedJ = NewEmptyJob()
	expectedJ.ID = ID(testutil.GenID())
	go func(expectedJ *Job) {
		skippedOnce := false
		for {
			select {
			case proxy := <-q.Lease():
				j, ok := proxy()
				if !skippedOnce && !ok {
					skippedOnce = true
					continue
				}
				if ok && j != expectedJ {
					t.Fatalf("Lease fail")
				}

				func() { done <- struct{}{} }()
			}
		}
	}(expectedJ)

	if !q.Schedule(expectedJ) {
		t.Fatalf("Unable to schedule")
	}
	<-done
}

func TestQueueDelete(t *testing.T) {
	q := NewWorkQueue()

	jAdd := NewEmptyJob()
	jAdd.ID = ID(testutil.GenID())
	if !q.Add(jAdd) {
		t.Fatalf("Unable to add")
	}
	assertLen(t, q, 1)

	jSched := NewEmptyJob()
	jSched.ID = ID(testutil.GenID())
	if !q.Schedule(jSched) {
		t.Fatalf("Unable to schedule")
	}
	assertLen(t, q, 2)

	if !q.Delete(jAdd) {
		t.Fatalf("Unable to delete")
	}
	assertLen(t, q, 1)

	if q.Delete(jAdd) {
		t.Fatalf("Expected nothing to delete")
	}

	if !q.Delete(jSched) {
		t.Fatalf("Unable to delete")
	}
	assertLen(t, q, 0)

	if q.Delete(jSched) {
		t.Fatalf("Expected nothing to delete")
	}
}

func TestQueueExists(t *testing.T) {
	q := NewWorkQueue()

	jAdd := NewEmptyJob()
	jAdd.ID = ID(testutil.GenID())
	if q.Exists(jAdd) {
		t.Fatalf("Unexpected item exists")
	}
	if !q.Add(jAdd) {
		t.Fatalf("Unable to add")
	}
	if !q.Exists(jAdd) {
		t.Fatalf("Expected item to exist")
	}

	jSched := NewEmptyJob()
	jSched.ID = ID(testutil.GenID())
	if q.Exists(jSched) {
		t.Fatalf("Unexpected item exists")
	}
	if !q.Schedule(jSched) {
		t.Fatalf("Unable to schedule")
	}
	if !q.Exists(jSched) {
		t.Fatalf("Expected item to exist")
	}
}

func TestQueueInspectorLens(t *testing.T) {
	q := NewWorkQueue()
	insp := NewInspector(q)
	readyLen, scheduledLen := insp.Lens()
	if readyLen != 0 || scheduledLen != 0 {
		t.Fatalf("Length mismatch")
	}

	jAdd := NewEmptyJob()
	jAdd.ID = ID(testutil.GenID())
	if !q.Add(jAdd) {
		t.Fatalf("Unable to add")
	}

	readyLen, scheduledLen = insp.Lens()
	if readyLen != 1 || scheduledLen != 0 {
		t.Fatalf("Length mismatch")
	}

	jSched := NewEmptyJob()
	jSched.ID = ID(testutil.GenID())
	if !q.Schedule(jSched) {
		t.Fatalf("Unable to schedule")
	}

	readyLen, scheduledLen = insp.Lens()
	if readyLen != 1 || scheduledLen != 1 {
		t.Fatalf("Length mismatch")
	}
}

func TestQueueInspectIterators(t *testing.T) {
	q := NewWorkQueue()
	insp := NewInspector(q)
	readyIt, scheduledIt := insp.Iterators()

	if readyIt.Current() != nil || scheduledIt.Current() != nil {
		t.Fatalf("Empty queue iterator mismatch")
	}

	jAdd := NewEmptyJob()
	jAdd.ID = ID(testutil.GenID())
	if !q.Add(jAdd) {
		t.Fatalf("Unable to add")
	}

	readyIt, _ = insp.Iterators()
	item := readyIt.Current()
	if item.(*Job) != jAdd {
		t.Fatalf("Iterator job mismatch")
	}

	jSched := NewEmptyJob()
	jSched.ID = ID(testutil.GenID())
	if !q.Schedule(jSched) {
		t.Fatalf("Unable to schedule")
	}

	_, scheduledIt = insp.Iterators()
	item = scheduledIt.Current()
	if item.(*Job) != jSched {
		t.Fatalf("Iterator job mismatch")
	}
}

func assertLen(t *testing.T, q *WorkQueue, expLen int) {
	if q.Len() != expLen {
		t.Fatalf("Work Queue len mismatch, exp=%d, act=%d", expLen, q.Len())
	}
}

func assertEmpty(t *testing.T, q *WorkQueue) {
	if _, ok := q.pop(); ok {
		t.Fatalf("Work Queue should be empty")
	}
}

type testCb func(interface{})
type testWorkQueue struct {
	asserts map[string]testCb
	lease   chan JobProxy
}

func newTestWorkQueue(asserts map[string]testCb) *testWorkQueue {
	return &testWorkQueue{asserts: asserts, lease: make(chan JobProxy, 1)}
}

func (q *testWorkQueue) Add(j *Job) bool {
	if q.asserts["Add"] != nil {
		q.asserts["Add"](j)
	}
	return true
}

func (q *testWorkQueue) Schedule(j *Job) bool {
	if q.asserts["Schedule"] != nil {
		q.asserts["Schedule"](j)
	}
	return true
}

func (q *testWorkQueue) Delete(j *Job) bool {
	if q.asserts["Delete"] != nil {
		q.asserts["Delete"](j)
	}
	return true
}

func (q *testWorkQueue) Awake(j *Job) bool {
	if q.asserts["Awake"] != nil {
		q.asserts["Awake"](j)
	}
	return true
}

func (q *testWorkQueue) Exists(j *Job) bool {
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

func TestQueueControllerAdd(t *testing.T) {
	called := false
	j := NewEmptyJob()
	j.ID = ID(testutil.GenID())
	asserts := make(map[string]testCb)
	asserts["Add"] = func(ji interface{}) {
		called = true
		jb := ji.(*Job)
		if jb.ID != j.ID {
			t.FailNow()
		}
	}
	makeQueueOrig := makeQueue
	defer func() { makeQueue = makeQueueOrig }()
	makeQueue = func() QueueInterface {
		return newTestWorkQueue(asserts)
	}
	ctrl := NewQueueController()
	ctrl.Add(j)

	if !called {
		t.FailNow()
	}
}

func TestQueueControllerRun(t *testing.T) {
	called := false
	j := NewEmptyJob()
	j.ID = ID(testutil.GenID())
	asserts := make(map[string]testCb)
	asserts["Add"] = func(ji interface{}) {
		called = true
		jb := ji.(*Job)
		if jb.ID != j.ID {
			t.Fatalf("ID mismatch")
		}
	}
	makeQueueOrig := makeQueue
	defer func() { makeQueue = makeQueueOrig }()
	makeQueue = func() QueueInterface {
		return newTestWorkQueue(asserts)
	}
	ctrl := NewQueueController()
	ctrl.Run(j)

	if !called {
		t.Fatalf("Run not called")
	}
}

func TestQueueControllerSchedule(t *testing.T) {
	called := false
	j := NewEmptyJob()
	j.ID = ID(testutil.GenID())
	asserts := make(map[string]testCb)
	asserts["Schedule"] = func(ji interface{}) {
		called = true
		jb := ji.(*Job)
		if jb.ID != j.ID {
			t.FailNow()
		}
	}
	makeQueueOrig := makeQueue
	defer func() { makeQueue = makeQueueOrig }()
	makeQueue = func() QueueInterface {
		return newTestWorkQueue(asserts)
	}
	ctrl := NewQueueController()
	ctrl.Schedule(j)

	if !called {
		t.FailNow()
	}
}

func TestQueueControllerAwake(t *testing.T) {
	called := false
	j := NewEmptyJob()
	j.ID = ID(testutil.GenID())
	asserts := make(map[string]testCb)
	asserts["Awake"] = func(ji interface{}) {
		called = true
		jb := ji.(*Job)
		if jb.ID != j.ID {
			t.FailNow()
		}
	}
	makeQueueOrig := makeQueue
	defer func() { makeQueue = makeQueueOrig }()
	makeQueue = func() QueueInterface {
		return newTestWorkQueue(asserts)
	}
	ctrl := NewQueueController()
	ctrl.Awake(j)

	if !called {
		t.FailNow()
	}
}

func TestQueueControllerDelete(t *testing.T) {
	called := false
	j := NewEmptyJob()
	j.ID = ID(testutil.GenID())
	asserts := make(map[string]testCb)
	asserts["Delete"] = func(ji interface{}) {
		called = true
		jb := ji.(*Job)
		if jb.ID != j.ID {
			t.FailNow()
		}
	}
	makeQueueOrig := makeQueue
	defer func() { makeQueue = makeQueueOrig }()
	makeQueue = func() QueueInterface {
		return newTestWorkQueue(asserts)
	}
	ctrl := NewQueueController()
	ctrl.Delete(j)

	if !called {
		t.FailNow()
	}
}

func TestQueueControllerExists(t *testing.T) {
	called := false
	j := NewEmptyJob()
	j.ID = ID(testutil.GenID())
	asserts := make(map[string]testCb)
	asserts["Exists"] = func(ji interface{}) {
		called = true
		jb := ji.(*Job)
		if jb.ID != j.ID {
			t.FailNow()
		}
	}
	makeQueueOrig := makeQueue
	defer func() { makeQueue = makeQueueOrig }()
	makeQueue = func() QueueInterface {
		return newTestWorkQueue(asserts)
	}
	ctrl := NewQueueController()
	ctrl.Exists(j)

	if !called {
		t.FailNow()
	}
}

func TestQueueControllerLease(t *testing.T) {
	name := "q1"
	expQ := newTestWorkQueue(make(map[string]testCb))

	makeQueueOrig := makeQueue
	defer func() { makeQueue = makeQueueOrig }()
	makeQueue = func() QueueInterface {
		return expQ
	}

	ctrl := NewQueueController()
	q := ctrl.Queue(name)
	if expQ != q {
		t.Fatalf("Queue mismatch")
	}

	ch := ctrl.Lease(name)
	if expQ.Lease() != ch {
		t.Fatalf("Lease mismatch")
	}
}

func TestQueueControllerQueue(t *testing.T) {
	ctrl := NewQueueController()
	a := ctrl.Queue("a")
	if a != ctrl.Queue("a") {
		t.Fatalf("Queue x2 creates duplicate")
	}
}

func TestQueueControllerQueues(t *testing.T) {
	ctrl := NewQueueController()
	queues, mu := ctrl.Queues()

	a := ctrl.Queue("a")
	mu.RLock()
	q, ok := queues["a"]
	mu.RUnlock()
	if !ok || q != a {
		t.Fatalf("Queue mismatch")
	}
}
