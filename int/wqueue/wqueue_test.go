package wqueue

import (
	"testing"
	"time"

	"github.com/iamduo/workq/int/job"
	"github.com/iamduo/workq/int/testutil"
)

func TestNewWorkQueue(t *testing.T) {
	q := NewWorkQueue()
	assertEmpty(t, q)
}

func TestAddAndPop(t *testing.T) {
	q := NewWorkQueue()

	j := job.NewEmptyJob()
	if !q.Add(j) {
		t.Fatalf("Unable to add")
	}
	expected, ok := q.pop()
	if !ok || j != expected {
		t.Fatalf("Popped job=%v does not match addded job=%v", expected, j)
	}

	assertEmpty(t, q)
}

func TestAddAndLease(t *testing.T) {
	q := NewWorkQueue()

	j := job.NewEmptyJob()
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

func TestAddAndPopReturnsPriorityOrder(t *testing.T) {
	q := NewWorkQueue()
	assertEmpty(t, q)

	tests := []struct {
		j1  *job.Job
		j2  *job.Job
		asc bool // Ascending order
	}{
		{
			&job.Job{Name: "j1", Priority: 0},
			&job.Job{Name: "j2", Priority: 1},
			true,
		},
		// Reversed, higher priority item first
		{
			&job.Job{Name: "j1", Priority: 1},
			&job.Job{Name: "j2", Priority: 0},
			false,
		},
		// Same priority, but older by created time
		{
			&job.Job{Name: "j1", Priority: 0, Created: time.Now().UTC()},
			&job.Job{Name: "j2", Priority: 0, Created: time.Now().UTC().Add(-1 * time.Second)},
			true,
		},
		// Reversed, same priority, but older by created time first
		{
			&job.Job{Name: "j1", Priority: 0, Created: time.Now().UTC().Add(-1 * time.Second)},
			&job.Job{Name: "j2", Priority: 0, Created: time.Now().UTC()},
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

func TestAddDuplicate(t *testing.T) {
	q := NewWorkQueue()
	assertEmpty(t, q)

	j := job.NewEmptyJob()
	if !q.Add(j) {
		t.Fatalf("Unable to add")
	}
	if q.Add(j) {
		t.Fatalf("Should not be able to add duplicate")
	}
}

func TestSchedule(t *testing.T) {
	q := NewWorkQueue()

	j := job.NewEmptyJob()
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

func TestScheduleDuplicate(t *testing.T) {
	q := NewWorkQueue()
	assertEmpty(t, q)

	j := job.NewEmptyJob()
	j.Time = time.Now().UTC()
	if !q.Schedule(j) {
		t.Fatalf("Unable to schedule")
	}
	if q.Schedule(j) {
		t.Fatalf("Should not be able to schedule duplicate")
	}
}

func TestScheduleAndPop(t *testing.T) {
	q := NewWorkQueue()
	assertEmpty(t, q)

	timeFormat := "2006-01-02T15:04:05Z"
	parseTime := func(value string) time.Time {
		t, _ := time.Parse(timeFormat, value)
		return t
	}

	tests := []struct {
		j1  *job.Job
		j2  *job.Job
		asc bool // Ascending order
	}{
		{
			// j1=Earlier created time
			// j2=Earlier scheduled time
			&job.Job{Name: "j1", Time: time.Now().UTC(), Created: time.Now().UTC().Add(-1 * time.Second)},
			&job.Job{Name: "j2", Time: time.Now().UTC().Add(-100 * time.Second), Created: time.Now().UTC()},
			true,
		},
		// j1=Earlier scheduled time
		// j2=Earlier created time
		{
			&job.Job{Name: "j1", Time: time.Now().UTC().Add(-100 * time.Second), Created: time.Now().UTC()},
			&job.Job{Name: "j2", Time: time.Now().UTC(), Created: time.Now().UTC().Add(-1 * time.Second)},
			false,
		},
		// Same scheduled time
		// j1=Earlier created time
		// j2=Higher priority
		{
			&job.Job{Name: "j1", Time: parseTime("2016-01-01T00:00:00Z"), Created: time.Now().UTC().Add(-1 * time.Second)},
			&job.Job{Name: "j2", Time: parseTime("2016-01-01T00:00:00Z"), Priority: 1, Created: time.Now().UTC()},
			true,
		},
		// Same scheduled time
		// j1=Higher priority
		// j2=Earlier created time
		{
			&job.Job{Name: "j1", Time: parseTime("2016-01-01T00:00:00Z"), Priority: 1, Created: time.Now().UTC()},
			&job.Job{Name: "j2", Time: parseTime("2016-01-01T00:00:00Z"), Created: time.Now().UTC().Add(-1 * time.Second)},
			false,
		},
	}

	var err error
	for _, in := range tests {
		in.j1.ID, err = job.IDFromBytes([]byte(testutil.GenId()))
		if err != nil {
			t.Fatalf("Unable to generate test id")
		}
		in.j2.ID, err = job.IDFromBytes([]byte(testutil.GenId()))
		if err != nil {
			t.Fatalf("Unable to generate test id")
		}

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

func TestLeaseAndWaitForJobs(t *testing.T) {
	q := NewWorkQueue()
	done := make(chan struct{}, 2)

	expectedJ := job.NewEmptyJob()
	expectedJ.ID, _ = job.IDFromBytes([]byte(testutil.GenId()))

	go func(expectedJ *job.Job) {
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

	expectedJ = job.NewEmptyJob()
	expectedJ.ID, _ = job.IDFromBytes([]byte(testutil.GenId()))
	go func(expectedJ *job.Job) {
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

func TestDelete(t *testing.T) {
	q := NewWorkQueue()

	jAdd := job.NewEmptyJob()
	jAdd.ID, _ = job.IDFromBytes([]byte(testutil.GenId()))
	if !q.Add(jAdd) {
		t.Fatalf("Unable to add")
	}
	assertLen(t, q, 1)

	jSched := job.NewEmptyJob()
	jSched.ID, _ = job.IDFromBytes([]byte(testutil.GenId()))
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

func TestExists(t *testing.T) {
	q := NewWorkQueue()

	jAdd := job.NewEmptyJob()
	jAdd.ID, _ = job.IDFromBytes([]byte(testutil.GenId()))
	if q.Exists(jAdd) {
		t.Fatalf("Unexpected item exists")
	}
	if !q.Add(jAdd) {
		t.Fatalf("Unable to add")
	}
	if !q.Exists(jAdd) {
		t.Fatalf("Expected item to exist")
	}

	jSched := job.NewEmptyJob()
	jSched.ID, _ = job.IDFromBytes([]byte(testutil.GenId()))
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

func TestInspectorLens(t *testing.T) {
	q := NewWorkQueue()
	insp := NewInspector(q)
	readyLen, scheduledLen := insp.Lens()
	if readyLen != 0 || scheduledLen != 0 {
		t.Fatalf("Length mismatch")
	}

	jAdd := job.NewEmptyJob()
	jAdd.ID, _ = job.IDFromBytes([]byte(testutil.GenId()))
	if !q.Add(jAdd) {
		t.Fatalf("Unable to add")
	}

	readyLen, scheduledLen = insp.Lens()
	if readyLen != 1 || scheduledLen != 0 {
		t.Fatalf("Length mismatch")
	}

	jSched := job.NewEmptyJob()
	jSched.ID, _ = job.IDFromBytes([]byte(testutil.GenId()))
	if !q.Schedule(jSched) {
		t.Fatalf("Unable to schedule")
	}

	readyLen, scheduledLen = insp.Lens()
	if readyLen != 1 || scheduledLen != 1 {
		t.Fatalf("Length mismatch")
	}
}

func TestInspectIterators(t *testing.T) {
	q := NewWorkQueue()
	insp := NewInspector(q)
	readyIt, scheduledIt := insp.Iterators()

	if readyIt.Current() != nil || scheduledIt.Current() != nil {
		t.Fatalf("Empty queue iterator mismatch")
	}

	jAdd := job.NewEmptyJob()
	jAdd.ID, _ = job.IDFromBytes([]byte(testutil.GenId()))
	if !q.Add(jAdd) {
		t.Fatalf("Unable to add")
	}

	readyIt, _ = insp.Iterators()
	item := readyIt.Current()
	if item.(*job.Job) != jAdd {
		t.Fatalf("Iterator job mismatch")
	}

	jSched := job.NewEmptyJob()
	jSched.ID, _ = job.IDFromBytes([]byte(testutil.GenId()))
	if !q.Schedule(jSched) {
		t.Fatalf("Unable to schedule")
	}

	_, scheduledIt = insp.Iterators()
	item = scheduledIt.Current()
	if item.(*job.Job) != jSched {
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
