package job

import (
	"testing"
	"time"

	"github.com/iamduo/workq/int/testutil"
)

func TestRegistry(t *testing.T) {
	reg := NewRegistry()
	if reg.Len() != 0 {
		t.Fatalf("Expected registry to be empty")
	}

	rec := NewRunRecord()
	if reg.Add(rec) {
		t.Fatalf("Expected record without job to fail")
	}
	if reg.Len() != 0 {
		t.Fatalf("Expected registry to be empty")
	}

	j := NewEmptyJob()
	j.ID = ID(testutil.GenID())
	rec.Job = j

	if !reg.Add(rec) {
		t.Fatalf("Unable to add valid record")
	}
	if reg.Len() != 1 {
		t.Fatalf("Expected registry to be empty")
	}

	if reg.Add(rec) {
		t.Fatalf("Unexpected re-add of duplicate record")
	}

	lookupRec, ok := reg.Record(j.ID)
	if rec != lookupRec || !ok {
		t.Fatalf("Registry lookup failed to return matching record")
	}

	rec.Timers[RunRecSchedTimerIdx] = NewTimer(1 * time.Second)
	rec.Timers[RunRecTTRTimerIdx] = NewTimer(1 * time.Second)
	rec.Timers[RunRecTTLTimerIdx] = NewTimer(1 * time.Second)

	if !reg.Delete(j.ID) {
		t.Fatalf("Unable to delete existing record")
	}

	for _, timer := range rec.Timers {
		select {
		case <-timer.Cancellation:
		default:
			t.Fatalf("Expected timer to be cancelled after delete")
		}
	}

	lookupRec, ok = reg.Record(j.ID)
	if lookupRec != nil || ok {
		t.Fatalf("Expected lookup to return nil on deleted record")
	}

	if reg.Len() != 0 {
		t.Fatalf("Expected registry to be empty")
	}

	// Delete on non existent key
	if reg.Delete(j.ID) {
		t.Fatalf("Expected delete to be return false on non existent ID")
	}
}

func TestRunRecord(t *testing.T) {
	rec := NewRunRecord()

	if rec.Running() || rec.Processed() || rec.Success() {
		t.Fatalf("Expected run record to be new")
	}

	if !rec.WriteResult(Result("result"), true) {
		t.Fatalf("Unable to write first result")
	}

	select {
	case <-rec.Wait:
	default:
		t.Fatalf("Expected a job result")
	}

	if rec.WriteResult(Result("result"), true) {
		t.Fatalf("Should not be able to write a duplicate result")
	}

	rec.State = StateLeased
	if !rec.Running() || rec.Success() || rec.Processed() {
		t.Fatalf("Run record state mismatch, rec=%+v", rec)
	}

	rec.State = StateFailed
	if rec.Running() || rec.Success() || !rec.Processed() {
		t.Fatalf("Run record state mismatch, rec=%+v", rec)
	}

	rec.State = StateCompleted
	if rec.Running() || !rec.Success() || !rec.Processed() {
		t.Fatalf("Run record state mismatch, rec=%+v", rec)
	}
}

func TestTimer(t *testing.T) {
	dur := 1 * time.Second
	jt := NewTimer(dur)

	diff := jt.Deadline.Sub(time.Now().UTC().Add(dur))
	if diff >= 1*time.Millisecond {
		t.Fatalf("Timer deadline out of range, diff=%v", diff)
	}

	select {
	case <-jt.Cancellation:
		t.Fatalf("Unexpected timer cancellation")
	default:
	}

	jt.Cancel()

	select {
	case <-jt.Cancellation:
	default:
		t.Fatalf("Expected timer to be cancelled")
	}

	// Asserting multiple calls to cancel do not block
	start := time.Now().UTC()
	jt.Cancel()
	jt.Cancel()
	if start.Sub(time.Now().UTC()) >= 1*time.Microsecond {
		t.Fatalf("Multiple timer cancellations out of range")
	}
}
