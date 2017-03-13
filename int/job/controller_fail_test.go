package job

import (
	"bytes"
	"testing"
	"time"

	"github.com/iamduo/workq/int/testutil"
)

func TestFail(t *testing.T) {
	tests := []struct {
		result []byte
	}{
		{result: []byte("")},
		{result: []byte("a")},
		{result: make([]byte, MaxResult)},
	}

	for _, tt := range tests {
		reg := NewRegistry()
		qc := NewQueueController()
		jc := NewController(reg, qc)

		j := NewEmptyJob()
		j.ID = ID(testutil.GenID())
		j.Name = "q1"

		rec := NewRunRecord()
		rec.Job = j
		rec.Timers[RunRecTTRTimerIdx] = NewTimer(10 * time.Millisecond)
		if !reg.Add(rec) {
			t.Fatalf("Registration failed")
		}

		if !qc.Add(j) {
			t.Fatalf("Enqueue failed")
		}

		err := jc.Fail(j.ID, tt.result)
		if err != nil {
			t.Fatalf("Fail unexpected err=%v", err)
		}

		rec, ok := reg.Record(j.ID)
		rec.Mu.RLock()
		if !ok {
			t.Fatalf("Expected run record")
		}

		if qc.Exists(j) {
			t.Fatalf("Unexpected job found after fail")
		}

		if rec.State != StateFailed {
			t.Fatalf("Unexpected job state=%v", rec.State)
		}

		if !bytes.Equal(tt.result, rec.Result) {
			t.Fatalf("Job result mismatch, exp=%v, act=%v", tt.result, rec.Result)
		}
		rec.Mu.RUnlock()
	}
}

func TestFailDuplicateResult(t *testing.T) {
	reg := NewRegistry()
	qc := NewQueueController()
	jc := NewController(reg, qc)

	j := NewEmptyJob()
	j.ID = ID(testutil.GenID())
	j.Name = "q1"

	rec := NewRunRecord()
	rec.Job = j
	rec.Timers[RunRecTTRTimerIdx] = NewTimer(10 * time.Millisecond)
	if !reg.Add(rec) {
		t.Fatalf("Registration failed")
	}

	if !qc.Add(j) {
		t.Fatalf("Enqueue failed")
	}

	err := jc.Fail(j.ID, []byte{})
	if err != nil {
		t.Fatalf("Fail unexpected err=%v", err)
	}

	err = jc.Fail(j.ID, []byte{})
	if err != ErrDuplicateResult {
		t.Fatalf("Expected ErrDuplicateResult, got=%v", err)
	}
}

func TestFailNotFound(t *testing.T) {
	reg := NewRegistry()
	qc := NewQueueController()
	jc := NewController(reg, qc)

	err := jc.Fail(ID(testutil.GenID()), []byte{})
	if err != ErrNotFound {
		t.Fatalf("Expected ErrNotFound, err=%s", err)
	}
}

func TestFailInvalidArgs(t *testing.T) {
	reg := NewRegistry()
	qc := NewQueueController()
	jc := NewController(reg, qc)

	tests := []struct {
		id     ID
		result []byte
		expErr error
	}{
		{
			ID([16]byte{}),
			[]byte{},
			ErrInvalidID,
		},
		{
			ID(testutil.GenID()),
			make([]byte, MaxResult+1),
			ErrInvalidResult,
		},
	}

	for _, tt := range tests {
		err := jc.Fail(tt.id, tt.result)
		if err != tt.expErr {
			t.Fatalf("Err mismatch, err=%s", err)
		}
	}
}
