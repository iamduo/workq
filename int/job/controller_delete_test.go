package job

import (
	"testing"

	"github.com/iamduo/workq/int/testutil"
)

func TestDelete(t *testing.T) {
	reg := NewRegistry()
	qc := NewQueueController()
	jc := NewController(reg, qc)

	j := NewEmptyJob()
	j.ID = ID(testutil.GenID())
	j.Name = "q1"

	rec := NewRunRecord()
	rec.Job = j
	if !reg.Add(rec) {
		t.Fatalf("Registration failed")
	}

	if !qc.Add(j) {
		t.Fatalf("Enqueue failed")
	}

	err := jc.Delete(j.ID)
	if err != nil {
		t.Fatalf("Delete unexpected err=%v", err)
	}

	_, ok := reg.Record(j.ID)
	if ok {
		t.Fatalf("Unexpected run record after delete")
	}

	if qc.Exists(j) {
		t.Fatalf("Unexpected job found after delete")
	}
}

func TestDeleteNotFound(t *testing.T) {
	reg := NewRegistry()
	qc := NewQueueController()
	jc := NewController(reg, qc)

	err := jc.Delete(ID(testutil.GenID()))
	if err != ErrNotFound {
		t.Fatalf("Expected ErrNotFound, err=%s", err)
	}
}

func TestDeleteInvalidID(t *testing.T) {
	reg := NewRegistry()
	qc := NewQueueController()
	jc := NewController(reg, qc)

	err := jc.Delete(ID([16]byte{}))
	if err != ErrInvalidID {
		t.Fatalf("Expected ErrInvalidID, err=%s", err)
	}
}
