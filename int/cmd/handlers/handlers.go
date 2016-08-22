package handlers

import (
	"strconv"
	"sync/atomic"

	"github.com/iamduo/workq/int/job"
	"github.com/iamduo/workq/int/prot"
	"github.com/iamduo/workq/int/wqueue"
)

const (
	maxJobWaitTimeout = 86400000
)

var (
	ErrDuplicateJob       = prot.NewClientErr("Duplicate job")
	ErrDuplicateResult    = prot.NewClientErr("Duplicate result")
	ErrLeaseExpired       = prot.NewClientErr("Lease expired")
	ErrInvalidWaitTimeout = prot.NewClientErr("Invalid wait timeout")
)

type Usage struct {
	EvictedJobs uint64
}

func waitTimeoutFromBytes(b []byte) (uint32, error) {
	timeout, err := strconv.ParseUint(string(b), 10, 32)
	if err != nil || timeout > maxJobWaitTimeout {
		return 0, ErrInvalidWaitTimeout
	}

	return uint32(timeout), nil
}

func expireJob(reg *job.Registry, qc wqueue.ControllerInterface, usage *Usage, id job.ID) {
	rec, ok := reg.Record(id)
	if !ok {
		return
	}

	rec.Mu.RLock()
	processed := rec.Processed()
	rec.Mu.RUnlock()
	if !processed {
		atomic.AddUint64(&usage.EvictedJobs, 1)
	}

	reg.Delete(id)
	qc.Delete(rec.Job)
}
