package cmdlog

import (
	"errors"
	"time"

	"github.com/iamduo/workq/int/captain"
	"github.com/iamduo/workq/int/job"
)

var (
	errTrailingGarbage = errors.New("trailing garbage detected")
)

type nextCursor interface {
	Next() (*captain.Record, error)
}

// Replay iterates through all records and replays them with the job controller.
// Stops on the first error.
// Complete, Fail, Delete commands have an exception for NOT-FOUND errors.
// Procesing proceeds for NOT-FOUND as the jobs they are targeted for may have
// already expired and will be cleaned by the Cleaner.
func Replay(c nextCursor, jc job.ControllerInterface) error {
	var r *captain.Record
	var err error
	for {
		r, err = c.Next()
		if err != nil {
			return err
		}

		// End of segments, error free
		if r == nil {
			break
		}

		d := newDecoder(r.Payload)
		cType, err := d.ReadType()
		if err != nil {
			return err
		}

		switch cType {
		case cmdAdd:
			err = replayAdd(d, jc)
		case cmdSchedule:
			err = replaySchedule(d, jc)
		case cmdComplete:
			err = replayComplete(d, jc)
		case cmdFail:
			err = replayFail(d, jc)
		case cmdDelete:
			err = replayDelete(d, jc)
		case cmdExpire:
			err = replayExpire(d, jc)
		case cmdStartAttempt:
			err = replayStartAttempt(d, jc)
		case cmdTimeoutAttempt:
			err = replayTimeoutAttempt(d, jc)
		}

		if err != nil {
			return err
		}

		b := make([]byte, 1)
		_, err = d.r.Read(b)
		if err == nil {
			return errTrailingGarbage
		}
	}

	return nil
}

func replayAdd(d *decoder, jc job.ControllerInterface) error {
	j, err := d.ReadJob()
	if err != nil {
		return err
	}

	if err = job.ValidateTime(j.Created.Add(time.Duration(j.TTL) * time.Millisecond)); err != nil {
		return nil
	}

	return jc.Add(j)
}

func replaySchedule(d *decoder, jc job.ControllerInterface) error {
	j, err := d.ReadJob()
	if err != nil {
		return err
	}

	if err = job.ValidateTime(j.Time.Add(time.Duration(j.TTL) * time.Millisecond)); err != nil {
		// Skip expired jobs.
		return nil
	}

	return jc.Schedule(j)
}

func replayComplete(d *decoder, jc job.ControllerInterface) error {
	id, err := d.ReadID()
	if err != nil {
		return err
	}

	result, err := d.ReadPayload()
	if err != nil {
		return err
	}

	err = jc.Complete(id, result)
	if err == job.ErrNotFound {
		return nil
	}

	return err
}

func replayFail(d *decoder, jc job.ControllerInterface) error {
	id, err := d.ReadID()
	if err != nil {
		return err
	}

	result, err := d.ReadPayload()
	if err != nil {
		return err
	}

	err = jc.Fail(id, result)
	if err == job.ErrNotFound {
		return nil
	}

	return err
}

func replayDelete(d *decoder, jc job.ControllerInterface) error {
	id, err := d.ReadID()
	if err != nil {
		return err
	}

	err = jc.Delete(id)
	if err == job.ErrNotFound {
		return nil
	}

	return err
}

func replayExpire(d *decoder, jc job.ControllerInterface) error {
	id, err := d.ReadID()
	if err != nil {
		return err
	}

	jc.Expire(id)
	return nil
}

func replayStartAttempt(d *decoder, jc job.ControllerInterface) error {
	id, err := d.ReadID()
	if err != nil {
		return err
	}

	err = jc.StartAttempt(id)
	if err == job.ErrNotFound {
		return nil
	}

	return err
}

func replayTimeoutAttempt(d *decoder, jc job.ControllerInterface) error {
	id, err := d.ReadID()
	if err != nil {
		return err
	}

	jc.TimeoutAttempt(id)
	return nil
}
