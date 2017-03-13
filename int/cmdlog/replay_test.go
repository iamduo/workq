package cmdlog

import (
	"bytes"
	"encoding/hex"
	"errors"
	"testing"
	"time"

	"github.com/iamduo/workq/int/captain"
	"github.com/iamduo/workq/int/job"
)

type testCursor struct {
	values []*testCursorValue
}

func (c *testCursor) Next() (*captain.Record, error) {
	if len(c.values) == 0 {
		return nil, nil
	}

	var cv *testCursorValue
	cv, c.values = c.values[len(c.values)-1], c.values[:len(c.values)-1]
	return cv.rec, cv.err
}

type testCursorValue struct {
	rec *captain.Record
	err error
}

func TestReplayCursorNextErr(t *testing.T) {
	expErr := errors.New("test error")
	c := &testCursor{[]*testCursorValue{
		&testCursorValue{nil, expErr},
	}}

	err := Replay(c, &controllerMock{})
	if err != expErr {
		t.Fatalf("got=%v, exp=%v", err, expErr)
	}
}

func TestReplayAdd(t *testing.T) {
	j := &job.Job{
		ID:          [16]byte{0x61, 0xa4, 0x44, 0xa0, 0x61, 0x28, 0x41, 0xc0, 0x80, 0x78, 0xcc, 0x75, 0x7d, 0x3b, 0xd2, 0xd8},
		Name:        "ping",
		TTR:         1000,
		TTL:         60000,
		Priority:    10,
		MaxAttempts: 3,
		MaxFails:    1,
		Payload:     []byte("a"),
		Created:     time.Now().Add(1 * time.Hour),
	}
	rec := captain.NewRecord(marshalAdd(j))
	cursor := &testCursor{
		values: []*testCursorValue{
			{rec, nil},
		},
	}
	var invoked bool
	jc := &controllerMock{
		addMethod: func(proxyJ *job.Job) error {
			invoked = true
			if !compareJobs(j, proxyJ) {
				t.Fatalf("Job replay mismatch, got=%+v", proxyJ)
			}

			return nil
		},
	}
	err := Replay(cursor, jc)
	if err != nil {
		t.Fatalf("Replay err=%s", err)
	}

	if !invoked {
		t.Fatal("Expected job controller call")
	}
}

func TestReplayAddExpired(t *testing.T) {
	j := &job.Job{
		ID:          [16]byte{0x61, 0xa4, 0x44, 0xa0, 0x61, 0x28, 0x41, 0xc0, 0x80, 0x78, 0xcc, 0x75, 0x7d, 0x3b, 0xd2, 0xd8},
		Name:        "ping",
		TTR:         1000,
		TTL:         60000,
		Priority:    10,
		MaxAttempts: 3,
		MaxFails:    1,
		Payload:     []byte("a"),
		Created:     time.Now().Add(-1 * time.Hour),
	}
	rec := captain.NewRecord(marshalAdd(j))
	cursor := &testCursor{
		values: []*testCursorValue{
			{rec, nil},
		},
	}
	var invoked bool
	jc := &controllerMock{
		addMethod: func(proxyJ *job.Job) error {
			invoked = true
			return nil
		},
	}
	err := Replay(cursor, jc)
	if err != nil {
		t.Fatalf("Replay err=%s", err)
	}

	if invoked {
		t.Fatalf("Unexpected job controller call")
	}
}

func TestReplaySchedule(t *testing.T) {
	j := &job.Job{
		ID:          [16]byte{0x61, 0xa4, 0x44, 0xa0, 0x61, 0x28, 0x41, 0xc0, 0x80, 0x78, 0xcc, 0x75, 0x7d, 0x3b, 0xd2, 0xd8},
		Name:        "ping",
		TTR:         1000,
		TTL:         60000,
		Priority:    10,
		MaxAttempts: 3,
		MaxFails:    1,
		Payload:     []byte("a"),
		Time:        time.Now().UTC().Add(1 * time.Hour),
		Created:     time.Date(2016, time.December, 1, 10, 0, 0, 0, time.UTC),
	}
	rec := captain.NewRecord(marshalSchedule(j))
	cursor := &testCursor{
		values: []*testCursorValue{
			{rec, nil},
		},
	}
	var invoked bool
	controller := &controllerMock{
		scheduleMethod: func(proxyJ *job.Job) error {
			invoked = true
			if !compareJobs(j, proxyJ) {
				t.Fatalf("Job replay mismatch, got=%+v", proxyJ)
			}

			return nil
		},
	}

	err := Replay(cursor, controller)
	if err != nil {
		t.Fatalf("Replay err=%s", err)
	}

	if !invoked {
		t.Fatal("Expected job controller call")
	}
}

func TestReplayScheduleExpired(t *testing.T) {
	j := &job.Job{
		ID:          [16]byte{0x61, 0xa4, 0x44, 0xa0, 0x61, 0x28, 0x41, 0xc0, 0x80, 0x78, 0xcc, 0x75, 0x7d, 0x3b, 0xd2, 0xd8},
		Name:        "ping",
		TTR:         1000,
		TTL:         60000,
		Priority:    10,
		MaxAttempts: 3,
		MaxFails:    1,
		Payload:     []byte("a"),
		Time:        time.Now().UTC().Add(-1 * time.Hour),
		Created:     time.Date(2016, time.December, 1, 10, 0, 0, 0, time.UTC),
	}
	rec := captain.NewRecord(marshalSchedule(j))
	cursor := &testCursor{
		values: []*testCursorValue{
			{rec, nil},
		},
	}
	var invoked bool
	controller := &controllerMock{
		scheduleMethod: func(proxyJ *job.Job) error {
			invoked = true
			return nil
		},
	}

	err := Replay(cursor, controller)
	if err != nil {
		t.Fatalf("Replay err=%s", err)
	}

	if invoked {
		t.Fatal("Unexpected job controller call")
	}
}

func TestReplayComplete(t *testing.T) {
	id := [16]byte{0x61, 0xa4, 0x44, 0xa0, 0x61, 0x28, 0x41, 0xc0, 0x80, 0x78, 0xcc, 0x75, 0x7d, 0x3b, 0xd2, 0xd8}
	result := []byte("a")
	rec := captain.NewRecord(marshalComplete(id, result))
	cursor := &testCursor{
		values: []*testCursorValue{
			{rec, nil},
		},
	}
	controller := &controllerMock{
		completeMethod: func(proxyID job.ID, proxyResult []byte) error {
			if id != proxyID || !bytes.Equal(result, proxyResult) {
				t.Fatalf("Complete mismatch, id=%x, result=%x", proxyID, proxyResult)
			}

			return nil
		},
	}

	err := Replay(cursor, controller)
	if err != nil {
		t.Fatalf("Replay err=%s", err)
	}
}

func TestReplayCompleteNotFound(t *testing.T) {
	id := [16]byte{0x61, 0xa4, 0x44, 0xa0, 0x61, 0x28, 0x41, 0xc0, 0x80, 0x78, 0xcc, 0x75, 0x7d, 0x3b, 0xd2, 0xd8}
	result := []byte("a")
	rec := captain.NewRecord(marshalComplete(id, result))
	cursor := &testCursor{
		values: []*testCursorValue{
			{rec, nil},
		},
	}
	var invoked bool
	controller := &controllerMock{
		completeMethod: func(proxyID job.ID, proxyResult []byte) error {
			invoked = true
			return job.ErrNotFound
		},
	}

	err := Replay(cursor, controller)
	if err != nil {
		t.Fatalf("Replay err=%s", err)
	}

	if !invoked {
		t.Fatalf("Expected job controller call")
	}
}

func TestReplayFail(t *testing.T) {
	id := [16]byte{0x61, 0xa4, 0x44, 0xa0, 0x61, 0x28, 0x41, 0xc0, 0x80, 0x78, 0xcc, 0x75, 0x7d, 0x3b, 0xd2, 0xd8}
	result := []byte("a")
	rec := captain.NewRecord(marshalFail(id, result))
	cursor := &testCursor{
		values: []*testCursorValue{
			{rec, nil},
		},
	}
	var invoked bool
	controller := &controllerMock{
		failMethod: func(proxyID job.ID, proxyResult []byte) error {
			invoked = true
			if id != proxyID || !bytes.Equal(result, proxyResult) {
				t.Fatalf("Fail mismatch, id=%x, result=%x", proxyID, proxyResult)
			}

			return nil
		},
	}

	err := Replay(cursor, controller)
	if err != nil {
		t.Fatalf("Replay err=%s", err)
	}

	if !invoked {
		t.Fatalf("Expected job controller call")
	}
}

func TestReplayFailNotFound(t *testing.T) {
	id := [16]byte{0x61, 0xa4, 0x44, 0xa0, 0x61, 0x28, 0x41, 0xc0, 0x80, 0x78, 0xcc, 0x75, 0x7d, 0x3b, 0xd2, 0xd8}
	result := []byte("a")
	rec := captain.NewRecord(marshalFail(id, result))
	cursor := &testCursor{
		values: []*testCursorValue{
			{rec, nil},
		},
	}
	var invoked bool
	controller := &controllerMock{
		failMethod: func(proxyID job.ID, proxyResult []byte) error {
			invoked = true
			return job.ErrNotFound
		},
	}

	err := Replay(cursor, controller)
	if err != nil {
		t.Fatalf("Replay err=%s", err)
	}

	if !invoked {
		t.Fatalf("Expected job controller call")
	}
}

func TestReplayDelete(t *testing.T) {
	id := [16]byte{0x61, 0xa4, 0x44, 0xa0, 0x61, 0x28, 0x41, 0xc0, 0x80, 0x78, 0xcc, 0x75, 0x7d, 0x3b, 0xd2, 0xd8}
	rec := captain.NewRecord(marshalDelete(id))
	cursor := &testCursor{
		values: []*testCursorValue{
			{rec, nil},
		},
	}
	var invoked bool
	controller := &controllerMock{
		deleteMethod: func(proxyID job.ID) error {
			invoked = true
			if id != proxyID {
				t.Fatalf("Delete mismatch, exp=%s, got=%s", id, proxyID)
			}

			return nil
		},
	}

	err := Replay(cursor, controller)
	if err != nil {
		t.Fatalf("Replay err=%s", err)
	}

	if !invoked {
		t.Fatalf("Expected job controller call")
	}
}

func TestReplayDeleteNotFound(t *testing.T) {
	id := [16]byte{0x61, 0xa4, 0x44, 0xa0, 0x61, 0x28, 0x41, 0xc0, 0x80, 0x78, 0xcc, 0x75, 0x7d, 0x3b, 0xd2, 0xd8}
	rec := captain.NewRecord(marshalDelete(id))
	cursor := &testCursor{
		values: []*testCursorValue{
			{rec, nil},
		},
	}
	var invoked bool
	controller := &controllerMock{
		deleteMethod: func(proxyID job.ID) error {
			invoked = true
			if id != proxyID {
				t.Fatalf("Delete mismatch, exp=%s, got=%s", id, proxyID)
			}

			return job.ErrNotFound
		},
	}

	err := Replay(cursor, controller)
	if err != nil {
		t.Fatalf("Replay err=%s", err)
	}

	if !invoked {
		t.Fatalf("Expected job controller call")
	}
}

func TestReplayExpire(t *testing.T) {
	id := [16]byte{0x61, 0xa4, 0x44, 0xa0, 0x61, 0x28, 0x41, 0xc0, 0x80, 0x78, 0xcc, 0x75, 0x7d, 0x3b, 0xd2, 0xd8}

	b, _ := hex.DecodeString("0661a444a0612841c08078cc757d3bd2d8")
	rec := captain.NewRecord(b)
	cursor := &testCursor{
		values: []*testCursorValue{
			{rec, nil},
		},
	}
	controller := &controllerMock{
		expireMethod: func(proxyID job.ID) {
			if id != proxyID {
				t.Fatalf("Expire mismatch, exp=%s, got=%s", id, proxyID)
			}

		},
	}

	err := Replay(cursor, controller)
	if err != nil {
		t.Fatalf("Replay err=%s", err)
	}
}

func TestReplayStartAttempt(t *testing.T) {
	expID := job.ID([16]byte{0x61, 0xa4, 0x44, 0xa0, 0x61, 0x28, 0x41, 0xc0, 0x80, 0x78, 0xcc, 0x75, 0x7d, 0x3b, 0xd2, 0xd8})
	rec := captain.NewRecord(marshalStartAttempt(expID))
	cursor := &testCursor{
		values: []*testCursorValue{
			{rec, nil},
		},
	}
	invoked := false
	controller := &controllerMock{
		startAttemptMethod: func(jid job.ID) error {
			invoked = true
			if jid != expID {
				t.Fatalf("ID mismatch, got=%v, exp=%v", jid, expID)
			}

			return nil
		},
	}

	err := Replay(cursor, controller)
	if err != nil {
		t.Fatalf("Replay err=%s", err)
	}

	if !invoked {
		t.Fatalf("Expected job controller call")
	}
}

func TestReplayStartAttemptNotFound(t *testing.T) {
	expID := job.ID([16]byte{0x61, 0xa4, 0x44, 0xa0, 0x61, 0x28, 0x41, 0xc0, 0x80, 0x78, 0xcc, 0x75, 0x7d, 0x3b, 0xd2, 0xd8})
	rec := captain.NewRecord(marshalStartAttempt(expID))
	cursor := &testCursor{
		values: []*testCursorValue{
			{rec, nil},
		},
	}
	invoked := false
	controller := &controllerMock{
		startAttemptMethod: func(jid job.ID) error {
			invoked = true
			return job.ErrNotFound
		},
	}

	err := Replay(cursor, controller)
	if err != nil {
		t.Fatalf("Replay err=%s", err)
	}

	if !invoked {
		t.Fatalf("Expected job controller call")
	}
}

func TestReplayTimeoutAttempt(t *testing.T) {
	id := [16]byte{0x61, 0xa4, 0x44, 0xa0, 0x61, 0x28, 0x41, 0xc0, 0x80, 0x78, 0xcc, 0x75, 0x7d, 0x3b, 0xd2, 0xd8}

	b, _ := hex.DecodeString("0861a444a0612841c08078cc757d3bd2d8")
	rec := captain.NewRecord(b)
	cursor := &testCursor{
		values: []*testCursorValue{
			{rec, nil},
		},
	}
	var invoked bool
	controller := &controllerMock{
		timeoutAttemptMethod: func(proxyID job.ID) {
			invoked = true
			if id != proxyID {
				t.Fatalf("TimeoutAttempt mismatch, exp=%s, got=%s", id, proxyID)
			}
		},
	}

	err := Replay(cursor, controller)
	if err != nil {
		t.Fatalf("Replay err=%s", err)
	}

	if !invoked {
		t.Fatalf("Expected job controller call")
	}
}

func TestReplayTrailingGarbage(t *testing.T) {
	b, _ := hex.DecodeString("1000")
	rec := captain.NewRecord(b)
	cursor := &testCursor{
		values: []*testCursorValue{
			{rec, nil},
		},
	}
	controller := &controllerMock{}
	err := Replay(cursor, controller)
	if err != errTrailingGarbage {
		t.Fatalf("Err mismatch, got=%s", err)
	}
}

func TestReplayInvalidType(t *testing.T) {
	rec := captain.NewRecord([]byte{})
	cursor := &testCursor{[]*testCursorValue{
		&testCursorValue{rec, nil},
	}}
	err := Replay(cursor, &controllerMock{})
	if err == nil {
		t.Fatalf("Got nil, expected an err=%v", err)
	}
}

func TestReplayInvalidRecords(t *testing.T) {
	tests := [][]byte{
		[]byte{byte(cmdAdd)},
		[]byte{byte(cmdSchedule)},
		[]byte{byte(cmdComplete)},
		[]byte{byte(cmdComplete), byte(1)}, // Invalid payload.
		[]byte{byte(cmdFail)},
		[]byte{byte(cmdFail), byte(1)}, // Invalid payload.
		[]byte{byte(cmdDelete)},
		[]byte{byte(cmdExpire)},
		[]byte{byte(cmdStartAttempt)},
		[]byte{byte(cmdTimeoutAttempt)},
	}

	for _, b := range tests {
		cursor := &testCursor{[]*testCursorValue{
			&testCursorValue{captain.NewRecord(b), nil},
		}}

		err := Replay(cursor, &controllerMock{})
		if err == nil {
			t.Fatalf("Got nil, expected an err=%v", err)
		}
	}
}
