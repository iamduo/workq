package cmdlog

import (
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/iamduo/workq/int/captain"
	"github.com/iamduo/workq/int/job"
	"github.com/iamduo/workq/int/testutil"
)

type testSegCursor struct {
	values []*testSegCursorValue
	n      int
}

func (c *testSegCursor) Next() (*captain.Record, error) {
	if len(c.values) == 0 {
		return nil, nil
	}

	var cv *testSegCursorValue
	cv, c.values = c.values[len(c.values)-1], c.values[:len(c.values)-1]
	c.n++
	return cv.rec, cv.err
}

func (c *testSegCursor) Segment() string {
	if len(c.values) == 0 {
		return ""
	}

	return c.values[0].seg
}

type testSegCursorValue struct {
	seg string
	rec *captain.Record
	err error
}

func TestNewWarmedCleaner(t *testing.T) {
	id := job.ID(testutil.GenID())
	cur := &testSegCursor{
		[]*testSegCursorValue{
			&testSegCursorValue{
				"path",
				captain.NewRecord(marshalDelete(id)),
				nil,
			},
		},
		0,
	}
	_, err := NewWarmedCommandCleaner(job.NewRegistry(), cur)
	if err != nil {
		t.Fatalf("got=%v, want=nil", err)
	}

	if cur.n != 1 {
		t.Fatalf("got=%d, want=1", cur.n)
	}
}

func TestNewWarmedCleanerErrors(t *testing.T) {
	tests := []*testSegCursor{
		&testSegCursor{
			[]*testSegCursorValue{
				&testSegCursorValue{
					"path",
					captain.NewRecord([]byte("bad cmd")),
					nil,
				},
			},
			0,
		},
		&testSegCursor{
			[]*testSegCursorValue{
				&testSegCursorValue{
					"path",
					nil,
					errors.New("test"),
				},
			},
			0,
		},
	}

	for _, cur := range tests {
		_, err := NewWarmedCommandCleaner(job.NewRegistry(), cur)
		if err == nil {
			t.Fatalf("got=%v, want=nil", err)
		}
	}
}

func TestClean(t *testing.T) {
	id := job.ID(testutil.GenID())
	tests := [][]byte{
		marshalAdd(job.NewEmptyJob()),
		marshalSchedule(job.NewEmptyJob()),
		marshalComplete(id, []byte{}),
		marshalFail(id, []byte{}),
		marshalDelete(id),
		marshalStartAttempt(id),
		marshalTimeoutAttempt(id),
	}

	cleaner := newCommandCleaner(job.NewRegistry())
	for _, v := range tests {
		r := captain.NewRecord(v)
		ok, err := cleaner.Clean("", r)
		if ok != true || err != nil {
			t.Fatalf("got ok=%v, err=%v, want ok=true, err=nil", ok, err)
		}
	}
}

func TestCleanInvalidRecords(t *testing.T) {
	tests := [][]byte{
		[]byte{},
		[]byte{byte(cmdAdd)},
		[]byte{byte(cmdSchedule)},
		[]byte{byte(cmdComplete)},
		[]byte{byte(cmdFail)},
		[]byte{byte(cmdDelete)},
		[]byte{byte(cmdExpire)},
		[]byte{byte(cmdStartAttempt)},
		[]byte{byte(cmdTimeoutAttempt)},
	}

	cleaner := newCommandCleaner(job.NewRegistry())
	for _, v := range tests {
		r := captain.NewRecord(v)
		ok, err := cleaner.Clean("", r)
		if ok != false || err == nil {
			t.Fatalf("got ok=%v, err=%v, want ok=true, err!=nil", ok, err)
		}
	}
}

func TestCleanByJob(t *testing.T) {
	tests := []struct {
		j  *job.Job
		ok bool
	}{
		// Valid
		{
			&job.Job{
				Created: time.Now().UTC(),
				TTL:     1000,
			},
			false,
		},
		// Expired
		{
			&job.Job{
				Created: time.Now().Add(-1 * time.Hour).UTC(),
				TTL:     1000,
			},
			true,
		},
		// Valid Scheduled
		{
			&job.Job{
				Time: time.Now().UTC(),
				TTL:  1000,
			},
			false,
		},
		// Expired Scheduled
		{
			&job.Job{
				Time: time.Now().Add(-1 * time.Hour).UTC(),
				TTL:  1000,
			},
			true,
		},
	}

	cleaner := newCommandCleaner(job.NewRegistry())
	for _, tt := range tests {
		ok, exp := cleaner.cleanByJob(tt.j)
		if ok != tt.ok {
			t.Fatalf("got.ok=%v, exp.ok=%v, j=%+v", ok, tt.ok, tt.j)
		}

		if ok && exp != (time.Time{}) {
			t.Fatalf("got=%v, want=nil time", exp)
		} else if !ok && exp != tt.j.Expiration() {
			t.Fatalf("got=%v, want=%v", exp, tt.j.Expiration())
		}
	}
}

func TestCleanByID(t *testing.T) {
	id := job.ID(testutil.GenID())
	reg := job.NewRegistry()
	cleaner := newCommandCleaner(reg)
	ok, exp := cleaner.cleanByID(id)
	if ok != true || exp != (time.Time{}) {
		t.Fatalf("got ok=%v, exp=%v, want ok=true, exp=nil", ok, exp)
	}

	rec := job.NewRunRecord()
	rec.Job = &job.Job{ID: id, Created: time.Now().UTC()}
	reg.Add(rec)
	ok, exp = cleaner.cleanByID(id)
	if ok != false || exp != rec.Job.Expiration() {
		t.Fatalf("got ok=%v, exp=%v, want ok=false, exp=%v", ok, exp, rec.Job.Expiration())
	}
}

func TestCleanExpirationAverage(t *testing.T) {
	cleaner := newCommandCleaner(job.NewRegistry())
	path := "a"
	j := &job.Job{
		Created: time.Now().UTC(),
		TTL:     0,
	}

	// No stats recorded for expired jobs.
	time.Sleep(1 * time.Millisecond)
	cleaner.Clean(path, captain.NewRecord(marshalAdd(j)))
	stat := cleaner.stats[path]
	if stat.total != 0 {
		t.Fatalf("got=%d, want=0", stat.total)
	}

	j.TTL = 60000
	cleaner.Clean(path, captain.NewRecord(marshalAdd(j)))
	stat = cleaner.stats[path]
	if stat.total < int64(j.TTL-10) || stat.total > int64(j.TTL+10) {
		t.Fatalf("got=%v, want=60000 -+10", stat.total)
	}
}

func TestCleanExpirationSkip(t *testing.T) {
	cleaner := newCommandCleaner(job.NewRegistry())
	j := &job.Job{
		Created: time.Now().UTC(),
		TTL:     5,
	}
	rec := captain.NewRecord(marshalAdd(j))

	// No stats recorded for expired jobs.
	ok, err := cleaner.Clean("a", rec)
	if ok != false || err != nil {
		t.Fatalf("got.ok=%v, want.ok=false, got.err=%v, want.err=nil", ok, err)
	}

	cleaner.Clean("b", rec)

	ok, err = cleaner.Clean("a", rec)
	if err != captain.ErrSkipSegment {
		t.Fatalf("got=%v, want=%v", err, captain.ErrSkipSegment.Error())
	}

	time.Sleep(5 * time.Millisecond)
	ok, err = cleaner.Clean("a", rec)
	if err != nil {
		t.Fatalf("got=%v, want=nil", err)
	}
}

type testCleaner struct{}

func (c *testCleaner) Clean(fn captain.CleanFn) error {
	fn("path", captain.NewRecord([]byte("record")))
	return nil
}

func (c *testCleaner) Lock() error {
	return nil
}

func (c *testCleaner) Unlock() error {
	return nil
}

func TestStartCleaningCycle(t *testing.T) {
	cleaner := &testCleaner{}
	var n uint32
	fn := func(path string, r *captain.Record) (bool, error) {
		atomic.AddUint32(&n, 1)
		return false, nil
	}
	StartCleaningCycle(cleaner, fn, 100)
	time.Sleep(250 * time.Millisecond)

	got := atomic.LoadUint32(&n)
	if got != 2 {
		t.Fatalf("Attempts got=%d, want=2", got)
	}
}
