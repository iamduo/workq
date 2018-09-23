package captain

import (
	"bytes"
	"path"
	"reflect"
	"strconv"
	"testing"
	"time"
)

func TestCursorNext(t *testing.T) {
	dir := "./test/cursor-next"
	s := NewStream(dir, testMagicHeader)
	c, err := s.OpenCursor()
	if err != nil {
		t.Fatalf("Unable to acquire cursor")
	}

	for i := 1; i <= 6; i++ {
		r, err := c.Next()
		if r == nil || err != nil {
			t.Fatalf("Unexpected Cursor Next r=%v, err=%s, i=%d", r, err, i)
		}

		expPay := []byte(strconv.Itoa(i))
		if !bytes.Equal(expPay, r.Payload) {
			t.Fatalf("Payload mismatch, act=%v, exp=%v", r.Payload, expPay)
		}
	}

	r, err := c.Next()
	if r != nil || err != nil {
		t.Fatalf("Cursor Next mismatch, act.record=%+v, act.err=%s, exp.record=nil, exp.err=nil", r, err)
	}
}

// Test for using a cursor on a directory with mixed
// invalid segment files. Ensures that all invalid segments
// will be skipped.
func TestCursorWithMixedInvalidSegments(t *testing.T) {
	s := NewStream("./test/cursor-mixed-invalid-segs", testMagicHeader)
	c, err := s.OpenCursor()
	if err != nil {
		t.Fatalf("Open Cursor err=%s", err)
	}

	for i := 1; i <= 3; i++ {
		r, err := c.Next()
		if r == nil || err != nil {
			t.Fatalf("Unexpected Cursor Next r=%v, err=%s, i=%d", r, err, i)
		}

		expPay := []byte(strconv.Itoa(i))
		if !bytes.Equal(expPay, r.Payload) {
			t.Fatalf("Payload mismatch, act=%v, exp=%v", r.Payload, expPay)
		}
	}

	r, err := c.Next()
	if r != nil || err != nil {
		t.Fatalf("Cursor Next mismatch, act.record=%+v, act.err=%s, exp.record=nil, exp.err=nil", r, err)
	}

	/*for i := 1; i <= 3; i++ {

	}
	// Next
	r, err := c.Next()
	if err != nil {
		t.Fatalf("Cursor Next err=%s", err)
	}*/

	/*exp := &segmentInfo{name: "test/cursor-mixed-invalid-segs/000000003.log", seq: 3}
	if len(c.segments) != 1 || *c.segments[0] != *exp {
		t.Fatalf("Segment mismatch, act=%+v, exp=%+v", c.segments, exp)
	}*/
}

func TestCursorNewWithInvalidDir(t *testing.T) {
	s := NewStream("./test/does-not-exist", testMagicHeader)
	_, err := s.OpenCursor()
	if err == nil {
		t.Fatalf("Expected invalid dir err")
	}
}

// Test for a segment file expected to exist / readable,
// but fails to be read during actual cursor iteration.
func TestCursorWithUnreadableSegmentFile(t *testing.T) {
	s := NewStream("./test/cursor-next", testMagicHeader)
	c, err := s.OpenCursor()
	if err != nil {
		t.Fatalf("Open Cursor err=%s", err)
	}

	c.segments = []*segmentInfo{
		&segmentInfo{name: "./test/cursor-next/does-not-exist.log", seq: 1},
	}

	r, err := c.Next()
	if r != nil || err == nil {
		t.Fatalf("Cursor Next mismatch, act.record=%v, act.err=%s, exp err", r, err)
	}
}

func TestCursorSegmentInvalidHeader(t *testing.T) {
	s := NewStream("./test/invalid-header", testMagicHeader)
	c, err := s.OpenCursor()
	if err != nil {
		t.Fatalf("Open Cursor err=%s", err)
	}

	_, err = c.Next()
	if err == nil {
		t.Fatalf("Cursor Next err, act=%s, exp=Invalid header err", err)
	}
}

func TestCursorSegment(t *testing.T) {
	dir := "./test/cursor-next"
	s := NewStream(dir, testMagicHeader)
	c, err := s.OpenCursor()
	if err != nil {
		t.Fatalf("Open Cursor err=%s", err)
	}

	for i := 1; i <= 6; i++ {
		r, err := c.Next()
		if r == nil || err != nil {
			t.Fatalf("Unexpected Cursor Next r=%v, err=%s, i=%d", r, err, i)
		}

		switch i {
		case 1:
			exp := path.Clean(dir + "/000000001.log")
			if c.Segment() != exp {
				t.Fatalf("Segment name act=%s, exp=%s", c.Segment(), exp)
			}
		case 4:
			exp := path.Clean(dir + "/000000002.log")
			if c.Segment() != exp {
				t.Fatalf("Segment name act=%s, exp=%s", c.Segment(), exp)
			}
		}
	}

	r, err := c.Next()
	if r != nil || err != nil {
		t.Fatalf("Expected nil, record=%+v, err=%s", r, err)
	}

	if c.Segment() != "" {
		t.Fatalf("Segment act=%s, exp=\"\"", c.Segment())
	}
}

// Test to ensure multiple cursors can take locks which are read locks.
func TestCursorReadLockUnlock(t *testing.T) {
	dir := "./test/cursor-next"
	s := NewStream(dir, testMagicHeader)
	c, err := s.OpenCursor()
	if err != nil {
		t.Fatalf("Open Cursor err=%s", err)
	}

	if err := c.Lock(); err != nil {
		t.Fatalf("Cursor Lock err=%s", err)
	}
	defer c.Unlock()

	done := make(chan struct{})
	go func() {
		c, err := s.OpenCursor()
		if err != nil {
			t.Fatalf("Open Cursor err=%s", err)
		}

		if err := c.Lock(); err != nil {
			t.Fatalf("Cursor Lock err=%s", err)
		}
		defer c.Unlock()

		close(done)
	}()

	timer := time.NewTimer(1000 * time.Millisecond)
	select {
	case <-done:
	case <-timer.C:
		t.Fatalf("Expected secondary lock")
	}

	err = c.Unlock()
	if err != nil {
		t.Fatalf("Cursor Unlock err=%s", err)
	}
}

func TestCursorReset(t *testing.T) {
	dir := "./test/cursor-next"
	s := NewStream(dir, testMagicHeader)
	c, err := s.OpenCursor()
	if err != nil {
		t.Fatalf("Unable to acquire cursor")
	}

	recA, err := c.Next()
	if err != nil {
		t.Fatalf("Cursor Next err=%s", err)
	}

	err = c.Reset()
	if err != nil {
		t.Fatalf("Cursor Reset err=%s", err)
	}

	recB, err := c.Next()
	if err != nil {
		t.Fatalf("Cursor Next err=%s", err)
	}

	if !reflect.DeepEqual(recB, recA) {
		t.Fatalf("Reset Cursor Record mismatch, act=%+v, exp=%+v", recB, recA)
	}
}
