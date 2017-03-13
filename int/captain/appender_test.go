package captain

import (
	"bytes"
	"encoding/binary"
	"errors"
	"io"
	"io/ioutil"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"
)

func TestAppend(t *testing.T) {
	dir, err := ioutil.TempDir("", "test-captain")
	if err != nil {
		t.Fatal(err)
	}

	defer os.RemoveAll(dir)

	s := NewStream(dir, testMagicHeader)
	a, err := s.OpenAppender(nil)
	if err != nil {
		t.Fatalf("Open Appender err=%s", err)
	}

	tests := [][]byte{
		[]byte("1"),
		[]byte("2"),
		[]byte("3"),
	}
	startTime := time.Now().UTC()
	for _, v := range tests {
		err = a.Append(v)
		if err != nil {
			t.Fatalf("Append err=%s", err)
		}
	}
	endTime := time.Now().UTC()

	c, err := s.OpenCursor()
	if err != nil {
		t.Fatalf("Cursor err=%s", err)
	}

	for i, v := range tests {
		r, err := c.Next()
		if err != nil || (r == nil && err == nil) {
			t.Fatalf("ursor.Next() mismatch, r=%+v, err=%s, index=%d", r, err, i)
		}

		if !bytes.Equal(r.Payload, v) {
			t.Fatalf("Payload mismatch, act=%+v, exp=%+v", r.Payload, v)
		}

		if r.Time.Before(startTime) || r.Time.After(endTime) {
			t.Fatalf("Record time out of range, act=%s, expected between %s - %s", r.Time, startTime, endTime)
		}
	}
}

func TestAppendRotate(t *testing.T) {
	dir, err := ioutil.TempDir("", "test-captain")
	if err != nil {
		t.Fatal(err)
	}

	defer os.RemoveAll(dir)

	// 71 bytes is the size of magic header + 3 single char records.
	s := NewStream(dir, testMagicHeader)
	options := &AppendOptions{SegmentSize: 71}
	a, err := s.OpenAppender(options)
	if err != nil {
		t.Fatalf("Open Appender err=%s", err)
	}

	oneSeg := []*segmentInfo{&segmentInfo{name: filepath.Clean(dir + "/000000001.log"), seq: 1}}
	twoSeg := append(oneSeg, &segmentInfo{name: filepath.Clean(dir + "/000000002.log"), seq: 2})
	tests := []struct {
		payload []byte
		expSize int
		expSegs []*segmentInfo
	}{
		{payload: []byte("1"), expSize: 29, expSegs: oneSeg},
		{payload: []byte("2"), expSize: 50, expSegs: oneSeg},
		{payload: []byte("3"), expSize: 71, expSegs: oneSeg},
		{payload: []byte("4"), expSize: 29, expSegs: twoSeg},
	}

	for _, tt := range tests {
		err := a.Append(tt.payload)
		if err != nil {
			t.Fatalf("Append err=%s", err)
		}

		segs := scanSegments(dir)
		if len(segs) != len(tt.expSegs) {
			t.Fatalf("Segment length mismatch, act=%d, exp=%d", len(segs), len(tt.expSegs))
		}

		for i, s := range tt.expSegs {
			if *segs[i] != *s {
				t.Fatalf("Segment mismatch, act=%+v, exp=%+v", segs[i], s)
			}
		}

		var stat os.FileInfo
		if len(tt.expSegs) == len(oneSeg) {
			stat, err = os.Stat(oneSeg[0].name)
		} else if len(tt.expSegs) == len(twoSeg) {
			stat, err = os.Stat(twoSeg[1].name)
		}

		if err != nil {
			t.Fatalf("Unable to stat, err=%s", err)
		}

		if stat.Size() != int64(tt.expSize) {
			t.Fatalf("Size mismatch, act=%d, exp=%d", stat.Size(), tt.expSize)
		}
	}
}

func TestAppendInvalidDir(t *testing.T) {
	dir, err := ioutil.TempDir("", "test-captain")
	if err != nil {
		t.Fatal(err)
	}

	defer os.RemoveAll(dir)

	s := NewStream(dir, testMagicHeader)
	a, err := s.OpenAppender(nil)
	if err != nil {
		t.Fatalf("Open Appender err=%s", err)
	}

	// Set dir to invalid.
	a.path = dir + "/does-not-exist"

	err = a.Append([]byte("a"))
	if err == nil {
		t.Fatalf("Expected err on invalid dir")
	}
}

// Test failure handling for an unlikely record marshaling error.
func TestAppendRecordMarshalFailure(t *testing.T) {
	testErr := errors.New("invalid file descriptor")
	copy := binaryWrite
	expData := []byte("a")
	binaryWrite = func(w io.Writer, order binary.ByteOrder, data interface{}) error {
		b, ok := data.([]byte)
		if ok && bytes.Equal(expData, b) {
			return testErr
		}

		return copy(w, order, data)
	}
	defer func() { binaryWrite = copy }()

	dir, err := ioutil.TempDir("", "test-captain")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)

	s := NewStream(dir, testMagicHeader)
	a, err := s.OpenAppender(nil)
	if err != nil {
		t.Fatalf("Open Appender err=%s", err)
	}

	err = a.Append(expData)
	if err != testErr {
		t.Fatalf("Append err, act=%s, exp=%s", err, testErr)
	}
}

// Test internal writer failure handling.
// e.g., Disk full, closed fd.
func TestAppendWriteFailure(t *testing.T) {
	expData := []byte("deadbeef")
	testErr := errors.New("invalid file descriptor")
	copy := binaryWrite
	binaryWrite = func(w io.Writer, order binary.ByteOrder, data interface{}) error {
		b, ok := data.([]byte)
		// Look for expData within marshaled record.
		if ok && len(b) == 28 && bytes.Equal(expData, b[16:24]) {
			return testErr
		}

		return copy(w, order, data)
	}
	defer func() { binaryWrite = copy }()

	dir, err := ioutil.TempDir("", "test-captain")
	if err != nil {
		t.Fatal(err)
	}

	defer os.RemoveAll(dir)

	s := NewStream(dir, testMagicHeader)
	a, err := s.OpenAppender(nil)
	if err != nil {
		t.Fatalf("Open Appender err=%s", err)
	}

	err = a.Append(expData)
	if err != testErr {
		t.Fatalf("Append err, act=%s, exp=%s", err, testErr)
	}
}

func TestAppendEmptyDir(t *testing.T) {
	dir, err := ioutil.TempDir("", "test-captain")
	if err != nil {
		t.Fatal(err)
	}

	defer os.RemoveAll(dir)

	segPath := dir + "/000000001.log"
	_, err = os.Stat(segPath)
	if os.IsExist(err) {
		t.Fatalf("Expected empty dir")
	}

	s := NewStream(dir, testMagicHeader)
	a, err := s.OpenAppender(nil)
	if err != nil {
		t.Fatalf("New Appender err=%s", err)
	}
	a.Append([]byte("a"))

	f, err := os.Open(segPath)
	if err != nil {
		t.Fatalf("Expected file path to exist")
	}

	if err = validateSegmentHeader(f, testMagicHeader); err != nil {
		t.Fatalf("Expected valid segment header, err=%s", err)
	}
}

func TestAppendInvalidHeader(t *testing.T) {
	s := NewStream("./test/invalid-header", testMagicHeader)
	a, err := s.OpenAppender(nil)
	if err != nil {
		t.Fatalf("New Appender err=%s", err)
	}
	err = a.Append([]byte("a"))
	if err == nil {
		t.Fatalf("Append err, act=nil, exp=err")
	}
}

func TestAppenderNewWithInvalidDir(t *testing.T) {
	s := NewStream("./test/does-not-exist", testMagicHeader)
	_, err := s.OpenAppender(nil)
	if err == nil {
		t.Fatalf("Expected not found err")
	}
}

// Opening a new appender on a directory with the last segment file already at
// the SegmentSize limit, should rotate it immediately.
func TestAppenderLastActiveFileAtLimit(t *testing.T) {
	dir := "./test/appender-rotate-limit"
	expFile := dir + "/000000003.log"
	defer os.Remove(expFile)

	// 71 bytes is the size of magic header + 3 single char records.
	s := NewStream(dir, testMagicHeader)
	options := &AppendOptions{SegmentSize: 71}
	a, err := s.OpenAppender(options)
	if err != nil {
		t.Fatalf("New appender err=%s", err)
	}

	f, err := a.activeSegment()
	if err != nil {
		t.Fatalf("Unexpected Appender.activeFile() err=%s", err)
	}
	if f.Name() != expFile {
		t.Fatalf("Rotated file mismatch, act=%s, exp=%s", f.Name(), expFile)
	}
}

func TestAppendProcessLock(t *testing.T) {
	dir, err := ioutil.TempDir("", "test-captain")
	if err != nil {
		t.Fatal(err)
	}

	defer os.RemoveAll(dir)

	s := NewStream(dir, testMagicHeader)
	a1, err := s.OpenAppender(nil)
	if err != nil {
		t.Fatalf("Open Appender err=%s", err)
	}

	a2, err := s.OpenAppender(nil)
	if err != nil {
		t.Fatalf("Open Appender err=%s", err)
	}

	if err := a1.Lock(); err != nil {
		t.Fatalf("Appender Lock err=%s", err)
	}
	defer a1.Unlock()

	done := make(chan struct{})
	go func() {
		if err := a2.Lock(); err != nil {
			t.Fatalf("Appender Lock err=%s", err)
		}
		defer a2.Unlock()

		close(done)
	}()

	timer := time.NewTimer(100 * time.Millisecond)
	select {
	case <-done:
		t.Fatalf("Unexpected second append lock")
	case <-timer.C:
	}

	a1.Unlock()
	timer = time.NewTimer(100 * time.Millisecond)
	select {
	case <-done:
	case <-timer.C:
		t.Fatalf("Expected successful a2 lock")
	}
}

func TestAppenderActiveFileExistingSegments(t *testing.T) {
	dir := "./test/appender-existing-segments"
	s := NewStream(dir, testMagicHeader)
	a, err := s.OpenAppender(nil)
	if err != nil {
		t.Fatalf("Open Appender err=%s", err)
	}

	f, err := a.activeSegment()
	if err != nil {
		t.Fatalf("Active segment err=%s", err)
	}

	expFile := dir + "/000000002.log"
	if f.Name() != expFile {
		t.Fatalf("Active file mismatch, act=%s, exp=%s", f.Name(), expFile)
	}
}

type testSegmentWriter struct {
	sync  func() error
	write func(b []byte) (int, error)
}

func (w *testSegmentWriter) Sync() error {
	return w.sync()
}

func (w *testSegmentWriter) Write(b []byte) (int, error) {
	return w.write(b)
}

func (w *testSegmentWriter) Close() error {
	return nil
}

func TestAppenderSyncInterval(t *testing.T) {
	dir, err := ioutil.TempDir("", "test-captain")
	if err != nil {
		t.Fatal(err)
	}

	defer os.RemoveAll(dir)

	s := NewStream(dir, testMagicHeader)
	a, err := s.OpenAppender(&AppendOptions{SyncPolicy: SyncInterval, SyncInterval: 10})
	if err != nil {
		t.Fatalf("Open Appender err=%s", err)
	}

	// Ensure sync is proper when there is no segment file to sync.
	// This will show up in coverage report.
	time.Sleep(15 * time.Millisecond)

	var n uint32
	w := &testSegmentWriter{
		sync: func() error {
			atomic.AddUint32(&n, 1)
			return nil
		},
	}
	a.rwlock.Lock()
	a.seg = &segmentWriter{writer: w}
	a.rwlock.Unlock()

	time.Sleep(40 * time.Millisecond)
	actN := atomic.LoadUint32(&n)
	if actN < 3 || actN > 5 {
		t.Fatalf("Sync count act=%d, exp=3 - 5", actN)
	}
}

func TestAppenderSyncAlways(t *testing.T) {
	dir, err := ioutil.TempDir("", "test-captain")
	if err != nil {
		t.Fatal(err)
	}

	defer os.RemoveAll(dir)

	s := NewStream(dir, testMagicHeader)
	a, err := s.OpenAppender(&AppendOptions{SyncPolicy: SyncAlways})
	if err != nil {
		t.Fatalf("Open Appender err=%s", err)
	}

	var n int
	w := &testSegmentWriter{
		sync: func() error {
			n++
			return nil
		},
		write: func(b []byte) (int, error) {
			return len(b), nil
		},
	}
	a.rwlock.Lock()
	a.seg = &segmentWriter{writer: w}
	a.rwlock.Unlock()

	err = a.Append([]byte("a"))
	if err != nil {
		t.Fatalf("Append err=%s", err)
	}

	if n != 1 {
		t.Fatalf("Sync count act=%d, exp=1", n)
	}
}
