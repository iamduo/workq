package captain

import (
	"errors"
	"io/ioutil"
	"os"
	"testing"
	"time"
)

func TestCleanFullRemoval(t *testing.T) {
	dir, err := ioutil.TempDir("", "test-captain")
	if err != nil {
		t.Fatal(err)
	}

	defer os.RemoveAll(dir)

	// 29 is the size of a Record with a single char.
	s := NewStream(dir, testMagicHeader)
	app, err := s.OpenAppender(&AppendOptions{SegmentSize: 29})
	if err != nil {
		t.Fatalf("Open Appender err=%s", err)
	}
	app.Append([]byte("1"))
	app.Append([]byte("2"))
	app.Append([]byte("3"))

	cleaner, err := s.OpenCleaner()
	if err != nil {
		t.Fatalf("Open Cleaner err=%s", err)
	}

	fn := func(path string, r *Record) (bool, error) {
		return true, nil
	}
	err = cleaner.Clean(fn)
	if err != nil {
		t.Fatalf("Clean err=%s", err)
	}

	segs := scanSegments(dir)
	if len(segs) != 1 {
		t.Fatalf("Scanned segs len, act=%d, exp=1", len(segs))
	}

	if segs[0].seq != 3 {
		t.Fatalf("Scanned segs act.len=%d, act.seq=%d, exp.len=1, exp.seq=3", len(segs), segs[0].seq)
	}
}

func TestCleanPartialRewrite(t *testing.T) {
	dir, err := ioutil.TempDir("", "test-captain")
	if err != nil {
		t.Fatal(err)
	}

	defer os.RemoveAll(dir)

	// 92 is the size of a 4 Records with a single byte payload.
	s := NewStream(dir, testMagicHeader)
	app, err := s.OpenAppender(&AppendOptions{SegmentSize: 92})
	if err != nil {
		t.Fatal(err)
	}

	for i := 1; i <= 16; i++ {
		app.Append([]byte{uint8(i)})
	}

	cleaner, err := s.OpenCleaner()
	if err != nil {
		t.Fatal(err)
	}

	fn := func(path string, r *Record) (bool, error) {
		// Remove all odds.
		return r.Payload[0]%2 == 1, nil
	}
	err = cleaner.Clean(fn)
	if err != nil {
		t.Fatal(err)
	}

	segs := scanSegments(dir)
	if len(segs) != 4 {
		t.Fatalf("Scanned segs act=%d, exp=4", len(segs))
	}

	var actInts []uint8
	expInts := []uint8{2, 4, 6, 8, 10, 12, 13, 14, 15, 16}
	for _, s := range segs {
		cur, err := openSegmentCursor(s.name, testMagicHeader)
		if err != nil {
			t.Fatalf("Open segment cursor err=%s", err)
		}

		for {
			rec, err := cur.Next()
			if err != nil {
				t.Fatalf("Cursor next err=%s", err)
			}

			if rec == nil {
				break
			}

			actInts = append(actInts, uint8(rec.Payload[0]))
		}
	}

	if len(actInts) != len(expInts) {
		t.Fatalf("Data len act=%d, exp=%d", len(actInts), len(expInts))
	}

	for i, v := range expInts {
		if actInts[i] != v {
			t.Fatalf("Data compare act=%d, exp=%d", actInts[i], v)
		}
	}
}

func TestCleanSkipSegment(t *testing.T) {
	dir, err := ioutil.TempDir("", "test-captain")
	if err != nil {
		t.Fatal(err)
	}

	defer os.RemoveAll(dir)

	// 29 is the size of a Record with a single char.
	s := NewStream(dir, testMagicHeader)
	app, err := s.OpenAppender(&AppendOptions{SegmentSize: 29})
	if err != nil {
		t.Fatalf("Open Appender err=%s", err)
	}
	app.Append([]byte("1"))
	app.Append([]byte("2"))
	app.Append([]byte("3"))

	var expTimes []time.Time
	for _, s := range scanSegments(dir) {
		info, _ := os.Stat(s.name)
		expTimes = append(expTimes, info.ModTime())
	}

	cleaner, err := s.OpenCleaner()
	if err != nil {
		t.Fatalf("Open Cleaner err=%s", err)
	}

	fn := func(path string, r *Record) (bool, error) {
		return false, ErrSkipSegment
	}
	err = cleaner.Clean(fn)
	if err != nil {
		t.Fatalf("Clean err=%s", err)
	}

	// Expecting nothing to be touched.
	var actTimes []time.Time
	for _, s := range scanSegments(dir) {
		info, _ := os.Stat(s.name)
		actTimes = append(actTimes, info.ModTime())
	}

	if len(actTimes) != len(expTimes) {
		t.Fatalf("Mod times act=%d, exp=%d", len(actTimes), len(expTimes))
	}

	for i := range expTimes {
		if !actTimes[i].Equal(expTimes[i]) {
			t.Fatalf("Mod times act=%d, exp=%d", actTimes[i], expTimes[i])
		}
	}
}

type testCleanerFS struct {
	fnOpenSegmentRewriter func(name string, seq uint32, header *MagicHeader) (*segmentWriter, error)
	fnOpenSegmentCursor   func(name string, header *MagicHeader) (recordCursor, error)
	fnRemove              func(name string) error
	fnRename              func(old, new string) error
}

func (fs *testCleanerFS) openSegmentRewriter(name string, seq uint32, header *MagicHeader) (*segmentWriter, error) {
	return fs.fnOpenSegmentRewriter(name, seq, header)
}

func (fs *testCleanerFS) openSegmentCursor(name string, header *MagicHeader) (recordCursor, error) {
	return fs.fnOpenSegmentCursor(name, header)
}

func (fs *testCleanerFS) remove(name string) error {
	return fs.fnRemove(name)
}

func (fs *testCleanerFS) rename(old, new string) error {
	return fs.fnRename(old, new)
}

type testFailingCursor struct {
	err error
}

func (c *testFailingCursor) Next() (*Record, error) {
	return nil, c.err
}

func (c *testFailingCursor) Close() error {
	return nil
}

var testNilSegmentWriter = &testSegmentWriter{
	write: func(b []byte) (int, error) {
		return 0, nil
	},
	sync: func() error {
		return nil
	},
}

func TestCleanErrors(t *testing.T) {
	dir, err := ioutil.TempDir("", "test-captain")
	if err != nil {
		t.Fatal(err)
	}

	defer os.RemoveAll(dir)

	// 29 is the size of a Record with a single char.
	s := NewStream(dir, testMagicHeader)
	app, err := s.OpenAppender(&AppendOptions{SegmentSize: 29})
	if err != nil {
		t.Fatalf("Open Appender err=%s", err)
	}
	app.Append([]byte("1"))
	app.Append([]byte("2"))
	app.Append([]byte("3"))

	nilCleanFn := func(path string, r *Record) (bool, error) {
		return false, nil
	}
	expErr := errors.New("test")
	defaultFS := &cleanerFS{}
	tests := []struct {
		fs *testCleanerFS
		fn CleanFn
	}{
		{
			&testCleanerFS{
				fnOpenSegmentRewriter: func(name string, seq uint32, header *MagicHeader) (*segmentWriter, error) {
					return nil, expErr
				},
				fnRemove: func(name string) error {
					return nil
				},
			},
			nilCleanFn,
		},
		{
			&testCleanerFS{
				fnOpenSegmentRewriter: func(name string, seq uint32, header *MagicHeader) (*segmentWriter, error) {
					return &segmentWriter{writer: testNilSegmentWriter}, nil
				},
				fnOpenSegmentCursor: func(name string, header *MagicHeader) (recordCursor, error) {
					return nil, expErr
				},
				fnRemove: func(name string) error {
					return nil
				},
			},
			nilCleanFn,
		},
		{
			&testCleanerFS{
				fnOpenSegmentRewriter: func(name string, seq uint32, header *MagicHeader) (*segmentWriter, error) {
					return &segmentWriter{writer: testNilSegmentWriter}, nil
				},
				fnOpenSegmentCursor: func(name string, header *MagicHeader) (recordCursor, error) {
					return &testFailingCursor{expErr}, nil
				},
				fnRemove: func(name string) error {
					return nil
				},
			},
			nilCleanFn,
		},
		{
			&testCleanerFS{
				fnOpenSegmentRewriter: func(name string, seq uint32, header *MagicHeader) (*segmentWriter, error) {
					w := func(b []byte) (int, error) {
						return 0, expErr
					}
					return &segmentWriter{writer: &testSegmentWriter{write: w}}, nil
				},
				fnOpenSegmentCursor: func(name string, header *MagicHeader) (recordCursor, error) {
					return defaultFS.openSegmentCursor(name, header)
				},
				fnRemove: func(name string) error {
					return nil
				},
			},
			nilCleanFn,
		},
		// Look for the record write, very brittle, but works for now.
		{
			&testCleanerFS{
				fnOpenSegmentRewriter: func(name string, seq uint32, header *MagicHeader) (*segmentWriter, error) {
					i := 0
					w := func(b []byte) (int, error) {
						i++
						if i == 1 {
							return 0, expErr
						}

						return 1, nil
					}

					return &segmentWriter{writer: &testSegmentWriter{write: w}}, nil
				},
				fnOpenSegmentCursor: func(name string, header *MagicHeader) (recordCursor, error) {
					return defaultFS.openSegmentCursor(name, header)
				},
				fnRemove: func(name string) error {
					return nil
				},
			},
			nilCleanFn,
		},
		{
			&testCleanerFS{
				fnOpenSegmentRewriter: func(name string, seq uint32, header *MagicHeader) (*segmentWriter, error) {
					wrt := func(b []byte) (int, error) {
						return 1, nil
					}
					sync := func() error {
						return expErr
					}

					return &segmentWriter{writer: &testSegmentWriter{write: wrt, sync: sync}}, nil
				},
				fnOpenSegmentCursor: func(name string, header *MagicHeader) (recordCursor, error) {
					return defaultFS.openSegmentCursor(name, header)
				},
				fnRemove: func(name string) error {
					return nil
				},
			},
			nilCleanFn,
		},
		{
			&testCleanerFS{
				fnOpenSegmentRewriter: func(name string, seq uint32, header *MagicHeader) (*segmentWriter, error) {
					wrt := func(b []byte) (int, error) {
						return 1, nil
					}
					sync := func() error {
						return nil
					}

					return &segmentWriter{writer: &testSegmentWriter{write: wrt, sync: sync}}, nil
				},
				fnOpenSegmentCursor: func(name string, header *MagicHeader) (recordCursor, error) {
					return defaultFS.openSegmentCursor(name, header)
				},
				fnRename: func(old string, new string) error {
					return expErr
				},
				fnRemove: func(name string) error {
					return nil
				},
			},
			nilCleanFn,
		},
		// Configure the cleanFN to always remove.
		// Trigger remove to fail.
		{
			&testCleanerFS{
				fnOpenSegmentRewriter: func(name string, seq uint32, header *MagicHeader) (*segmentWriter, error) {
					wrt := func(b []byte) (int, error) {
						return 1, nil
					}
					sync := func() error {
						return nil
					}

					return &segmentWriter{writer: &testSegmentWriter{write: wrt, sync: sync}}, nil
				},
				fnOpenSegmentCursor: func(name string, header *MagicHeader) (recordCursor, error) {
					return defaultFS.openSegmentCursor(name, header)
				},
				fnRemove: func(name string) error {
					return expErr
				},
			},
			func(path string, r *Record) (bool, error) {
				return true, nil
			},
		},
		// CleanFn error
		{
			&testCleanerFS{
				fnOpenSegmentRewriter: func(name string, seq uint32, header *MagicHeader) (*segmentWriter, error) {
					wrt := func(b []byte) (int, error) {
						return 1, nil
					}
					sync := func() error {
						return nil
					}

					return &segmentWriter{writer: &testSegmentWriter{write: wrt, sync: sync}}, nil
				},
				fnOpenSegmentCursor: func(name string, header *MagicHeader) (recordCursor, error) {
					return defaultFS.openSegmentCursor(name, header)
				},
				fnRemove: func(name string) error {
					return nil
				},
			},
			func(path string, r *Record) (bool, error) {
				return false, expErr
			},
		},
	}

	for _, tt := range tests {
		cleaner, err := s.OpenCleaner()
		if err != nil {
			t.Fatalf("Open Cleaner err=%s", err)
		}

		cleaner.fs = tt.fs
		err = cleaner.Clean(tt.fn)

		if err != expErr {
			t.Errorf("Clean err, act=%s, exp=%s", err, expErr)
		}
	}
}

func TestOpenCleanerErrors(t *testing.T) {
	var tests []string
	dir, err := ioutil.TempDir("", "test-captain")
	if err != nil {
		t.Fatal(err)
	}

	defer os.RemoveAll(dir)

	// Can't create locks within this dir.
	os.Chmod(dir, 0444)
	tests = append(tests, dir)
	tests = append(tests, "this-path-does-not-exist")
	for _, tt := range tests {
		s := NewStream(tt, testMagicHeader)
		_, err := s.OpenCleaner()
		if err == nil {
			t.Fatalf("Expected err")
		}
	}
}

func TestCleanerProcessLock(t *testing.T) {
	dir, err := ioutil.TempDir("", "test-captain")
	if err != nil {
		t.Fatal(err)
	}

	defer os.RemoveAll(dir)

	s := NewStream(dir, testMagicHeader)
	c1, err := s.OpenCleaner()
	if err != nil {
		t.Fatalf("Open Cleaner err=%s", err)
	}

	c2, err := s.OpenCleaner()
	if err != nil {
		t.Fatalf("Open Cleaner err=%s", err)
	}

	if err := c1.Lock(); err != nil {
		t.Fatalf("Cleaner Lock err=%s", err)
	}
	defer c1.Unlock()

	done := make(chan struct{})
	go func() {
		if err := c2.Lock(); err != nil {
			t.Fatalf("Cursor Lock err=%s", err)
		}
		defer c2.Unlock()

		close(done)
	}()

	timer := time.NewTimer(100 * time.Millisecond)
	select {
	case <-done:
		t.Fatalf("Unexpected second cursor lock")
	case <-timer.C:
	}

	c1.Unlock()
	timer = time.NewTimer(100 * time.Millisecond)
	select {
	case <-done:
	case <-timer.C:
		t.Fatalf("Expected successful cursor #2 lock")
	}
}

// Test to ensure opening an existing rewrite file with a failure to remove
// the existing file fails.
func TestOpenSegmentRewriterInitialRemoveFailure(t *testing.T) {
	dir, err := ioutil.TempDir("", "test-captain")
	if err != nil {
		t.Fatal(err)
	}

	defer os.RemoveAll(dir)

	f, err := os.Create(dir + "/000000001.log.rw")
	if err != nil {
		t.Fatal(err)
	}

	// Change the dir to be unmodifiable.
	os.Chmod(dir, 0444)
	defer os.Chmod(dir, 0755)

	fs := &cleanerFS{}
	_, err = fs.openSegmentRewriter(f.Name(), 1, testMagicHeader)
	if err == nil {
		t.Fatalf("Open Segment Rewriter err, act=nil, exp err")
	}
}
