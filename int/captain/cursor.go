package captain

import (
	"errors"
	"sync"
)

var (
	errEndOfStream = errors.New("end of stream")
	readLockFile   = "read.lock"
)

// Cursor represents an iterator that traverses Records in log time order.
// Cursors can only move forward with the ability to Reset back to the front.
type Cursor struct {
	path     string
	header   *MagicHeader
	plock    fileLocker     // Process read locker (advisory).
	rwlock   sync.RWMutex   // Mutex for segment rotation.
	segments []*segmentInfo // Segment file paths.
	segIdx   int            // Current segment sequence.
	rotate   bool           // Flag to rotate on next record invocation.
	current  *segmentCursor // Current segment reader.
}

// OpenCursor returns a cursor for the current stream.
func (s *Stream) OpenCursor() (*Cursor, error) {
	plock, err := openFileMutex(s.path + "/" + readLockFile)
	if err != nil {
		return nil, err
	}

	return &Cursor{
		path:   s.path,
		header: s.header,
		plock:  plock,
	}, nil
}

// Lock (read) across processes.
// Ensures a consistent view of the data.
func (c *Cursor) Lock() error {
	return c.plock.RLock()
}

// Unlock (read) across processes.
func (c *Cursor) Unlock() error {
	return c.plock.RUnlock()
}

// Segment returns the full filename/path to the segment file.
func (c *Cursor) Segment() string {
	if c.segIdx > len(c.segments)-1 {
		return ""
	}

	return c.segments[c.segIdx].name
}

// Next returns the next Record in the cursor.
// Handles rotating to the next segment file.
// A record of nil, with an error of nil represents the end of the cursor.
// Requires a read lock through Lock().
func (c *Cursor) Next() (*Record, error) {
	c.rwlock.Lock()
	defer c.rwlock.Unlock()

	return c.nextRecord()
}

// Main implementation of nextRecord.
// This indirection allows for nextRecord to be called recursively
// without locking (handled in Next() parent method).
func (c *Cursor) nextRecord() (*Record, error) {
	if c.current == nil && c.segments == nil {
		// Initialize cursor with segments.
		c.segments = scanSegments(c.path)
	}

	if c.current == nil || c.rotate {
		seg, err := c.nextSegment()
		if err == errEndOfStream {
			return nil, nil
		}

		if err != nil {
			return nil, err
		}

		if err := c.closeCurrent(); err != nil {
			return nil, err
		}

		c.current = seg
		c.rotate = false
	}

	r, err := c.current.Next()
	if err != nil {
		return nil, err
	}

	if r == nil && err == nil {
		c.rotate = true
		return c.nextRecord()
	}

	return r, nil
}

// Move cursor to next segment.
// Not to be executed directly, locks take place in Next().
func (c *Cursor) nextSegment() (*segmentCursor, error) {
	if c.current != nil {
		c.segIdx++
	}

	if c.segIdx > len(c.segments)-1 {
		return nil, errEndOfStream
	}

	return openSegmentCursor(c.segments[c.segIdx].name, c.header)
}

// Reset the cursor, rewinding back to the start.
func (c *Cursor) Reset() error {
	c.rwlock.Lock()
	defer c.rwlock.Unlock()

	if err := c.closeCurrent(); err != nil {
		return err
	}

	c.current = nil
	c.segIdx = 0
	c.segments = nil
	return nil
}

func (c *Cursor) closeCurrent() error {
	if c.current != nil {
		return c.current.Close()
	}

	return nil
}
