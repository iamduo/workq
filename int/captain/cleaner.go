package captain

import (
	"errors"
	"os"
)

// ErrSkipSegment signals the cleaner to stop & skip over the current segment.
var ErrSkipSegment = errors.New("skip this segment")

// Cleaner is responsible for cleaning the read-only portion of the stream.
// Policy for cleaning is based on the CleanFn passed to the Clean method.
type Cleaner struct {
	path   string
	header *MagicHeader
	plock  fileLocker // Cleaning process file lock (advisory).
	dir    syncer     // Used to sync the directory after renames for durability.
	fs     cleanerVFS // Indirection for edge case error testing.
}

// OpenCleaner returns a Cleaner on the stream.
func (s *Stream) OpenCleaner() (*Cleaner, error) {
	dir, err := os.Open(s.path)
	if err != nil {
		return nil, err
	}

	plock, err := openFileMutex(s.path + "/" + readLockFile)
	if err != nil {
		return nil, err
	}

	return &Cleaner{
		path:   s.path,
		header: s.header,
		plock:  plock,
		dir:    dir,
		fs:     &cleanerFS{},
	}, nil
}

// Lock Cleaner across processes (advisory).
func (c *Cleaner) Lock() error {
	return c.plock.Lock()
}

// Unlock Cleaner across processes (advisory).
func (c *Cleaner) Unlock() error {
	return c.plock.Unlock()
}

// Clean takes a CleanFn, iterates through every read-only segment file and
// invokes CleanFn against every log record:
// * Deleting the record when CleanFn returns true.
// * Retaining the record when CleanFn returns false.
// * Skipping the segment file if ErrSkipSegment is returned.
// * Stopping the cleaning if CleanFn returns any other error.
//
// Cleaning works by rewriting the segment file with only the relevant log records.
// Requires a cleaning lock via Lock() which is an exclusive lock, blocking cursors.
func (c *Cleaner) Clean(fn CleanFn) error {
	// No cursor lock needed here, Cleaner has the exlusive lock.
	segs := scanSegments(c.path)

	// Skip the last seg which is the active append seg.
	if len(segs) > 0 {
		segs = segs[0 : len(segs)-1]
	}

SegLoop:
	// Skip last seg which is the active append seg.
	for i := 0; i < len(segs); i++ {
		name := segs[i].name
		seq := segs[i].seq

		// Open "rewrite" version of the seg.
		rwPath := segs[i].name + ".rw"
		defer c.fs.remove(rwPath)

		w, err := c.fs.openSegmentRewriter(rwPath, seq, c.header)
		if err != nil {
			return err
		}

		cur, err := c.fs.openSegmentCursor(name, c.header)
		if err != nil {
			return err
		}

		var total int
		var cleaned int
		var r *Record
		for {
			r, err = cur.Next()
			if err != nil {
				return err
			}

			// Graceful end of cursor.
			if r == nil {
				break
			}

			total++
			ok, err := fn(name, r)
			if err != nil {
				if err == ErrSkipSegment {
					cur.Close()
					w.Close()
					continue SegLoop
				}

				return err
			}

			if ok {
				// Ok to delete, skip the rewrite of this rec below.
				cleaned++
				continue
			}

			if err = writeRecord(w, r); err != nil {
				return err
			}
		}

		// Delete the entire file if all records have been cleaned.
		if total > 0 && total == cleaned {
			if err = c.fs.remove(name); err != nil {
				return err
			}

			c.fs.remove(rwPath)
		} else if w.Size() > 0 {
			if err = w.Sync(); err != nil {
				return err
			}

			// Atomically replace the segment file with the rewrite version.
			if err = c.fs.rename(rwPath, name); err != nil {
				return err
			}

			if err = c.dir.Sync(); err != nil {
				return err
			}
		}

		cur.Close()
		w.Close()
	}

	return nil
}

// CleanFn represents the function signature that is passed to the Clean function.
// Returning true signals the cleaner to remove the record.
// Returning false signals to retain the record.
type CleanFn func(path string, r *Record) (bool, error)

type fileLocker interface {
	RLock() error
	RUnlock() error
	Lock() error
	Unlock() error
}

// cleanVFS represents a pseduo file access layer. Indirection allows for edge
// case testing.
type cleanerVFS interface {
	openSegmentRewriter(name string, seq uint32, header *MagicHeader) (*segmentWriter, error)
	openSegmentCursor(name string, header *MagicHeader) (recordCursor, error)
	remove(name string) error
	rename(old, new string) error
}

// cleanFS implements cleanVFS and is the default implementation.
type cleanerFS struct{}

func (fs *cleanerFS) openSegmentRewriter(name string, seq uint32, header *MagicHeader) (*segmentWriter, error) {
	// Speculative remove, only allow for a "not exist" error.
	// Any other error could mean the old file is hanging around.
	err := os.Remove(name)
	if err != nil && !os.IsNotExist(err) {
		return nil, err
	}

	return openSegmentWriter(name, seq, header)
}

func (fs *cleanerFS) openSegmentCursor(name string, header *MagicHeader) (recordCursor, error) {
	return openSegmentCursor(name, header)
}

func (fs *cleanerFS) remove(name string) error {
	return os.Remove(name)
}

func (fs *cleanerFS) rename(old, new string) error {
	return os.Rename(old, new)
}

type recordCursor interface {
	Next() (*Record, error)
	Close() error
}
