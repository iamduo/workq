package captain

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"log"
	"sync"
	"time"
)

// AppendOptions represents the options for opening an Appender.
type AppendOptions struct {
	// Minimum segment size before rolling into a new one.
	SegmentSize uint
	// Speficies which policy to use, one of the Sync* constants.
	SyncPolicy uint
	// Sync interval in ms, valid when sync policy is set to SyncInterval.
	SyncInterval uint
}

const (
	// SyncOS policy defers the sync to the OS.
	SyncOS = 1
	// SyncInterval policy syncs at an interval specfied in Options.SyncInterval.
	SyncInterval = 2
	// SyncAlways policy syncs on every Append.
	SyncAlways = 3
)

const (
	// DefaultSegmentSize 64 MiB
	DefaultSegmentSize = 67108864
	// DefaultSyncPolicy SyncInterval
	DefaultSyncPolicy = SyncInterval
	// DefaultSyncInterval 1000ms
	DefaultSyncInterval = 1000
)

const appendLockFile = "append.lock"

// Appender represents a stream writer in append-only mode.
// Appenders must be locked before use and unlocked when done as there can only
// be a single active appender at a time.
type Appender struct {
	path    string
	header  *MagicHeader
	options *AppendOptions
	seg     *segmentWriter // Current active segment writer.
	plock   fileLocker     // Advisory append process file lock.
	rwlock  sync.RWMutex   // Lock for safe concurrent appends.
}

// OpenAppender opens an Appender object on the stream.
// Nil options will set default options.
func (s *Stream) OpenAppender(options *AppendOptions) (*Appender, error) {
	if options == nil {
		options = &AppendOptions{}
	}

	if options.SegmentSize == 0 {
		options.SegmentSize = DefaultSegmentSize
	}

	if options.SyncPolicy == 0 && options.SyncInterval == 0 {
		options.SyncPolicy = DefaultSyncPolicy
		options.SyncInterval = DefaultSyncInterval
	}

	if options.SyncInterval != 0 && options.SyncPolicy != SyncInterval {
		return nil, errors.New("invalid sync interval policy")
	}

	mu, err := openFileMutex(s.path + "/" + appendLockFile)
	if err != nil {
		return nil, err
	}

	a := &Appender{
		path:    s.path,
		header:  s.header,
		options: options,
		plock:   mu,
	}

	if options.SyncPolicy == SyncInterval {
		go a.startIntervalSync(options.SyncInterval)
	}

	return a, nil
}

func (a *Appender) startIntervalSync(intvl uint) {
	ticker := time.NewTicker(time.Duration(intvl) * time.Millisecond)
	for {
		<-ticker.C
		a.rwlock.RLock()
		seg := a.seg
		a.rwlock.RUnlock()

		if seg == nil {
			continue
		}

		err := seg.Sync()
		if err != nil {
			log.Print(err)
		}
	}
}

// Lock appender
// Safe across multiple processes via an advisory file lock.
func (a *Appender) Lock() error {
	return a.plock.Lock()
}

// Unlock appender
func (a *Appender) Unlock() error {
	return a.plock.Unlock()
}

// Append payload to the log.
// Formats as a log Record.
func (a *Appender) Append(b []byte) error {
	a.rwlock.Lock()
	defer a.rwlock.Unlock()

	seg, err := a.activeSegment()
	if err != nil {
		return err
	}

	if err = writeRecord(seg, NewRecord(b)); err != nil {
		return err
	}

	return a.syncAlways(seg)
}

// Return the active append file handling rotation if neccessary.
func (a *Appender) activeSegment() (*segmentWriter, error) {
	// If no active segment exists, find the latest.
	if a.seg == nil {
		segs := scanSegments(a.path)
		var seq uint32
		if len(segs) > 0 {
			seq = segs[len(segs)-1].seq
		} else {
			seq = uint32(1)
		}

		seg, err := a.openSegment(seq)
		if err != nil {
			return nil, err
		}

		a.seg = seg
	}

	if a.seg.Size() >= int(a.options.SegmentSize) {
		seg, err := a.openSegment(a.seg.Sequence() + 1)
		if err != nil {
			return nil, err
		}

		if err := a.seg.Close(); err != nil {
			return nil, err
		}

		a.seg = seg

		// Recurse once more to ensure newly opened segment is within size.
		return a.activeSegment()
	}

	return a.seg, nil
}

// Open a segment by sequence as a writer.
// Rotates to the next segment if requested segment is already full.
func (a *Appender) openSegment(seq uint32) (*segmentWriter, error) {
	path := fmt.Sprintf("%s/%09d.log", a.path, seq)
	return openSegmentWriter(path, seq, a.header)
}

func (a *Appender) syncAlways(seg writeSyncCloser) error {
	if a.options.SyncPolicy == SyncAlways {
		return seg.Sync()
	}

	return nil
}

func writeRecord(w io.Writer, r *Record) error {
	bin, err := r.MarshalBinary()
	if err != nil {
		return err
	}

	if err = binaryWrite(w, binary.BigEndian, bin); err != nil {
		return err
	}

	return nil
}

// Proxy func to allow for internal error testing.
var binaryWrite = func(w io.Writer, order binary.ByteOrder, data interface{}) error {
	return binary.Write(w, order, data)
}
