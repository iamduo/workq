package captain

import (
	"bufio"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
)

var (
	errInvalidSegmentFileName = errors.New("invalid segment file name")
	errInvalidSegmentHeader   = errors.New("invalid segment header")
)

// segmentCursor represents a cursor for a single segment file.
type segmentCursor struct {
	name string
	rdr  RecordReader
	cls  io.Closer
}

// openSegmentCursor opens a cursor by file name and validates with the specified header.
func openSegmentCursor(name string, header *MagicHeader) (*segmentCursor, error) {
	f, err := os.Open(name)
	if err != nil {
		return nil, err
	}

	r := bufio.NewReader(f)
	if err = validateSegmentHeader(r, header); err != nil {
		return nil, fmt.Errorf("open segment cursor %s: %s", name, err)
	}

	return &segmentCursor{
		name: f.Name(),
		rdr:  r,
		cls:  f,
	}, nil
}

// Next returns the next Record in the cursor.
// A record of nil, with an error of nil represents the end of segment.
func (c *segmentCursor) Next() (*Record, error) {
	r := &Record{}
	err := r.UnmarshalBinaryFromReader(c.rdr)
	if err == io.EOF {
		return nil, nil
	}

	if err != nil {
		return nil, err
	}

	return r, nil
}

func (c *segmentCursor) Close() error {
	return c.cls.Close()
}

type writeSyncCloser interface {
	io.Writer
	io.Closer
	syncer
}

type syncer interface {
	Sync() error
}

// segmentWriter represents a segment file as an append-only writer and records the current
// size & sequence number of the segment.
// Writer indirection allows for testing / disconnects from os.File.
type segmentWriter struct {
	name   string
	writer writeSyncCloser
	size   int
	seq    uint32
}

// openSegmentWriter returns a segmentWriter, if the file is new, it will be created
// with the specified header.
func openSegmentWriter(name string, seq uint32, header *MagicHeader) (*segmentWriter, error) {
	f, err := openAppendSegmentFile(name)
	if err != nil {
		return nil, err
	}

	stat, err := f.Stat()
	if err != nil {
		return nil, err
	}

	w := &segmentWriter{
		name:   name,
		writer: f,
		size:   int(stat.Size()),
		seq:    seq,
	}

	if w.size == 0 {
		if err = writeSegmentHeader(w, header); err != nil {
			return nil, err
		}
	} else {
		if err = validateSegmentHeader(f, header); err != nil {
			return nil, fmt.Errorf("open segment writer %s: %s", name, err)
		}
	}

	return w, nil
}

// Name returns the name of the segment file (same as os.File.Name()).
func (w *segmentWriter) Name() string {
	return w.name
}

// Write implements the io.Writer interface.
// Records the current size of the segment.
// Not safe for concurrent use.
func (w *segmentWriter) Write(data []byte) (int, error) {
	n, err := w.writer.Write(data)
	w.size += n
	return n, err
}

// Sync implements the os.File.Sync.
func (w *segmentWriter) Sync() error {
	return w.writer.Sync()
}

// Size returns the current size.
func (w *segmentWriter) Size() int {
	return w.size
}

// Sequence returns the sequence of the segment.
func (w *segmentWriter) Sequence() uint32 {
	return w.seq
}

func (w *segmentWriter) Close() error {
	return w.writer.Close()
}

// segmentInfo describes a single segments meta.
// Used in scanSegments() to return a slice of segment listings.
type segmentInfo struct {
	name string
	seq  uint32
}

// scanSegments scans a directory for segment files and returns a slice of segmentInfo objects
// in sequence order.
func scanSegments(dir string) []*segmentInfo {
	var segments []*segmentInfo
	paths := make(map[int]string)
	matches, _ := filepath.Glob(dir + "/*.log")

	var order []int
	for _, v := range matches {
		seq, err := parseSequence(v)
		// Skip invalid files.
		if err != nil {
			continue
		}

		order = append(order, int(seq))
		paths[int(seq)] = v
	}

	sort.Ints(order)
	for _, v := range order {
		segments = append(segments, &segmentInfo{name: paths[v], seq: uint32(v)})
	}

	return segments
}

// openAppendSegmentFile opens a segment file with append mode.
func openAppendSegmentFile(name string) (*os.File, error) {
	f, err := os.OpenFile(name, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0666)
	if err != nil {
		return nil, err
	}

	return f, nil
}

// parseSequence returns the sequence number from a filename.
// e.g. /path/to/captain/000000001.log -> uint32(1)
func parseSequence(path string) (uint32, error) {
	split := strings.SplitN(filepath.Base(path), ".", 2)
	if len(split) != 2 || len(split[0]) != 9 || split[1] != "log" {
		return 0, errInvalidSegmentFileName
	}

	u, err := strconv.ParseUint(split[0], 10, 32)
	if err != nil {
		return 0, err
	}

	return uint32(u), nil
}

var headSize = 8

// MagicHeader represents the magic bytes and version of a segment file.
type MagicHeader struct {
	Magic   uint32
	Version uint32
}

// MarshalBinary returns the magic header in BigEndian.
func (h *MagicHeader) MarshalBinary() ([]byte, error) {
	buf := make([]byte, 8)
	binary.BigEndian.PutUint32(buf[0:4], h.Magic)
	binary.BigEndian.PutUint32(buf[4:8], h.Version)
	return buf, nil
}

// MarshalBinary sets the magic header from data in BigEndian.
func (h *MagicHeader) UnmarshalBinary(data []byte) error {
	if len(data) != headSize {
		return errors.New("invalid length")
	}

	h.Magic = binary.BigEndian.Uint32(data[0:4])
	h.Version = binary.BigEndian.Uint32(data[4:8])

	return nil
}

func writeSegmentHeader(w io.Writer, header *MagicHeader) error {
	// Header Marshal Binary err is always nil.
	// Checking err here causes a branch that will never be triggered.
	data, _ := header.MarshalBinary()
	_, err := w.Write(data)
	return err
}

func validateSegmentHeader(r io.Reader, header *MagicHeader) error {
	h := &MagicHeader{}
	buf := make([]byte, headSize)
	if _, err := io.ReadFull(r, buf); err != nil {
		return fmt.Errorf("%s: %s", errInvalidSegmentHeader, err)
	}

	h.UnmarshalBinary(buf)
	if h.Magic != header.Magic || h.Version != header.Version {
		return errInvalidSegmentHeader
	}

	return nil
}
