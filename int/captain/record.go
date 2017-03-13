package captain

import (
	"bytes"
	"encoding/binary"
	"errors"
	"hash/crc32"
	"io"
	"time"
)

// ErrCRCMismatch signifies the CRC included in the Record did not match
// the actual CRC reconstructed from the message.
var ErrCRCMismatch = errors.New("crc mismatch")

var crcTable = crc32.MakeTable(crc32.Castagnoli)

// Record represents a log entry.
type Record struct {
	Time    time.Time
	Payload []byte
}

// NewRecord returns a new record with the current time set to UTC.
func NewRecord(p []byte) *Record {
	return &Record{
		Time:    time.Now().UTC(),
		Payload: p,
	}
}

// MarshalBinary encodes a Record in its binary form.
// Appends a CRC32 to the end of the message.
func (r *Record) MarshalBinary() ([]byte, error) {
	var err error
	buf := new(bytes.Buffer)

	t, err := r.Time.MarshalBinary()
	if err != nil {
		return nil, err
	}

	vint := make([]byte, binary.MaxVarintLen32)
	n := binary.PutUvarint(vint, uint64(len(r.Payload)))
	data := []interface{}{
		t,
		vint[:n],
		r.Payload,
	}

	for _, v := range data {
		if err = binaryWrite(buf, binary.BigEndian, v); err != nil {
			return nil, err
		}
	}

	crc := crc32.Checksum(buf.Bytes(), crcTable)
	if err = binaryWrite(buf, binary.BigEndian, crc); err != nil {
		return nil, err
	}

	return buf.Bytes(), nil
}

// RecordReader is used for UnmarshalBinaryFromReader.
type RecordReader interface {
	io.Reader
	io.ByteReader
}

// teeByteReader is ByteReader that writes to w what it reads from r.
// Used to capture original Varint in UnmarshalBinaryFromReader for CRC reconstruction.
type teeByteReader struct {
	r io.ByteReader
	w io.Writer
}

// ReadByte implements io.ByteReader .
func (r *teeByteReader) ReadByte() (byte, error) {
	b, err := r.r.ReadByte()
	if err == nil {
		r.w.Write([]byte{b})
	}

	return b, err
}

// UnmarshalBinaryFromReader decodes a Record from a RecordReader.
// Returns an error if the data could not be completely decoded.
// Returns ErrCRCMismatch if the checksum did not match the original appended
// in the Record.
func (r *Record) UnmarshalBinaryFromReader(rdr RecordReader) error {
	var err error
	buf := make([]byte, 15)
	if err = binary.Read(rdr, binary.BigEndian, buf); err != nil {
		// An io.EOF error is expected here if the reader is empty.
		// This signals that there are no more records remaining safely.
		// io.EOF for every other read further down must be translated into an
		// an "unexpected eof" as this means we read a partial record.
		return err
	}

	t := time.Time{}
	err = t.UnmarshalBinary(buf[0:15])
	if err != nil {
		return r.normalizeError(err)
	}

	vintBuf := new(bytes.Buffer)
	teeRdr := &teeByteReader{rdr, vintBuf}
	size, err := binary.ReadUvarint(teeRdr)
	if err != nil {
		return r.normalizeError(err)
	}

	buf = append(buf, vintBuf.Bytes()...)
	pay := make([]byte, size)
	if err = binary.Read(rdr, binary.BigEndian, &pay); err != nil {
		return r.normalizeError(err)
	}

	buf = append(buf, pay...)
	var crc uint32
	if err = binary.Read(rdr, binary.BigEndian, &crc); err != nil {
		return r.normalizeError(err)
	}

	if crc != crc32.Checksum(buf, crcTable) {
		return ErrCRCMismatch
	}

	r.Time = t
	r.Payload = pay
	return nil
}

// normalizeError wraps an io.EOF to be a local ErrUnexpectedEOR.
// This is used to wrap read calls from UnmarshalBinaryFromReader that are
// NOT the very first read call. An io.EOF after the first call means we have a
// partial record.
func (r *Record) normalizeError(err error) error {
	if err == io.EOF {
		return io.ErrUnexpectedEOF
	}

	return err
}
