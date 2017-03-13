package captain

import (
	"bytes"
	"encoding/binary"
	"errors"
	"io"
	"testing"
	"time"
)

func TestNewRecord(t *testing.T) {
	p := []byte{'a'}
	now := time.Now().UTC()
	r := NewRecord(p)
	if r.Time.Sub(now) >= 1*time.Millisecond {
		t.Fatalf("New Record time mismatch")
	}

	if !bytes.Equal(r.Payload, p) {
		t.Fatalf("Payload mismatch, act=%s, exp=%s", r.Payload, p)
	}
}

func TestRecordMarshalUnmarshalBinary(t *testing.T) {
	payload := []byte{'a'}
	expR := NewRecord(payload)

	b, err := expR.MarshalBinary()
	if err != nil {
		t.Fatalf("Marshal Binary err=%s", err)
	}

	actR := &Record{}
	err = actR.UnmarshalBinaryFromReader(bytes.NewReader(b))
	if err != nil {
		t.Fatalf("Unmarshal Binary err=%s", err)
	}

	if expR.Time != actR.Time {
		t.Fatalf("Time mismatch, act=%s, exp=%s", actR.Time, expR.Time)
	}

	if !bytes.Equal(expR.Payload, actR.Payload) {
		t.Fatalf("Payload mismatch, act=%s, exp=%s", actR.Payload, expR.Payload)
	}
}

func TestRecordMarshalBinaryInvalidTime(t *testing.T) {
	r := &Record{
		Time: time.Date(0, 1, 2, 3, 4, 5, 6, time.FixedZone("", -1*60)),
	}

	b, err := r.MarshalBinary()
	if b != nil || err == nil {
		t.Fatalf("Marshal Binary mismatch, act.bytes=%s, act.err=%s, exp err", b, err)
	}
}

func TestRecordMarshaBinaryInvalidBinaryWrite(t *testing.T) {
	testErr := errors.New("test error")
	copyWrite := binaryWrite
	binaryWrite = func(w io.Writer, order binary.ByteOrder, data interface{}) error {
		return testErr
	}
	defer func() { binaryWrite = copyWrite }()

	r := NewRecord([]byte("a"))
	b, err := r.MarshalBinary()
	if err != testErr || b != nil {
		t.Fatalf("Marshal Binary mismatch, act.bytes=%s, act.err=%s, exp.err=%s", b, err, testErr)
	}
}

func TestRecordMarshalBinaryInvalidCRCWrite(t *testing.T) {
	testErr := errors.New("test error")
	copyWrite := binaryWrite
	binaryWrite = func(w io.Writer, order binary.ByteOrder, data interface{}) error {
		d, ok := data.(uint32)
		// Intercept CRC write
		if ok && d == uint32(3508673586) {
			return testErr
		}

		return copyWrite(w, order, data)
	}
	defer func() { binaryWrite = copyWrite }()

	r := NewRecord([]byte("a"))
	r.Time = time.Unix(1482828625, 0).UTC()
	b, err := r.MarshalBinary()
	if err != testErr || b != nil {
		t.Fatalf("Marshal Binary mismatch, act.bytes=%#v, act.err=%s, exp.err=%s", b, err, testErr)
	}
}

func TestRecordUnmarshalBinaryReaderInvalidInputs(t *testing.T) {
	tests := [][]byte{
		[]byte{},
		[]byte{0x0, 0x0, 0x0, 0x0, 0xe, 0xcf, 0xf4, 0x1f, 0x52, 0x2b, 0x35, 0x18, 0x70, 0xff, 0xff, 0x0, 0x0, 0x0, 0x1, 0x61, 0x22, 0xc2, 0x5e, 0x2b}, // Invalid time version byte
		[]byte{0x1, 0x0, 0x0, 0x0, 0xe, 0xcf, 0xf4, 0x1f, 0x52, 0x2b, 0x35, 0x18, 0x70, 0xff, 0xff},                                                   // Missing Size Varint
		[]byte{0x1, 0x0, 0x0, 0x0, 0xe, 0xcf, 0xf4, 0x1f, 0x52, 0x2b, 0x35, 0x18, 0x70, 0xff, 0xff, 0x1},                                              // Missing Payload
		[]byte{0x1, 0x0, 0x0, 0x0, 0xe, 0xcf, 0xf4, 0x1f, 0x52, 0x2b, 0x35, 0x18, 0x70, 0xff, 0xff, 0x1, 0x61, 0x22, 0xc2, 0x5e},                      // CRC Length Mismatch (missing last byte)
		[]byte{0x1, 0x0, 0x0, 0x0, 0xe, 0xcf, 0xf4, 0x1f, 0x52, 0x2b, 0x35, 0x18, 0x70, 0xff, 0xff, 0x1, 0x61, 0x21, 0xc2, 0x5e, 0x2b},                // CRC Value Mismatch
	}

	for _, b := range tests {
		buf := bytes.NewReader(b)
		r := &Record{}
		err := r.UnmarshalBinaryFromReader(buf)
		if err == nil {
			t.Fatalf("Unmarshal Binary err, act=nil, exp err")
		}
	}
}
