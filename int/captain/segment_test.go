package captain

import (
	"bytes"
	"testing"
)

// Magic represents "wqwq" (Workq).
var testMagicHeader = &MagicHeader{Magic: 0x77717771, Version: 1}

// Most segment helper functions are tested through Cursor/Appender due to high
// file system coupling that require specific test samples.

func TestMagicHeaderMarshalBinary(t *testing.T) {
	exp := []byte{0x77, 0x71, 0x77, 0x71, 0x00, 0x00, 0x00, 0x01}
	data, err := testMagicHeader.MarshalBinary()
	if err != nil {
		t.Fatalf("Marshal Binary err=%s", err)
	}

	if !bytes.Equal(data, exp) {
		t.Fatalf("Marshal Binary mismatch, act=%v, exp=%v", data, exp)
	}
}

func TestMagicHeaderUnmarshalBinary(t *testing.T) {
	h := &MagicHeader{}
	err := h.UnmarshalBinary([]byte{0x77, 0x71, 0x77, 0x71, 0x00, 0x00, 0x00, 0x01})
	if err != nil {
		t.Fatalf("Unmarshal Binary err=%s", err)
	}
	if *h != *testMagicHeader {
		t.Fatalf("Unmarshal Binary mismatch, act=%v, exp=%v", h, testMagicHeader)
	}
}

func TestMagicHeaderUnmarshalBinaryPartialBytes(t *testing.T) {
	h := &MagicHeader{}
	err := h.UnmarshalBinary([]byte{0x77, 0x71, 0x77, 0x71, 0x00, 0x00, 0x00})
	if err == nil {
		t.Fatalf("Unmarshal Binary err, act=nil, exp err")
	}
}

func TestValidateSegmentHeaderPartialReader(t *testing.T) {
	r := bytes.NewReader([]byte{0x71})
	err := validateSegmentHeader(r, testMagicHeader)
	if err == nil {
		t.Fatalf("Validate Segment Header, act=nil, exp err")
	}
}
