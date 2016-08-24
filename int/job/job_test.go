package job

import (
	"bytes"
	"strings"
	"testing"
	"time"
)

func TestNewEmptyJob(t *testing.T) {
	now := time.Now().UTC()
	j := NewEmptyJob()
	if j.Created.Sub(now) >= 100*time.Microsecond {
		t.Fatalf("New empty job created time out of range")
	}
}

func TestIDString(t *testing.T) {
	exp := "6ba7b810-9dad-11d1-80b4-00c04fd430c4"
	var jid ID
	copy(jid[:], []byte{0x6b, 0xa7, 0xb8, 0x10, 0x9d, 0xad, 0x11, 0xd1, 0x80, 0xb4, 0x0, 0xc0, 0x4f, 0xd4, 0x30, 0xc4})
	if jid.String() != exp {
		t.Fatalf("ID string mismatch, exp=%s, act=%s", exp, jid.String())
	}
}

func TestIDFromBytes(t *testing.T) {
	tests := []struct {
		b      []byte
		expID  ID
		expErr bool
	}{
		{
			[]byte("6ba7b810-9dad-11d1-80b4-00c04fd430c4"),
			ID([16]byte{0x6b, 0xa7, 0xb8, 0x10, 0x9d, 0xad, 0x11, 0xd1, 0x80, 0xb4, 0x0, 0xc0, 0x4f, 0xd4, 0x30, 0xc4}),
			false,
		},
		{
			[]byte(""),
			ID{},
			true,
		},
	}

	for _, tt := range tests {
		jid, err := IDFromBytes([]byte(tt.b))
		if tt.expErr && (err != ErrInvalidID || jid != ID{}) {
			t.Fatalf("Unexpected ID=%s, err=%s", jid, err)
		}

		if !tt.expErr && (err != nil || tt.expID != jid) {
			t.Fatalf("ID mismatch, exp=%s, act=%s", tt.b, jid)
		}
	}
}

func TestNameFromBytes(t *testing.T) {
	tests := []struct {
		b      []byte
		expErr bool
	}{
		{[]byte("q"), false},
		{[]byte("q1"), false},
		{[]byte("q1-"), false},
		{[]byte("q_1-"), false},
		{[]byte(strings.Repeat("q", 128)), false},

		{[]byte(""), true},
		{[]byte("*"), true},
		{[]byte("q@"), true},
		{[]byte(strings.Repeat("q", 129)), true},
	}

	for _, tt := range tests {
		name, err := NameFromBytes(tt.b)
		if tt.expErr && (err != ErrInvalidName || name != "") {
			t.Fatalf("Unexpected name=%s, err=%s", name, err)
		}

		if !tt.expErr && (err != nil || string(tt.b) != name) {
			t.Fatalf("Name mismatch, exp=%s, act=%s", tt.b, name)
		}
	}
}

func TestPayloadFromBytes(t *testing.T) {
	tests := []struct {
		size    []byte
		payload []byte
		expErr  bool
	}{
		{[]byte("0"), []byte(""), false},
		{[]byte("1"), []byte("q"), false},
		{[]byte("1048576"), make([]byte, 1048576), false},

		{[]byte(""), []byte(""), true},
		{[]byte(" "), []byte(" "), true},
		{[]byte(""), []byte("q"), true},
		{[]byte("-1"), []byte("q"), true},
		{[]byte(" "), []byte("q"), true},
		{[]byte("1"), []byte("qq"), true},
		{[]byte("2"), []byte("q"), true},
		{[]byte("1048577"), make([]byte, 1048577), true},
	}

	for _, tt := range tests {
		payload, err := PayloadFromBytes(tt.size, tt.payload)
		if tt.expErr && (err != ErrInvalidPayload || payload != nil) {
			t.Fatalf("Unexpected payload=%s, err=%s", payload, err)
		}

		if !tt.expErr && (err != nil || !bytes.Equal(tt.payload, payload)) {
			t.Fatalf("Payload mismatch, exp=%s, act=%s", tt.payload, payload)
		}
	}
}

func TestResultFromBytes(t *testing.T) {
	tests := []struct {
		size   []byte
		result []byte
		expErr bool
	}{
		{[]byte("0"), []byte(""), false},
		{[]byte("1"), []byte("q"), false},
		{[]byte("1048576"), make([]byte, 1048576), false},

		{[]byte(""), []byte(""), true},
		{[]byte(" "), []byte(" "), true},
		{[]byte(""), []byte("q"), true},
		{[]byte("-1"), []byte("q"), true},
		{[]byte(" "), []byte("q"), true},
		{[]byte("1"), []byte("qq"), true},
		{[]byte("2"), []byte("q"), true},
		{[]byte("1048577"), make([]byte, 1048577), true},
	}

	for _, tt := range tests {
		result, err := ResultFromBytes(tt.size, tt.result)
		if tt.expErr && (err != ErrInvalidResult || result != nil) {
			t.Fatalf("Unexpected result=%s, err=%s", result, err)
		}

		if !tt.expErr && (err != nil || !bytes.Equal(tt.result, result)) {
			t.Fatalf("Result mismatch, exp=%s, act=%s", tt.result, result)
		}
	}
}

func TestTTRFromBytes(t *testing.T) {
	tests := []struct {
		b      []byte
		expTTR uint32
		expErr bool
	}{
		{[]byte("0"), 0, false},
		{[]byte("1"), 1, false},
		{[]byte("86400000"), 86400000, false},

		{[]byte(""), 0, true},
		{[]byte("-1"), 0, true},
		{[]byte("1*"), 0, true},
		{[]byte("*"), 0, true},
		{[]byte("86400001"), 0, true},
	}

	for _, tt := range tests {
		ttr, err := TTRFromBytes(tt.b)
		if tt.expErr && (err != ErrInvalidTTR || ttr != 0) {
			t.Fatalf("Unexpected TTR=%d, err=%s", ttr, err)
		}

		if !tt.expErr && (err != nil || tt.expTTR != ttr) {
			t.Fatalf("TTR mismatch, exp=%d, act=%d", tt.expTTR, ttr)
		}
	}
}

func TestTTLFromBytes(t *testing.T) {
	tests := []struct {
		b      []byte
		expTTL uint64
		expErr bool
	}{
		{[]byte("0"), 0, false},
		{[]byte("1"), 1, false},
		{[]byte("2592000000"), 2592000000, false},

		{[]byte(""), 0, true},
		{[]byte("-1"), 0, true},
		{[]byte("1*"), 0, true},
		{[]byte("*"), 0, true},
		{[]byte("2592000001"), 0, true},
	}

	for _, tt := range tests {
		ttl, err := TTLFromBytes(tt.b)
		if tt.expErr && (err != ErrInvalidTTL || ttl != 0) {
			t.Fatalf("Unexpected TTL=%d, err=%s", ttl, err)
		}

		if !tt.expErr && (err != nil || tt.expTTL != ttl) {
			t.Fatalf("TTL mismatch, exp=%d, act=%d", tt.expTTL, ttl)
		}
	}
}

func TestTimeFromBytes(t *testing.T) {
	tests := []struct {
		b       []byte
		expTime time.Time
		expErr  bool
	}{
		{[]byte("2016-01-01T00:00:00Z"), time.Time{}, false},

		{[]byte("2016-01-01T00:00:00"), time.Time{}, true},
		{[]byte("2016-01-01T01:00:00Z07:00"), time.Time{}, true},
		{[]byte("2016-01-01T99:00:00Z"), time.Time{}, true},
	}

	nilTime := time.Time{}
	for _, tt := range tests {
		tt.expTime, _ = time.Parse(TimeFormat, string(tt.b[:]))
		jTime, err := TimeFromBytes(tt.b)
		if tt.expErr && (err != ErrInvalidTime || jTime != nilTime) {
			t.Fatalf("Unexpected time=%s, err=%s", jTime, err)
		}

		if !tt.expErr && (err != nil || tt.expTime != jTime) {
			t.Fatalf("Time mismatch, exp=%s, act=%s", tt.expTime, jTime)
		}
	}
}

func TestIsTimeRelevant(t *testing.T) {
	jTime := time.Now().UTC().Add(1 * time.Second)
	if !IsTimeRelevant(jTime) {
		t.Fatalf("Expected time to still be relevant, time=%s", jTime)
	}

	jTime = time.Now().UTC().Add(-1 * time.Second)
	if IsTimeRelevant(jTime) {
		t.Fatalf("Expected time to not be relevant, time=%s", jTime)
	}
}

func TestMaxAttemptsFromBytes(t *testing.T) {
	tests := []struct {
		b         []byte
		expMaxAtt uint8
		expErr    bool
	}{
		{[]byte("1"), 1, false},
		{[]byte("255"), 255, false},

		{[]byte("0"), 0, true},
		{[]byte("-1"), 0, true},
		{[]byte("256"), 0, true},
	}

	for _, tt := range tests {
		maxAtt, err := MaxAttemptsFromBytes(tt.b)
		if tt.expErr && (err != ErrInvalidMaxAttempts || maxAtt != 0) {

			t.Fatalf("Unexpected max attempts=%d, err=%s", maxAtt, err)
		}

		if !tt.expErr && (err != nil || tt.expMaxAtt != maxAtt) {
			t.Fatalf("Max attempts mismatch, exp=%d, act=%d, err=%v", tt.expMaxAtt, maxAtt, err)
		}
	}
}

func TestMaxFailsFromBytes(t *testing.T) {
	tests := []struct {
		b           []byte
		expMaxFails uint8
		expErr      bool
	}{
		{[]byte("1"), 1, false},
		{[]byte("255"), 255, false},

		{[]byte("0"), 0, true},
		{[]byte("-1"), 0, true},
		{[]byte("256"), 0, true},
	}

	for _, tt := range tests {
		maxFails, err := MaxFailsFromBytes(tt.b)
		if tt.expErr && (err != ErrInvalidMaxFails || maxFails != 0) {
			t.Fatalf("Unexpected max fails=%d, err=%s", maxFails, err)
		}

		if !tt.expErr && (err != nil || tt.expMaxFails != maxFails) {
			t.Fatalf("Max fails mismatch, exp=%d, act=%d, err=%v", tt.expMaxFails, maxFails, err)
		}
	}
}

func TestPriorityFromBytes(t *testing.T) {
	tests := []struct {
		b           []byte
		expPriority int32
		expErr      bool
	}{
		{[]byte("0"), 0, false},
		{[]byte("1"), 1, false},
		{[]byte("-2147483648"), -2147483648, false},
		{[]byte("2147483647"), 2147483647, false},

		{[]byte("-2147483649"), 0, true},
		{[]byte("2147483648"), 0, true},
	}

	for _, tt := range tests {
		priority, err := PriorityFromBytes(tt.b)
		if tt.expErr && (err != ErrInvalidPriority || priority != 0) {
			t.Fatalf("Unexpected priority=%d, err=%s", priority, err)
		}

		if !tt.expErr && (err != nil || tt.expPriority != priority) {
			t.Fatalf("Priority mismatch, exp=%d, act=%d, err=%v", tt.expPriority, priority, err)
		}
	}
}
