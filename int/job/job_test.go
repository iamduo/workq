package job

import (
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
	exp := "61a444a0-6128-41c0-8078-cc757d3bd2d8"
	var jid ID
	copy(jid[:], []byte{0x61, 0xa4, 0x44, 0xa0, 0x61, 0x28, 0x41, 0xc0, 0x80, 0x78, 0xcc, 0x75, 0x7d, 0x3b, 0xd2, 0xd8})
	if jid.String() != exp {
		t.Fatalf("ID string mismatch, exp=%s, act=%s", exp, jid.String())
	}
}

func TestJobExpiration(t *testing.T) {
	tests := []struct {
		j   *Job
		exp time.Time
	}{
		{&Job{Created: time.Unix(1483228800, 0), TTL: 10000}, time.Unix(1483228800+10, 0)},
		{&Job{Created: time.Unix(1283228800, 0), Time: time.Unix(1483228800, 0), TTL: 10000}, time.Unix(1483228800+10, 0)},
	}

	for _, tt := range tests {
		if !tt.j.Expiration().Equal(tt.exp) {
			t.Fatalf("Job Expiration mismatch, act=%s, exp=%s", tt.j.Expiration(), tt.exp)
		}
	}
}

func TestValidateName(t *testing.T) {
	tests := []struct {
		name   string
		expErr bool
	}{
		{"q", false},
		{"q1", false},
		{"q1-", false},
		{"q_1-", false},
		{strings.Repeat("q", MaxName), false},

		{"", true},
		{"*", true},
		{"q@", true},
		{strings.Repeat("q", MaxName+1), true},
	}

	for _, tt := range tests {
		err := ValidateName(tt.name)

		if tt.expErr && err != ErrInvalidName {
			t.Fatalf("Err mismatch, name=%s, err=%s", tt.name, err)
		}
	}
}

func TestValidatePayload(t *testing.T) {
	tests := []struct {
		payload []byte
		expErr  bool
	}{
		{[]byte(""), false},
		{[]byte("q"), false},
		{make([]byte, MaxPayload), false},

		{make([]byte, MaxPayload+1), true},
	}

	for _, tt := range tests {
		err := ValidatePayload(tt.payload)
		if tt.expErr && err != ErrInvalidPayload {
			t.Fatalf("Err mismatch, payload=%v, err=%s", tt.payload, err)
		}
	}
}
