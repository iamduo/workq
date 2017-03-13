package handlers

import (
	"bytes"
	"testing"
	"time"

	"github.com/iamduo/workq/int/job"
)

func testIDFromBytes(t *testing.T) {
	tests := []struct {
		b      []byte
		expID  job.ID
		expErr bool
	}{
		{
			[]byte("61a444a0-6128-41c0-8078-cc757d3bd2d8"),
			job.ID([16]byte{0x6b, 0xa7, 0xb8, 0x10, 0x9d, 0xad, 0x11, 0xd1, 0x80, 0xb4, 0x0, 0xc0, 0x4f, 0xd4, 0x30, 0xc4}),
			false,
		},
		{
			[]byte(""),
			job.ID{},
			true,
		},
	}

	for _, tt := range tests {
		jid, err := parseID([]byte(tt.b))
		if tt.expErr && (err != job.ErrInvalidID || jid != job.ID{}) {
			t.Fatalf("Unexpected ID=%s, err=%s", jid, err)
		}

		if !tt.expErr && (err != nil || tt.expID != jid) {
			t.Fatalf("ID mismatch, exp=%s, act=%s", tt.b, jid)
		}
	}
}

func TestParseName(t *testing.T) {
	expName := "q1"
	name, err := parseName([]byte(expName))
	if expName != name || err != nil {
		t.Fatalf("Unexpected name mismatch, name=%s, err=%s", name, err)
	}
}

func TestParsePayload(t *testing.T) {
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
	}

	for _, tt := range tests {
		payload, err := parsePayload(tt.size, tt.payload)
		if tt.expErr && (err != job.ErrInvalidPayload || payload != nil) {
			t.Fatalf("Unexpected payload=%s, err=%s", payload, err)
		}

		if !tt.expErr && (err != nil || !bytes.Equal(tt.payload, payload)) {
			t.Fatalf("Payload mismatch, exp=%s, act=%s", tt.payload, payload)
		}
	}
}

func TestParseResult(t *testing.T) {
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
	}

	for _, tt := range tests {
		result, err := parseResult(tt.size, tt.result)
		if tt.expErr && (err != job.ErrInvalidResult || result != nil) {
			t.Fatalf("Unexpected result=%s, err=%s", result, err)
		}

		if !tt.expErr && (err != nil || !bytes.Equal(tt.result, result)) {
			t.Fatalf("Result mismatch, exp=%s, act=%s", tt.result, result)
		}
	}
}

func TestParseTTR(t *testing.T) {
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
	}

	for _, tt := range tests {
		ttr, err := parseTTR(tt.b)
		if tt.expErr && (err != job.ErrInvalidTTR || ttr != 0) {
			t.Fatalf("Unexpected TTR=%d, err=%s", ttr, err)
		}

		if !tt.expErr && (err != nil || tt.expTTR != ttr) {
			t.Fatalf("TTR mismatch, exp=%d, act=%d", tt.expTTR, ttr)
		}
	}
}

func TestParseTTL(t *testing.T) {
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
	}

	for _, tt := range tests {
		ttl, err := parseTTL(tt.b)
		if tt.expErr && (err != job.ErrInvalidTTL || ttl != 0) {
			t.Fatalf("Unexpected TTL=%d, err=%s", ttl, err)
		}

		if !tt.expErr && (err != nil || tt.expTTL != ttl) {
			t.Fatalf("TTL mismatch, exp=%d, act=%d", tt.expTTL, ttl)
		}
	}
}

func TestParseTime(t *testing.T) {
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
		tt.expTime, _ = time.Parse(job.TimeFormat, string(tt.b[:]))
		jTime, err := parseTime(tt.b)
		if tt.expErr && (err != job.ErrInvalidTime || jTime != nilTime) {
			t.Fatalf("Unexpected time=%s, err=%s", jTime, err)
		}

		if !tt.expErr && (err != nil || tt.expTime != jTime) {
			t.Fatalf("Time mismatch, exp=%s, act=%s", tt.expTime, jTime)
		}
	}
}

func TestParseMaxAttempts(t *testing.T) {
	tests := []struct {
		b         []byte
		expMaxAtt uint8
		expErr    bool
	}{
		{[]byte("1"), 1, false},
		{[]byte("255"), 255, false},
		{[]byte("0"), 0, false},

		{[]byte("-1"), 0, true},
		{[]byte("256"), 0, true},
	}

	for _, tt := range tests {
		maxAtt, err := parseMaxAttempts(tt.b)
		if tt.expErr && (err != job.ErrInvalidMaxAttempts || maxAtt != 0) {

			t.Fatalf("Unexpected max attempts=%d, err=%s", maxAtt, err)
		}

		if !tt.expErr && (err != nil || tt.expMaxAtt != maxAtt) {
			t.Fatalf("Max attempts mismatch, exp=%d, act=%d, err=%v", tt.expMaxAtt, maxAtt, err)
		}
	}
}

func TestParseMaxFails(t *testing.T) {
	tests := []struct {
		b           []byte
		expMaxFails uint8
		expErr      bool
	}{
		{[]byte("1"), 1, false},
		{[]byte("255"), 255, false},
		{[]byte("0"), 0, false},

		{[]byte("-1"), 0, true},
		{[]byte("256"), 0, true},
	}

	for _, tt := range tests {
		maxFails, err := parseMaxFails(tt.b)
		if tt.expErr && (err != job.ErrInvalidMaxFails || maxFails != 0) {
			t.Fatalf("Unexpected max fails=%d, err=%s", maxFails, err)
		}

		if !tt.expErr && (err != nil || tt.expMaxFails != maxFails) {
			t.Fatalf("Max fails mismatch, exp=%d, act=%d, err=%v", tt.expMaxFails, maxFails, err)
		}
	}
}

func TestParsePriority(t *testing.T) {
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
		priority, err := parsePriority(tt.b)
		if tt.expErr && (err != job.ErrInvalidPriority || priority != 0) {
			t.Fatalf("Unexpected priority=%d, err=%s", priority, err)
		}

		if !tt.expErr && (err != nil || tt.expPriority != priority) {
			t.Fatalf("Priority mismatch, exp=%d, act=%d, err=%v", tt.expPriority, priority, err)
		}
	}
}

func assertNewRunRecord(t *testing.T, rec *job.RunRecord) {
	ok := rec.Attempts == 0 &&
		rec.State == job.StateNew &&
		rec.Fails == 0 &&
		rec.Job != nil &&
		rec.Result == nil &&
		rec.Timers[job.RunRecTTLTimerIdx] != nil

	if !ok {
		t.Fatalf("Run record does not appear to be 'new', rec=%+v", rec)
	}
}

func assertRunRecords(t *testing.T, a *job.RunRecord, b *job.RunRecord) {
	ok := a.Attempts == b.Attempts &&
		a.State == b.State &&
		a.Fails == b.Fails &&
		bytes.Equal(a.Result, b.Result)

	if !ok {
		t.Fatalf("Run record mismatch, exp=%+v, act=%+v", a, b)
	}

	if a.Job != nil {
		if b.Job == nil || a.Job.ID != b.Job.ID {
			t.Fatalf("Run record job mismatch, exp=%s, act=%s", a.Job.ID, b.Job.ID)
		}
	}

	// Must do diff range acceptance assert on timers

	if a.Timers[job.RunRecSchedTimerIdx] != nil {
		if b.Timers[job.RunRecSchedTimerIdx] == nil {
			t.Fatalf("Unexpected nil scheduled timer")
		}

		bDeadline := a.Timers[job.RunRecSchedTimerIdx].Deadline
		diff := a.Timers[job.RunRecSchedTimerIdx].Deadline.Sub(bDeadline) * time.Millisecond
		if diff < -1000 || diff > 1000 {
			t.Fatalf("Out of range scheduled timer")
		}
	}

	if a.Timers[job.RunRecTTLTimerIdx] != nil {
		if b.Timers[job.RunRecTTLTimerIdx] == nil {
			t.Fatalf("Unexpected nil TTL timer")
		}

		bDeadline := a.Timers[job.RunRecTTLTimerIdx].Deadline
		diff := a.Timers[job.RunRecTTLTimerIdx].Deadline.Sub(bDeadline) * time.Millisecond
		if diff < -1000 || diff > 1000 {
			t.Fatalf("Out of range TTL timer")
		}
	}
}

func assertJobs(t *testing.T, a *job.Job, b *job.Job) {
	createTimeDiff := a.Time.Sub(b.Time)
	ok := a.ID == b.ID &&
		a.Name == b.Name &&
		bytes.Equal(a.Payload, b.Payload) &&
		a.Priority == b.Priority &&
		a.MaxAttempts == b.MaxAttempts &&
		a.MaxFails == b.MaxFails &&
		a.TTR == b.TTR &&
		a.TTL == b.TTL &&
		a.Time == b.Time &&
		createTimeDiff < 500
	if !ok {
		t.Fatalf("Job mismatch, exp=%+v, act=%+v", a, b)
	}
}
