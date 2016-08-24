package job

import (
	"errors"
	"regexp"
	"strconv"
	"time"

	"github.com/satori/go.uuid"
)

const (
	// 1 MiB = 1,024 KiB = 1,048,576 bytes
	MaxPayload = 1048576
	MaxResult  = 1048576
	// Abritrary safetey boundary
	MaxAttempts = 255
	MaxFails    = 255

	// 24 hours in ms
	MaxTTR = 86400000
	// 30 days in ms
	MaxTTL          = 2592000000
	MaxHardAttempts = 255
	TimeFormat      = "2006-01-02T15:04:05Z"
)

var (
	ErrInvalidID          = errors.New("Invalid Job ID")
	ErrInvalidName        = errors.New("Invalid Job Name")
	ErrInvalidPayload     = errors.New("Invalid Job Payload")
	ErrInvalidResult      = errors.New("Invalid Job Result")
	ErrInvalidMaxAttempts = errors.New("Invalid Job Max Attempts")
	ErrInvalidMaxFails    = errors.New("Invalid Job Max Fails")
	ErrInvalidPriority    = errors.New("Invalid Job Priority")
	ErrInvalidTTR         = errors.New("Invalid Job TTR")
	ErrInvalidTTL         = errors.New("Invalid Job TTL")
	ErrInvalidTime        = errors.New("Invalid Job Time")
)

type Job struct {
	ID          ID
	Name        string    // Unique name of job
	Payload     []byte    // 1MB limit
	Priority    int32     // Priority from lowest to highest
	MaxAttempts uint8     // Num of allowed attempts
	MaxFails    uint8     // Num of allowed failures
	TTR         uint32    // time to run in ms
	TTL         uint64    // max time to live in ms
	Time        time.Time // Scheduled Time to Exec
	Created     time.Time
}

// New Empty Job returns an Job with its created time initiliazed.
func NewEmptyJob() *Job {
	return &Job{Created: time.Now().UTC()}
}

// 16 byte UUIDv4
type ID [16]byte // UUIDv4

// Returns canonical UUIDv4 form
// Implements fmt.Stringer
func (id ID) String() string {
	u := uuid.UUID(id)
	return u.String()
}

// Returns an ID from a UUIDv4 byte slice
// Returns an error if input is not a valid UUIDv4
func IDFromBytes(b []byte) (ID, error) {
	s := string(b)
	id, err := uuid.FromString(s)
	if err != nil {
		return ID{}, ErrInvalidID
	}

	jid := ID(id)
	return jid, nil
}

var nameRe = regexp.MustCompile("^[a-zA-Z0-9_.-]*$")

// Return a name string from byte slice
// Returns an error if name is not alphanumeric + special chars: "_", ".", "-"
func NameFromBytes(b []byte) (string, error) {
	name := string(b)
	l := len(name)
	if l > 0 && l <= 128 && nameRe.MatchString(name) {
		return name, nil
	}

	return "", ErrInvalidName
}

// Return a valid payload slice from a size and payload slice
// Returns an error if size does not match actual payload size
// or if payload exceeds max size.
func PayloadFromBytes(size []byte, payload []byte) ([]byte, error) {
	assertSize, err := strconv.Atoi(string(size))
	if err != nil {
		return nil, ErrInvalidPayload
	}

	actSize := len(payload)
	if assertSize != actSize || actSize > MaxPayload {
		return nil, ErrInvalidPayload
	}

	return payload, nil
}

// Return a valid result from a size and result slice
// Returns an error if size does no match actual result size
// or if result exceeds max size
func ResultFromBytes(size []byte, result []byte) ([]byte, error) {
	assertSize, err := strconv.Atoi(string(size))
	if err != nil {
		return nil, ErrInvalidResult
	}

	actSize := len(result)
	if assertSize != actSize || actSize > MaxResult {
		return nil, ErrInvalidResult
	}

	return result, nil
}

// Return a valid TTR from slice.
// A valid TTR is 2^32 - 1 and non-negative.
// Returns an error if TTR is out of range.
func TTRFromBytes(b []byte) (uint32, error) {
	ttr, err := strconv.ParseUint(string(b), 10, 32)
	if err != nil || ttr > MaxTTR {
		return 0, ErrInvalidTTR
	}

	return uint32(ttr), nil
}

// Return a valid TTL from slice
// Valid TTL is 2^64 - 1 and non non-negative
// Returns an error if TTL is out of range
func TTLFromBytes(b []byte) (uint64, error) {
	ttl, err := strconv.ParseUint(string(b), 10, 64)
	if err != nil || ttl > MaxTTL {
		return 0, ErrInvalidTTL
	}

	return ttl, nil
}

// Return a valid scheduled time from slice.
// Valid time format is set in const TimeFormat and is UTC.
// Only legal if scheduled time is in the future.
func TimeFromBytes(b []byte) (time.Time, error) {
	t, err := time.Parse(TimeFormat, string(b))
	if err != nil {
		return time.Time{}, ErrInvalidTime
	}

	return t, err
}

func IsTimeRelevant(t time.Time) bool {
	dur := t.Sub(time.Now().UTC())
	if dur.Seconds() >= 0 {
		return true
	}

	return false
}

// Return a valid max attempt from slice.
// Valid max attempt is 0-255.
// Returns an error if out of range.
func MaxAttemptsFromBytes(b []byte) (uint8, error) {
	attempts, err := strconv.ParseUint(string(b), 10, 8)
	if err != nil || attempts <= 0 || attempts > MaxAttempts {
		return 0, ErrInvalidMaxAttempts
	}

	return uint8(attempts), nil
}

// Return a valid max fails from slice.
// Valid max fails is 0-255.
// Returns an error if out of range.
func MaxFailsFromBytes(b []byte) (uint8, error) {
	fails, err := strconv.ParseUint(string(b), 10, 8)
	if err != nil || fails <= 0 || fails > MaxFails {
		return 0, ErrInvalidMaxFails
	}

	return uint8(fails), nil
}

// Return a valid priority from slice.
// Valid priority is within int32.
// Returns an error if out of range.
func PriorityFromBytes(b []byte) (int32, error) {
	priority, err := strconv.ParseInt(string(b), 10, 32)
	if err != nil {
		return 0, ErrInvalidPriority
	}

	return int32(priority), nil
}
