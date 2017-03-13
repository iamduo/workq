package job

import (
	"errors"
	"regexp"
	"time"

	"github.com/satori/go.uuid"
)

const (
	MaxName = 255

	// 1 MiB = 1,024 KiB = 1,048,576 bytes
	MaxPayload = 1048576
	MaxResult  = 1048576

	MaxAttempts = 255
	MaxFails    = 255

	// 24 hours in ms
	MaxTTR = 86400000
	// 30 days in ms
	MaxTTL          = 2592000000
	MaxHardAttempts = 255
	TimeFormat      = "2006-01-02T15:04:05Z"

	// Max timeout for wait related cmds (lease).
	MaxTimeout = 86400000
)

var (
	ErrInvalidID          = errors.New("Invalid ID")
	ErrInvalidName        = errors.New("Invalid Name")
	ErrInvalidPayload     = errors.New("Invalid Payload")
	ErrInvalidResult      = errors.New("Invalid Result")
	ErrInvalidMaxAttempts = errors.New("Invalid Max Attempts")
	ErrInvalidMaxFails    = errors.New("Invalid Max Fails")
	ErrInvalidPriority    = errors.New("Invalid Priority")
	ErrInvalidTTR         = errors.New("Invalid TTR")
	ErrInvalidTTL         = errors.New("Invalid TTL")
	ErrInvalidTime        = errors.New("Invalid Time")
	ErrInvalidTimeout     = errors.New("Invalid Timeout")
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

func (j *Job) Expiration() time.Time {
	dur := time.Duration(j.TTL) * time.Millisecond
	if !j.Time.Equal(time.Time{}) {
		return j.Time.Add(dur)
	}

	return j.Created.Add(dur)
}

// 16 byte UUIDv4
type ID [16]byte // UUIDv4

// Returns canonical UUIDv4 form
// Implements fmt.Stringer
func (id ID) String() string {
	u := uuid.UUID(id)
	return u.String()
}

func ValidateID(id ID) error {
	u := uuid.UUID(id)
	if u != uuid.Nil && u.Version() == 4 && u.Variant() == uuid.VariantRFC4122 {
		return nil
	}

	return ErrInvalidID
}

var nameRe = regexp.MustCompile("^[a-zA-Z0-9_.-]*$")

func ValidateName(name string) error {
	l := len(name)
	if l == 0 || l > MaxName || !nameRe.MatchString(name) {
		return ErrInvalidName
	}

	return nil
}

func ValidatePayload(p []byte) error {
	if len(p) > MaxPayload {
		return ErrInvalidPayload
	}

	return nil
}

func ValidateResult(r []byte) error {
	if len(r) > MaxResult {
		return ErrInvalidResult
	}

	return nil
}

// A valid TTR is 2^32 - 1, non zero, and non-negative.
func ValidateTTR(ttr uint32) error {
	if ttr == 0 || ttr > MaxTTR {
		return ErrInvalidTTR
	}

	return nil
}

// Valid TTL is 2^64 - 1, non zero, and non-negative.
func ValidateTTL(ttl uint64) error {
	if ttl == 0 || ttl > MaxTTL {
		return ErrInvalidTTL
	}

	return nil
}

// Valid time is in UTC, and greater or equal to current time.
func ValidateTime(t time.Time) error {
	if time.Now().After(t) {
		return ErrInvalidTime
	}

	return nil
}

func ValidateTimeout(t uint32) error {
	if t > MaxTimeout {
		return ErrInvalidTimeout
	}

	return nil
}
