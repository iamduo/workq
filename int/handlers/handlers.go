package handlers

import (
	"strconv"
	"time"

	"github.com/iamduo/workq/int/job"
	"github.com/iamduo/workq/int/prot"
	"github.com/satori/go.uuid"
)

var (
	ErrDuplicateJob       = prot.NewClientErr(job.ErrDuplicateJob.Error())
	ErrLeaseExpired       = prot.NewClientErr("Lease expired")
	ErrInvalidWaitTimeout = prot.NewClientErr("Invalid wait timeout")
)

func parseTimeout(b []byte) (uint32, error) {
	timeout, err := strconv.ParseUint(string(b), 10, 32)
	if err != nil || timeout > job.MaxTimeout {
		return 0, ErrInvalidWaitTimeout
	}

	return uint32(timeout), nil
}

// Returns an ID from a UUIDv4 byte slice
// Returns an error if input is not a valid UUIDv4
func parseID(b []byte) (job.ID, error) {
	s := string(b)
	id, err := uuid.FromString(s)
	if err != nil {
		return job.ID{}, job.ErrInvalidID
	}

	jid := job.ID(id)
	return jid, nil
}

// Return a name string from byte slice
// Error will always be nil, present to align with other *FromBytes methods.
func parseName(b []byte) (string, error) {
	if len(b) == 0 {
		return "", job.ErrInvalidName
	}

	return string(b), nil
}

// Return a payload slice from a size and payload slice
// Returns an error if size does not match actual payload size.
func parsePayload(size []byte, payload []byte) ([]byte, error) {
	assertSize, err := strconv.Atoi(string(size))
	if err != nil || assertSize != len(payload) {
		return nil, job.ErrInvalidPayload
	}

	return payload, nil
}

// Return a result from a size and result slice
// Returns an error if size does no match actual result size.
func parseResult(size []byte, result []byte) ([]byte, error) {
	assertSize, err := strconv.Atoi(string(size))
	if err != nil || assertSize != len(result) {
		return nil, job.ErrInvalidResult
	}

	return result, nil
}

// Return a TTR from byte slice.
// Returns an error if TTR parsing fails.
func parseTTR(b []byte) (uint32, error) {
	ttr, err := strconv.ParseUint(string(b), 10, 32)
	if err != nil {
		return 0, job.ErrInvalidTTR
	}

	return uint32(ttr), nil
}

// Return a TTL from byte slice
// Returns an error if TTL parsing fails.
func parseTTL(b []byte) (uint64, error) {
	ttl, err := strconv.ParseUint(string(b), 10, 64)
	if err != nil {
		return 0, job.ErrInvalidTTL
	}

	return ttl, nil
}

// Return a scheduled time from slice.
// Valid time format is set in const TimeFormat and is UTC.
func parseTime(b []byte) (time.Time, error) {
	t, err := time.Parse(job.TimeFormat, string(b))
	if err != nil {
		return time.Time{}, job.ErrInvalidTime
	}

	return t, err
}

// Return a max attempt from byte slice.
// Valid max attempt is 0-255.
// Returns an error if max attempt parsing fails.
func parseMaxAttempts(b []byte) (uint8, error) {
	attempts, err := strconv.ParseUint(string(b), 10, 8)
	if err != nil {
		return 0, job.ErrInvalidMaxAttempts
	}

	return uint8(attempts), nil
}

// Parse a max fail value from byte slice.
// Returns an error if failed to parse
func parseMaxFails(b []byte) (uint8, error) {
	fails, err := strconv.ParseUint(string(b), 10, 8)
	if err != nil {
		return 0, job.ErrInvalidMaxFails
	}

	return uint8(fails), nil
}

// Return a priority from byte slice.
// Returns an error if priority parsing fails.
func parsePriority(b []byte) (int32, error) {
	priority, err := strconv.ParseInt(string(b), 10, 32)
	if err != nil {
		return 0, job.ErrInvalidPriority
	}

	return int32(priority), nil
}
