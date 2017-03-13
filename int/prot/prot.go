package prot

import (
	"bufio"
	"bytes"
	"fmt"
	"io"
	"strconv"

	"github.com/iamduo/workq/int/job"
)

const (
	CmdAdd      = "add"
	CmdSchedule = "schedule"
	CmdLease    = "lease"
	CmdRun      = "run"
	CmdComplete = "complete"
	CmdFail     = "fail"
	CmdDelete   = "delete"
	CmdResult   = "result"
	CmdInspect  = "inspect"

	// Max read for a single cmd read.
	// 10000 bytes - max command limit +
	// 1048576 bytes - job max payload
	MaxRead = 1058576

	termLen = 2 // Length of \r\n terminator
)

var (
	// ErrReadErr used for primarily EOF read issues
	// Signifies it may be time to close the conn
	ErrReadErr            = io.EOF
	ErrInvalidCmdArgs     = NewClientErr("Invalid command args")
	ErrInvalidCmdFlags    = NewClientErr("Invalid command flags")
	ErrInvalidPayloadSize = NewClientErr("Invalid payload size")
	ErrInvalidResultSize  = NewClientErr("Invalid result size")

	sepByte  = []byte{' '}
	CrnlByte = []byte{'\r', '\n'}
)

type Interface interface {
	ParseCmd(rdr *bufio.Reader) (*Cmd, error)
	SendReply(w io.Writer, b []byte) error
	SendErr(w io.Writer, errStr string) error
}

type Prot struct{}

// Creates an encapsulation of the request as a minimal Cmd data structure and
// holds all the data required for processing. It is up to each handler to
// ensure the validity of the command data.
func (p Prot) ParseCmd(rdr *bufio.Reader) (*Cmd, error) {
	line, err := rdr.ReadBytes('\n')
	if err != nil {
		return nil, ErrReadErr
	}

	if len(line) < termLen {
		return nil, ErrReadErr
	}

	if len(line) >= termLen {
		if line[len(line)-termLen] != '\r' {
			return nil, ErrReadErr
		}

		line = line[:len(line)-termLen]
	}

	splitArgs := bytes.Split(line, sepByte)
	args := make([][]byte, 0, len(splitArgs))
	flags := make(CmdFlags)
	splitBy := []byte("=")
	for _, v := range splitArgs {
		if len(v) == 0 {
			continue
		}

		if v[0] != '-' || bytes.IndexByte(v, splitBy[0]) == -1 {
			args = append(args, v)
			continue
		}

		s := bytes.Split(v, splitBy)
		// Verify flag key and value are not empty
		// Flag key len includes "-"
		if len(s[0]) <= 1 || len(s[1]) == 0 {
			// Do not allow garbage.
			return nil, ErrInvalidCmdFlags
		}

		// Chop "-" prefix
		k := string(s[0][1:])
		if _, ok := flags[k]; ok {
			return nil, ErrInvalidCmdFlags
		}

		flags[k] = s[1]
	}

	argC := len(args)

	// Switch command name on raw input args
	// First arg must be command name, not flag
	var cmd *Cmd
	switch string(splitArgs[0]) {
	default:
		return nil, ErrUnknownCmd
	case CmdAdd:
		// Verify enough args to read payload size
		if argC != 6 {
			return nil, ErrInvalidCmdArgs
		}

		size, err := payloadSizeFromBytes(args[5])
		if err != nil {
			return nil, ErrInvalidPayloadSize
		}

		payload, err := readPayload(rdr, size)
		if err != nil {
			return nil, err
		}
		args = append(args[1:], payload)
		cmd = NewCmd(CmdAdd, args, flags)
	case CmdSchedule:
		// Verify enough args to read payload size
		if argC != 7 {
			return nil, ErrInvalidCmdArgs
		}

		size, err := payloadSizeFromBytes(args[6])
		if err != nil {
			return nil, ErrInvalidPayloadSize
		}

		payload, err := readPayload(rdr, size)
		if err != nil {
			return nil, err
		}
		args := append(args[1:], payload)
		cmd = NewCmd(CmdSchedule, args, flags)
	case CmdDelete:
		if argC != 2 {
			return nil, ErrInvalidCmdArgs
		}

		args = args[1:]
		cmd = NewCmd(CmdDelete, args, flags)
	case CmdLease:
		if argC < 3 {
			return nil, ErrInvalidCmdArgs
		}

		args = args[1:]
		cmd = NewCmd(CmdLease, args, flags)
	case CmdRun:
		// Verify enough args to read payload size
		if argC != 6 {
			return nil, ErrInvalidCmdArgs
		}

		size, err := payloadSizeFromBytes(args[5])
		if err != nil {
			return nil, ErrInvalidPayloadSize
		}

		payload, err := readPayload(rdr, size)
		if err != nil {
			return nil, err
		}
		args := append(args[1:], payload)
		cmd = NewCmd(CmdRun, args, flags)
	case CmdComplete:
		if argC != 3 {
			return nil, ErrInvalidCmdArgs
		}

		size, err := payloadSizeFromBytes(args[2])
		if err != nil {
			return nil, ErrInvalidPayloadSize
		}

		payload, err := readPayload(rdr, size)
		if err != nil {
			return nil, err
		}

		args := append(args[1:], payload)
		cmd = NewCmd(CmdComplete, args, flags)
	case CmdFail:
		if argC != 3 {
			return nil, ErrInvalidCmdArgs
		}

		size, err := payloadSizeFromBytes(args[2])
		if err != nil {
			return nil, ErrInvalidResultSize
		}

		payload, err := readPayload(rdr, size)
		if err != nil {
			return nil, ErrInvalidResultSize
		}

		args := append(args[1:], payload)
		cmd = NewCmd(CmdFail, args, flags)
	case CmdResult:
		if argC != 3 {
			return nil, ErrInvalidCmdArgs
		}

		args = args[1:]
		cmd = NewCmd(CmdResult, args, flags)
	case CmdInspect:
		if argC < 2 {
			return nil, ErrInvalidCmdArgs
		}

		args = args[1:]
		cmd = NewCmd(CmdInspect, args, flags)
	}

	return cmd, nil
}

func (p Prot) SendReply(w io.Writer, b []byte) error {
	return SendReply(w, b)
}

func (p Prot) SendErr(w io.Writer, errStr string) error {
	return SendErr(w, errStr)
}

func readPayload(rdr *bufio.Reader, size int) ([]byte, error) {
	if size < 0 || size > job.MaxPayload {
		return nil, ErrInvalidPayloadSize
	}

	payload := make([]byte, size)
	_, err := io.ReadAtLeast(rdr, payload, size)
	if err == io.EOF {
		return nil, ErrInvalidPayloadSize
	}

	b := make([]byte, termLen)
	n, err := rdr.Read(b)
	if err != nil || n != termLen || string(b) != string(CrnlByte) {
		// Payload size does not match end of line.
		// Trailing garbage is not allowed.
		return nil, ErrInvalidPayloadSize
	}

	return payload, nil
}

func payloadSizeFromBytes(b []byte) (int, error) {
	return strconv.Atoi(string(b))
}

func SendReply(w io.Writer, b []byte) error {
	r := []byte("+")
	r = append(r, b...)
	_, err := w.Write(r)
	return err
}

func SendErr(w io.Writer, errStr string) error {
	_, err := w.Write([]byte("-" + errStr))
	return err
}

func OkJobResp(id job.ID, name string, payload []byte) []byte {
	b := []byte("OK 1")
	b = append(b, CrnlByte...)
	b = append(b, []byte(fmt.Sprintf("%s %s %d", id, name, len(payload)))...)
	b = append(b, CrnlByte...)
	b = append(b, payload...)
	return append(b, CrnlByte...)
}

func OkResultResp(id job.ID, success bool, r []byte) []byte {
	successInt := 0
	if success {
		successInt = 1
	}

	b := []byte("OK 1")
	b = append(b, CrnlByte...)
	b = append(b, []byte(fmt.Sprintf("%s %d %d", id, successInt, len(r)))...)
	b = append(b, CrnlByte...)
	b = append(b, r...)
	b = append(b, CrnlByte...)
	return b
}

func OkResp() []byte {
	b := []byte("OK")
	return append(b, CrnlByte...)
}
