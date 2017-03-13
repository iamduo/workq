package cmdlog

import (
	"bytes"
	"encoding/binary"
	"io"
	"log"
	"time"

	"github.com/iamduo/workq/int/captain"
	"github.com/iamduo/workq/int/job"
)

const (
	cmdAdd            = 1
	cmdSchedule       = 2
	cmdComplete       = 3
	cmdFail           = 4
	cmdDelete         = 5
	cmdExpire         = 6
	cmdStartAttempt   = 7
	cmdTimeoutAttempt = 8
)

// MagicHeader represents the header all workq log files should use.
var MagicHeader = &captain.MagicHeader{Magic: 0x77717771, Version: 1}

// ControllerProxy intercepts job.Controller methods that change state and
// records a cmlog entry before returning to the caller.
type ControllerProxy struct {
	buf                [][]byte
	appender           appender
	controller         job.ControllerInterface
	expireFunc         func(job.ID)
	timeoutAttemptFunc func(job.ID)
}

// NewControllerProxy returns a new ControllerProxy and wraps the specified job
// controller.
func NewControllerProxy(a appender, c job.ControllerInterface) *ControllerProxy {
	p := &ControllerProxy{
		appender:   a,
		controller: c,
	}

	// Swap Expire & TimeoutAttempt callbacks.
	// Allows for the interception of these background commands.

	// Configure the Expire & TimeoutAttempt proxies.
	// Inject the origin controller's Expire & TimeoutAttempt callbacks.
	//

	// Proxy Expire & TimeoutAttempt callbacks by swapping the origin's callbacks
	// with the local proxy callback. // Allows for the interception/logging of these
	// background callbacks.
	// origin.Expire -> proxy.Expire
	// origin.TimeoutAttempt -> proxy.TimeoutAttempt.
	p.HandleExpire(c.ExpireFunc())
	p.HandleTimeoutAttempt(c.TimeoutAttemptFunc())

	c.HandleExpire(p.Expire)
	c.HandleTimeoutAttempt(p.TimeoutAttempt)
	return p
}

type appender interface {
	Append(b []byte) error
}

func (c *ControllerProxy) Add(j *job.Job) error {
	err := c.controller.Add(j)
	if err == nil {
		c.append(marshalAdd(j))
	}

	return err
}

func marshalAdd(j *job.Job) []byte {
	buf := new(bytes.Buffer)
	writeType(buf, cmdAdd)
	writeJob(buf, j)
	return buf.Bytes()
}

func (c *ControllerProxy) Complete(id job.ID, result []byte) error {
	err := c.controller.Complete(id, result)
	if err == nil {
		c.append(marshalComplete(id, result))
	}

	return err
}

func marshalComplete(id job.ID, result []byte) []byte {
	buf := new(bytes.Buffer)
	writeType(buf, cmdComplete)
	writeID(buf, id)
	writePayload(buf, result)
	return buf.Bytes()
}

func (c *ControllerProxy) Fail(id job.ID, result []byte) error {
	err := c.controller.Fail(id, result)
	if err == nil {
		c.append(marshalFail(id, result))
	}

	return err
}

func marshalFail(id job.ID, result []byte) []byte {
	buf := new(bytes.Buffer)
	writeType(buf, cmdFail)
	writeID(buf, id)
	writePayload(buf, result)
	return buf.Bytes()
}

func (c *ControllerProxy) Delete(id job.ID) error {
	err := c.controller.Delete(id)
	if err == nil {
		c.append(marshalDelete(id))
	}

	return err
}

func marshalDelete(id job.ID) []byte {
	buf := new(bytes.Buffer)
	writeType(buf, cmdDelete)
	writeID(buf, id)
	return buf.Bytes()
}

func (c *ControllerProxy) Lease(names []string, timeout uint32) (*job.Job, error) {
	j, err := c.controller.Lease(names, timeout)
	if err == nil {
		// Replay on Lease cmd is transformed into the actual job leased as an
		// internal "StartAttempt" cmd which is idempotent.
		// Allows for lease replays to be unambigous and does not rely on the order of the
		// queue during replay. This is important as the log may be cleaned/compacted.
		c.append(marshalStartAttempt(j.ID))
	}

	return j, err
}

func marshalStartAttempt(id job.ID) []byte {
	buf := new(bytes.Buffer)
	writeType(buf, cmdStartAttempt)
	writeID(buf, id)
	return buf.Bytes()
}

func (c *ControllerProxy) StartAttempt(id job.ID) error {
	// StartAttempt cmd is the internal translation of a successful lease cmd.
	// We only proxy directly here for replay. The capturing takes place in lease.
	return c.controller.StartAttempt(id)
}

func (c *ControllerProxy) TimeoutAttempt(id job.ID) {
	c.timeoutAttemptFunc(id)
	c.append(marshalTimeoutAttempt(id))
}

func marshalTimeoutAttempt(id job.ID) []byte {
	buf := new(bytes.Buffer)
	writeType(buf, cmdTimeoutAttempt)
	writeID(buf, id)
	return buf.Bytes()
}

func (c *ControllerProxy) HandleTimeoutAttempt(f func(job.ID)) {
	c.timeoutAttemptFunc = f
}

func (c *ControllerProxy) TimeoutAttemptFunc() func(job.ID) {
	return c.timeoutAttemptFunc
}

func (c *ControllerProxy) Run(j *job.Job, timeout uint32) (*job.RunResult, error) {
	return c.controller.Run(j, timeout)
}

func (c *ControllerProxy) Schedule(j *job.Job) error {
	err := c.controller.Schedule(j)
	if err == nil {
		c.append(marshalSchedule(j))
	}

	return err
}

func marshalSchedule(j *job.Job) []byte {
	buf := new(bytes.Buffer)
	writeType(buf, cmdSchedule)
	writeJob(buf, j)
	return buf.Bytes()
}

func (c *ControllerProxy) Expire(id job.ID) {
	c.expireFunc(id)
	c.append(marshalExpire(id))
}

func marshalExpire(id job.ID) []byte {
	buf := new(bytes.Buffer)
	writeType(buf, cmdExpire)
	writeID(buf, id)
	return buf.Bytes()
}

func (c *ControllerProxy) HandleExpire(f func(job.ID)) {
	c.expireFunc = f
}

func (c *ControllerProxy) ExpireFunc() func(job.ID) {
	return c.expireFunc
}

func (c *ControllerProxy) append(b []byte) {
	err := c.appender.Append(b)
	if err != nil {
		// Fatalf here as the appender above is intended to be a CircuitBreakerAppender.
		// CircuitBreakerAppender will attempt to recover a failed append forever until success.
		// Fatalf will provide a develop stage warning in case of an incorrect appender used.
		log.Fatalf("cmdlog append err=%s", err)
	}
}

func writeType(b *bytes.Buffer, t int) {
	b.WriteByte(byte(t))
}

func writeID(b *bytes.Buffer, id job.ID) {
	b.Write(id[:])
}

func writeName(b *bytes.Buffer, name string) {
	vBuf := make([]byte, binary.MaxVarintLen16)
	n := binary.PutUvarint(vBuf, uint64(len(name)))
	b.Write(vBuf[:n])
	b.Write([]byte(name))
}

func writeJob(b *bytes.Buffer, j *job.Job) error {
	writeID(b, j.ID)
	writeName(b, j.Name)

	vBuf := make([]byte, binary.MaxVarintLen64)

	n := binary.PutUvarint(vBuf, uint64(j.TTR))
	b.Write(vBuf[:n])

	n = binary.PutUvarint(vBuf, uint64(j.TTL))
	b.Write(vBuf[:n])

	n = binary.PutVarint(vBuf, int64(j.Priority))
	b.Write(vBuf[:n])

	n = binary.PutUvarint(vBuf, uint64(j.MaxAttempts))
	b.Write(vBuf[:n])

	n = binary.PutUvarint(vBuf, uint64(j.MaxFails))
	b.Write(vBuf[:n])

	writePayload(b, j.Payload)

	tb, err := j.Time.MarshalBinary()
	if err != nil {
		return err
	}

	b.Write(tb)

	tb, err = j.Created.MarshalBinary()
	if err != nil {
		return err
	}

	b.Write(tb)

	return nil
}

func writePayload(b *bytes.Buffer, p []byte) {
	vBuf := make([]byte, binary.MaxVarintLen64)
	n := binary.PutUvarint(vBuf, uint64(len(p)))
	b.Write(vBuf[:n])
	b.Write(p)
}

type decodeReader interface {
	io.Reader
	io.ByteReader
}

type decoder struct {
	r decodeReader
}

func newDecoder(b []byte) *decoder {
	return &decoder{
		r: bytes.NewReader(b),
	}
}

func (d *decoder) ReadType() (int, error) {
	b := make([]byte, 1)
	_, err := io.ReadFull(d.r, b)
	if err != nil {
		return 0, err
	}

	return int(b[0]), nil
}

func (d *decoder) ReadJob() (*job.Job, error) {
	var err error
	j := &job.Job{}

	j.ID, err = d.ReadID()
	if err != nil {
		return nil, err
	}

	j.Name, err = d.ReadName()
	if err != nil {
		return nil, err
	}

	j.TTR, err = d.ReadTTR()
	if err != nil {
		return nil, err
	}

	j.TTL, err = d.ReadTTL()
	if err != nil {
		return nil, err
	}

	j.Priority, err = d.ReadPriority()
	if err != nil {
		return nil, err
	}

	j.MaxAttempts, err = d.ReadMaxAttempts()
	if err != nil {
		return nil, err
	}

	j.MaxFails, err = d.ReadMaxFails()
	if err != nil {
		return nil, err
	}

	j.Payload, err = d.ReadPayload()
	if err != nil {
		return nil, err
	}

	j.Time, err = d.ReadTime()
	if err != nil {
		return nil, err
	}

	j.Created, err = d.ReadTime()
	if err != nil {
		return nil, err
	}

	return j, nil
}

func (d *decoder) ReadID() (job.ID, error) {
	b := make([]byte, 16)
	_, err := io.ReadFull(d.r, b)
	if err != nil {
		return job.ID{}, err
	}

	id := job.ID{}
	copy(id[:], b[:])
	return id, nil
}

func (d *decoder) ReadName() (string, error) {
	l, err := binary.ReadUvarint(d.r)
	if err != nil {
		return "", err
	}

	name := make([]byte, l)
	_, err = io.ReadFull(d.r, name)
	if err != nil {
		return "", err
	}

	return string(name), nil
}

func (d *decoder) ReadTTR() (uint32, error) {
	ttr, err := binary.ReadUvarint(d.r)
	if err != nil {
		return 0, err
	}

	return uint32(ttr), nil
}

func (d *decoder) ReadTTL() (uint64, error) {
	ttl, err := binary.ReadUvarint(d.r)
	if err != nil {
		return 0, err
	}

	return ttl, nil
}

func (d *decoder) ReadPriority() (int32, error) {
	pri, err := binary.ReadVarint(d.r)
	if err != nil {
		return 0, err
	}

	return int32(pri), nil
}

func (d *decoder) ReadMaxAttempts() (uint8, error) {
	max, err := binary.ReadUvarint(d.r)
	if err != nil {
		return 0, err
	}

	return uint8(max), nil
}

func (d *decoder) ReadMaxFails() (uint8, error) {
	max, err := binary.ReadUvarint(d.r)
	if err != nil {
		return 0, err
	}

	return uint8(max), nil
}

func (d *decoder) ReadPayload() ([]byte, error) {
	l, err := binary.ReadUvarint(d.r)
	if err != nil {
		return nil, err
	}

	pay := make([]byte, l)
	_, err = io.ReadFull(d.r, pay)
	if err != nil {
		return nil, err
	}

	return pay, nil
}

func (d *decoder) ReadTime() (time.Time, error) {
	b := make([]byte, 15)
	_, err := io.ReadFull(d.r, b)
	if err != nil {
		return time.Time{}, err
	}

	t := time.Time{}
	err = t.UnmarshalBinary(b)
	if err != nil {
		return time.Time{}, err
	}

	return t, nil
}
