package cmdlog

import (
	"bytes"
	"encoding/hex"
	"reflect"
	"testing"
	"time"

	"github.com/iamduo/workq/int/job"
	"github.com/iamduo/workq/int/testutil"
)

type testAppender struct {
	expBytes []byte
	t        *testing.T
	invoked  bool
}

func (a *testAppender) Append(b []byte) error {
	if !bytes.Equal(a.expBytes, b) {
		a.t.Fatalf("Append mismatch, exp=%q, got=%q", a.expBytes, b)
	}
	a.invoked = true
	return nil
}

func (a *testAppender) AssertInvoked() {
	if !a.invoked {
		a.t.Fatalf("Appender not invoked")
	}
}

type controllerMock struct {
	addMethod            func(*job.Job) error
	scheduleMethod       func(j *job.Job) error
	runMethod            func(j *job.Job, timeout uint32) (*job.RunResult, error)
	completeMethod       func(id job.ID, result []byte) error
	failMethod           func(id job.ID, result []byte) error
	deleteMethod         func(id job.ID) error
	expireMethod         func(job.ID)
	leaseMethod          func(names []string, timeout uint32) (*job.Job, error)
	startAttemptMethod   func(id job.ID) error
	timeoutAttemptMethod func(id job.ID)
}

func (c *controllerMock) Add(j *job.Job) error {
	return c.addMethod(j)
}

func (c *controllerMock) Expire(id job.ID) {
	c.expireMethod(id)
}

func (c *controllerMock) HandleExpire(f func(job.ID)) {
	c.expireMethod = f
}

func (c *controllerMock) ExpireFunc() func(job.ID) {
	return c.expireMethod
}

func (c *controllerMock) Complete(id job.ID, result []byte) error {
	return c.completeMethod(id, result)
}

func (c *controllerMock) Fail(id job.ID, result []byte) error {
	return c.failMethod(id, result)
}

func (c *controllerMock) Delete(id job.ID) error {
	return c.deleteMethod(id)
}

func (c *controllerMock) Lease(names []string, timeout uint32) (*job.Job, error) {
	return c.leaseMethod(names, timeout)
}

func (c *controllerMock) StartAttempt(id job.ID) error {
	return c.startAttemptMethod(id)
}

func (c *controllerMock) TimeoutAttempt(id job.ID) {
	c.timeoutAttemptMethod(id)
}

func (c *controllerMock) HandleTimeoutAttempt(f func(job.ID)) {
	c.timeoutAttemptMethod = f
}

func (c *controllerMock) TimeoutAttemptFunc() func(job.ID) {
	return c.timeoutAttemptMethod
}

func (c *controllerMock) Run(j *job.Job, timeout uint32) (*job.RunResult, error) {
	return c.runMethod(j, timeout)
}

func (c *controllerMock) Schedule(j *job.Job) error {
	return c.scheduleMethod(j)
}

func TestAdd(t *testing.T) {
	j := &job.Job{}
	j.ID = [16]byte{0x61, 0xa4, 0x44, 0xa0, 0x61, 0x28, 0x41, 0xc0, 0x80, 0x78, 0xcc, 0x75, 0x7d, 0x3b, 0xd2, 0xd8}
	j.Name = "ping"
	j.TTR = 1000
	j.TTL = 60000
	j.Priority = 10
	j.MaxAttempts = 3
	j.MaxFails = 1
	j.Payload = []byte("a")
	j.Created = time.Date(2016, time.December, 1, 10, 0, 0, 0, time.UTC)

	expBytes, _ := hex.DecodeString("0161a444a0612841c08078cc757d3bd2d80470696e67e807e0d403140301016101000000000000000000000000ffff010000000ecfd1eba000000000ffff")
	app := &testAppender{t: t, expBytes: expBytes}
	controller := &controllerMock{
		addMethod: func(proxyJ *job.Job) error {
			if j != proxyJ {
				t.Fatalf("Add mismatch, exp=%+v, got=%+v", j, proxyJ)
			}

			return nil
		},
	}
	proxy := NewControllerProxy(app, controller)

	err := proxy.Add(j)
	if err != nil {
		t.Fatalf("Add mismatch, err=%s", err)
	}

	app.AssertInvoked()
}

func TestSchedule(t *testing.T) {
	j := &job.Job{}
	j.ID = [16]byte{0x61, 0xa4, 0x44, 0xa0, 0x61, 0x28, 0x41, 0xc0, 0x80, 0x78, 0xcc, 0x75, 0x7d, 0x3b, 0xd2, 0xd8}
	j.Name = "ping"
	j.TTR = 1000
	j.TTL = 60000
	j.Priority = 10
	j.MaxAttempts = 3
	j.MaxFails = 1
	j.Payload = []byte("a")
	j.Time = time.Date(2016, time.December, 31, 0, 0, 0, 0, time.UTC)
	j.Created = time.Date(2016, time.December, 1, 10, 0, 0, 0, time.UTC)

	expBytes, _ := hex.DecodeString("0261a444a0612841c08078cc757d3bd2d80470696e67e807e0d4031403010161010000000ecff8ec0000000000ffff010000000ecfd1eba000000000ffff")
	app := &testAppender{t: t, expBytes: expBytes}
	controller := &controllerMock{
		scheduleMethod: func(proxyJ *job.Job) error {
			if j != proxyJ {
				t.Fatalf("Schedule proxy mismatch, exp=%+v, got=%+v", j, proxyJ)
			}

			return nil
		},
	}

	proxy := NewControllerProxy(app, controller)
	err := proxy.Schedule(j)
	if err != nil {
		t.Fatalf("Schedule mismatch, err=%s", err)
	}

	app.AssertInvoked()
}

func TestRun(t *testing.T) {
	j := job.NewEmptyJob()
	timeout := uint32(1000)
	expResult := &job.RunResult{Result: []byte("pong")}
	controller := &controllerMock{
		runMethod: func(proxyJ *job.Job, proxyTimeout uint32) (*job.RunResult, error) {
			if j != proxyJ || timeout != proxyTimeout {
				t.Fatalf("Add proxy mismatch")
			}

			return expResult, nil
		},
	}
	proxy := NewControllerProxy(&testAppender{}, controller)
	result, err := proxy.Run(j, timeout)
	if expResult != result || err != nil {
		t.Fatalf("Add mismatch, result=%+v, err=%s", result, err)
	}
}

func TestComplete(t *testing.T) {
	id := [16]byte{0x61, 0xa4, 0x44, 0xa0, 0x61, 0x28, 0x41, 0xc0, 0x80, 0x78, 0xcc, 0x75, 0x7d, 0x3b, 0xd2, 0xd8}
	result := []byte("a")

	encoded, _ := hex.DecodeString("0361a444a0612841c08078cc757d3bd2d80161")
	app := &testAppender{t: t, expBytes: encoded}
	controller := &controllerMock{
		completeMethod: func(proxyID job.ID, proxyResult []byte) error {
			if id != proxyID || !bytes.Equal(result, proxyResult) {
				t.Fatalf("Complete mismatch, id=%x, result=%x", proxyID, proxyResult)
			}

			return nil
		},
	}
	proxy := NewControllerProxy(app, controller)
	err := proxy.Complete(id, result)
	if err != nil {
		t.Fatalf("Complete mismatch, err=%s", err)
	}

	app.AssertInvoked()
}

func TestFail(t *testing.T) {
	id := [16]byte{0x61, 0xa4, 0x44, 0xa0, 0x61, 0x28, 0x41, 0xc0, 0x80, 0x78, 0xcc, 0x75, 0x7d, 0x3b, 0xd2, 0xd8}
	result := []byte("a")
	expBytes, _ := hex.DecodeString("0461a444a0612841c08078cc757d3bd2d80161")
	app := &testAppender{t: t, expBytes: expBytes}
	controller := &controllerMock{
		failMethod: func(proxyID job.ID, proxyResult []byte) error {
			if id != proxyID || !bytes.Equal(result, proxyResult) {
				t.Fatalf("Fail mismatch, id=%x, result=%x", proxyID, proxyResult)
			}

			return nil
		},
	}
	proxy := NewControllerProxy(app, controller)
	err := proxy.Fail(id, result)
	if err != nil {
		t.Fatalf("Fail mismatch, err=%s", err)
	}

	app.AssertInvoked()
}

func TestDelete(t *testing.T) {
	id := [16]byte{0x61, 0xa4, 0x44, 0xa0, 0x61, 0x28, 0x41, 0xc0, 0x80, 0x78, 0xcc, 0x75, 0x7d, 0x3b, 0xd2, 0xd8}
	expBytes, _ := hex.DecodeString("0561a444a0612841c08078cc757d3bd2d8")
	app := &testAppender{t: t, expBytes: expBytes}
	controller := &controllerMock{
		deleteMethod: func(proxyID job.ID) error {
			if id != proxyID {
				t.Fatalf("Delete mismatch, exp=%s, got=%s", id, proxyID)
			}

			return nil
		},
	}
	proxy := NewControllerProxy(app, controller)
	err := proxy.Delete(id)
	if err != nil {
		t.Fatalf("Delete mismatch, err=%s", err)
	}

	app.AssertInvoked()
}

func TestLease(t *testing.T) {
	names := []string{"ping1", "ping2", "ping3"}
	timeout := uint32(1000)
	expID := job.ID([16]byte{0x61, 0xa4, 0x44, 0xa0, 0x61, 0x28, 0x41, 0xc0, 0x80, 0x78, 0xcc, 0x75, 0x7d, 0x3b, 0xd2, 0xd8})
	expBytes := []byte{0x7, 0x61, 0xa4, 0x44, 0xa0, 0x61, 0x28, 0x41, 0xc0, 0x80, 0x78, 0xcc, 0x75, 0x7d, 0x3b, 0xd2, 0xd8}
	app := &testAppender{t: t, expBytes: expBytes}
	controller := &controllerMock{
		leaseMethod: func(proxyNames []string, proxyTimeout uint32) (*job.Job, error) {
			if !reflect.DeepEqual(names, proxyNames) || timeout != proxyTimeout {
				t.Fatalf("Lease proxy mismatch")
			}
			return &job.Job{ID: expID, Name: names[1]}, nil
		},
	}
	proxy := NewControllerProxy(app, controller)
	j, err := proxy.Lease(names, timeout)
	if err != nil {
		t.Fatalf("Lease mismatch, exp=%x, got=%x, err=%s", expID, j.ID, err)
	}

	app.AssertInvoked()
}

func TestStartAttempt(t *testing.T) {
	var expErr error
	expID := job.ID(testutil.GenID())
	invoked := false
	controller := &controllerMock{
		startAttemptMethod: func(id job.ID) error {
			invoked = true
			if id != expID {
				t.Fatalf("got=%v, want=%v", id, expID)
			}

			return expErr
		},
	}
	proxy := NewControllerProxy(&testAppender{}, controller)
	err := proxy.StartAttempt(expID)
	if err != expErr {
		t.Fatalf("got=%s, want=%s", err, expErr)
	}

	if !invoked {
		t.Fatalf("Expected invocation")
	}
}

func TestExpire(t *testing.T) {
	id := [16]byte{0x61, 0xa4, 0x44, 0xa0, 0x61, 0x28, 0x41, 0xc0, 0x80, 0x78, 0xcc, 0x75, 0x7d, 0x3b, 0xd2, 0xd8}
	expBytes, _ := hex.DecodeString("0661a444a0612841c08078cc757d3bd2d8")
	app := &testAppender{t: t, expBytes: expBytes}
	invoked := false
	controller := &controllerMock{
		expireMethod: func(proxyID job.ID) {
			if id != proxyID {
				t.Fatalf("Expire mismatch, exp=%s, got=%s", id, proxyID)
			}
			invoked = true
		},
	}
	proxy := NewControllerProxy(app, controller)
	proxy.Expire(id)
	if !invoked {
		t.Fatalf("Expected invocation")
	}

	app.AssertInvoked()
}

// Test to ensure Expire calls from origin will invoke the Proxy when wrapped.
// Covers cases when timer calls are invoked from origin controller.
func TestExpireProxyWrapper(t *testing.T) {
	id := [16]byte{0x61, 0xa4, 0x44, 0xa0, 0x61, 0x28, 0x41, 0xc0, 0x80, 0x78, 0xcc, 0x75, 0x7d, 0x3b, 0xd2, 0xd8}
	expBytes, _ := hex.DecodeString("0661a444a0612841c08078cc757d3bd2d8")
	app := &testAppender{t: t, expBytes: expBytes}
	invoked := false
	controller := &controllerMock{
		expireMethod: func(proxyID job.ID) {
			if id != proxyID {
				t.Fatalf("Expire mismatch, exp=%s, got=%s", id, proxyID)
			}
			invoked = true
		},
	}
	NewControllerProxy(app, controller)

	// Calling origin
	controller.Expire(id)
	if !invoked {
		t.Fatalf("Expected invocation")
	}

	app.AssertInvoked()
}

func TestTimeoutAttempt(t *testing.T) {
	id := [16]byte{0x61, 0xa4, 0x44, 0xa0, 0x61, 0x28, 0x41, 0xc0, 0x80, 0x78, 0xcc, 0x75, 0x7d, 0x3b, 0xd2, 0xd8}
	expBytes, _ := hex.DecodeString("0861a444a0612841c08078cc757d3bd2d8")
	app := &testAppender{t: t, expBytes: expBytes}
	invoked := false
	controller := &controllerMock{
		timeoutAttemptMethod: func(proxyID job.ID) {
			if id != proxyID {
				t.Fatalf("TimeoutAttempt mismatch, exp=%s, got=%s", id, proxyID)
			}
			invoked = true
		},
	}
	proxy := NewControllerProxy(app, controller)
	proxy.TimeoutAttempt(id)
	if !invoked {
		t.Fatalf("Expected invocation")
	}

	app.AssertInvoked()
}

// Test to ensure TimeoutAttempt() calls from origin will invoke the Proxy when wrapped.
// Covers cases when timer calls are invoked from origin controller.
func TestTimeoutAttemptProxyWrapper(t *testing.T) {
	id := [16]byte{0x61, 0xa4, 0x44, 0xa0, 0x61, 0x28, 0x41, 0xc0, 0x80, 0x78, 0xcc, 0x75, 0x7d, 0x3b, 0xd2, 0xd8}
	expBytes, _ := hex.DecodeString("0861a444a0612841c08078cc757d3bd2d8")
	app := &testAppender{t: t, expBytes: expBytes}
	invoked := false
	controller := &controllerMock{
		timeoutAttemptMethod: func(proxyID job.ID) {
			if id != proxyID {
				t.Fatalf("TimeoutAttempt mismatch, exp=%s, got=%s", id, proxyID)
			}
			invoked = true
		},
	}
	NewControllerProxy(app, controller)

	controller.TimeoutAttempt(id)
	if !invoked {
		t.Fatalf("Expected invocation")
	}

	app.AssertInvoked()
}

func TestDecoderReadType(t *testing.T) {
	dec := newDecoder([]byte{0x3})
	tp, err := dec.ReadType()
	if err != nil || tp != 3 {
		t.Fatalf("Decode type mismatch, type=%d, err=%s", tp, err)
	}
}

func TestDecoderReadJob(t *testing.T) {
	expJ := &job.Job{}
	expJ.ID = [16]byte{0x61, 0xa4, 0x44, 0xa0, 0x61, 0x28, 0x41, 0xc0, 0x80, 0x78, 0xcc, 0x75, 0x7d, 0x3b, 0xd2, 0xd8}
	expJ.Name = "ping"
	expJ.TTR = 1000
	expJ.TTL = 60000
	expJ.Priority = 10
	expJ.MaxAttempts = 3
	expJ.MaxFails = 1
	expJ.Payload = []byte("a")
	expJ.Created = time.Date(2016, time.December, 1, 10, 0, 0, 0, time.UTC)
	encoded, _ := hex.DecodeString("61a444a0612841c08078cc757d3bd2d80470696e67e807e0d403140301016101000000000000000000000000ffff010000000ecfd1eba000000000ffff")

	decoder := newDecoder(encoded)
	j, err := decoder.ReadJob()
	if err != nil || !compareJobs(expJ, j) {
		t.Fatalf("Decoded job mismatch, exp=%+v, got=%+v, err=%s", expJ, j, err)
	}
}

func TestDecoderReadJobInvalidInput(t *testing.T) {
	buf, _ := hex.DecodeString("61a444a0612841c08078cc757d3bd2d80470696e67e807e0d403140301016101000000000000000000000000ffff010000000ecfd1eba000000000ffff")
	var tests [][]byte
	// Chop a single byte off on every test.
	for i := 1; i < len(buf); i++ {
		tests = append(tests, buf[0:len(buf)-1-i])
	}

	for i, b := range tests {
		decoder := newDecoder(b)
		_, err := decoder.ReadJob()
		if err == nil {
			t.Fatalf("got=nil, want=err, i=%d", i)
		}
	}
}

func TestDecoderReadID(t *testing.T) {
	expID := []byte{0x61, 0xa4, 0x44, 0xa0, 0x61, 0x28, 0x41, 0xc0, 0x80, 0x78, 0xcc, 0x75, 0x7d, 0x3b, 0xd2, 0xd8}
	dec := newDecoder(expID)
	id, err := dec.ReadID()
	if err != nil || !bytes.Equal(expID, id[:]) {
		t.Fatalf("Decode ID mismatch, exp=%x, got=%x, err=%s", expID, id, err)
	}
}

func TestDecoderReadName(t *testing.T) {
	expName := "ping"
	expBytes := []byte{0x4, 0x70, 0x69, 0x6e, 0x67}
	dec := newDecoder(expBytes)
	name, err := dec.ReadName()
	if err != nil || expName != name {
		t.Fatalf("Decode name mismatch, exp=%s, got=%s, err=%s", expName, name, err)
	}
}

func TestDecoderReadPayload(t *testing.T) {
	expPay := []byte("pay")
	dec := newDecoder([]byte{0x3, 0x70, 0x61, 0x79})
	pay, err := dec.ReadPayload()
	if err != nil || !bytes.Equal(expPay, pay) {
		t.Fatalf("Decode payload mismatch payload=%s, err=%s", pay, err)
	}
}

func compareJobs(a *job.Job, b *job.Job) bool {
	return a.ID == b.ID &&
		a.Name == b.Name &&
		bytes.Equal(a.Payload, b.Payload) &&
		a.Priority == b.Priority &&
		a.MaxAttempts == b.MaxAttempts &&
		a.MaxFails == b.MaxFails &&
		a.TTR == b.TTR &&
		a.TTL == b.TTL &&
		a.Time.Equal(b.Time) &&
		a.Created.Equal(b.Created)
}
