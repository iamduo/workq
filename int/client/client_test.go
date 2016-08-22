package client

import (
	"bytes"
	"io"
	"testing"
	"time"
)

type testCb func(interface{})
type TestConn struct {
	buf     *bytes.Buffer
	asserts map[string]testCb
}

func (c *TestConn) Read(b []byte) (int, error) {
	return c.buf.Read(b)
}

func (c *TestConn) Write(b []byte) (int, error) {
	return c.buf.Write(b)
}

func (c *TestConn) Close() error {
	return nil
}

func (c *TestConn) SetKeepAlive(keepalive bool) error {
	c.asserts["SetKeepAlive"](keepalive)
	return nil
}

func (c *TestConn) SetKeepAlivePeriod(d time.Duration) error {
	c.asserts["SetKeepAlivePeriod"](d)
	return nil
}

func TestClient(t *testing.T) {
	actCalls := []bool{false, false}
	asserts := make(map[string]testCb)
	asserts["SetKeepAlive"] = func(arg interface{}) {
		actCalls[0] = true
		keepalive := arg.(bool)
		if !keepalive {
			t.FailNow()
		}
	}
	asserts["SetKeepAlivePeriod"] = func(arg interface{}) {
		actCalls[1] = true
		d := arg.(time.Duration)
		if time.Duration(KeepAlivePeriod) != d {
			t.FailNow()
		}
	}
	buf := bytes.NewBuffer([]byte("12"))
	conn := &TestConn{
		asserts: asserts,
		buf:     buf,
	}
	var limitN int64 = 1
	c := New(conn, limitN)

	if actCalls[0] != true || actCalls[1] != true {
		t.Fatalf("Calls mismatch")
	}

	rdr := c.Reader()
	p := make([]byte, limitN)
	_, err := rdr.Read(p)
	if err != nil || !bytes.Equal(p, []byte("1")) {
		t.Fatalf("Read mismatch, err=%v, read=%v", err, p)
	}

	// Read should fail as we are > limitN
	p = make([]byte, limitN)
	n, err := rdr.Read(p)
	if n != 0 || err != io.EOF || !bytes.Equal(p, []byte{0}) {
		t.Fatalf("Read limit mismatch, err=%v, read=%+v", err, p)
	}

	c.ResetLimit()
	_, err = rdr.Read(p)
	if err != nil || !bytes.Equal(p, []byte("2")) {
		t.Fatalf("Read mismatch, err=%v, read=%v", err, p)
	}

	wtr := c.Writer()
	wtr.Write([]byte("4"))
	p = make([]byte, 1)
	_, err = buf.Read(p)
	if err != nil || !bytes.Equal(p, []byte("4")) {
		t.Fatalf("Read mismatch, err=%v, read=%v", err, p)
	}

	cls := c.Closer()
	if cls.Close() != nil {
		t.Fatalf("Close mismatch")
	}
}
