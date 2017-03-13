package testutil

import (
	"bufio"
	"fmt"
	"net"
	"os/exec"
	"path/filepath"
	"testing"
	"time"

	"github.com/satori/go.uuid"
)

// Various test utilities.
// This will always be in an unstable state.
// Changing quickly to adapt to test needs.
// FYI, There will be a replacement to this at a later time.

func GenID() uuid.UUID {
	return uuid.NewV4()
}

func GenIDString() string {
	return uuid.NewV4().String()
}

func GenName() string {
	return fmt.Sprintf("hi-%s", GenID())
}

func StartServer() *exec.Cmd {
	serverCmd, _ := filepath.Abs("../workq-server")
	cmd := exec.Command(serverCmd)
	err := cmd.Start()
	if err != nil {
		fmt.Print("Unable to start workq-server during cleanup")
	}

	time.Sleep(100 * time.Millisecond)
	return cmd
}

type Client struct {
	conn *net.TCPConn
	rdr  *bufio.Reader
	t    *testing.T
}

func NewClient(t *testing.T) *Client {
	c := &Client{
		t: t,
	}
	c.initConn()
	c.initReader()
	return c
}

func (c *Client) initConn() {
	var err error
	addr, _ := net.ResolveTCPAddr("tcp", ":8080")
	c.conn, err = net.DialTCP("tcp", nil, addr)
	if err != nil {
		c.t.Errorf("Connect: err=%s", err)
	}
}

func (c *Client) initReader() {
	c.rdr = bufio.NewReader(c.conn)
}

func (c *Client) Conn() *net.TCPConn {
	return c.conn
}

func (c *Client) Reader() *bufio.Reader {
	return c.rdr
}

func (c *Client) Exec(cmd []byte, expResp []byte) {
	_, err := c.conn.Write(cmd)
	if err != nil {
		c.t.Errorf("Exec failed: err=%v", err)
	}

	resp := make([]byte, len(expResp))
	_, err = c.rdr.Read(resp)

	if string(expResp) != string(resp) || err != nil {
		c.t.Errorf(
			"Response mismatch: expResp=%q, expLen=%d, actResp=%q, "+
				"actLen=%d, err=%v, fromCmd=%s",
			string(expResp),
			len(expResp),
			string(resp),
			len(resp),
			err,
			cmd,
		)
	}

	// There should be no trailing garbage
	bufN := c.rdr.Buffered()
	if bufN > 0 {
		b := make([]byte, bufN)
		c.rdr.Read(b)
		c.t.Errorf("Trailing garbage: len=%d, garbage=%v", bufN, b)
	}
}

func Connect(t *testing.T) net.Conn {
	conn, err := net.Dial("tcp", ":8080")
	if err != nil {
		t.Errorf("Connect: err=%s", err)
	}

	return conn
}
