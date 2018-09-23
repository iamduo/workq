package client

import (
	"bufio"
	"io"
	"time"

	"github.com/satori/go.uuid"
)

var (
	KeepAlivePeriod = 300
)

// Conn interface to clarify actual behavior required
// and for easier integration testing.
type Conn interface {
	io.Reader
	io.Writer
	io.Closer
	SetKeepAlive(keepalive bool) error
	SetKeepAlivePeriod(d time.Duration) error
}

// Client represents a single unique connection with convenience methods to
// return a Limited bufio.Reader, Writer, and Closer.
// Limited Reader provides a circuit breaker for out of bound requests.
type Client struct {
	id       uuid.UUID // UUIDv4
	conn     Conn
	rdr      *bufio.Reader
	writer   io.Writer
	limitRdr *io.LimitedReader
	limitN   int64 // Limit n bytes to Readers
}

func New(conn Conn, n int64) *Client {
	limitRdr := &io.LimitedReader{R: conn, N: n}
	c := &Client{
		id:       uuid.Must(uuid.NewV4()),
		conn:     conn,
		rdr:      bufio.NewReader(limitRdr),
		limitRdr: limitRdr,
		limitN:   n,
	}

	conn.SetKeepAlive(true)
	conn.SetKeepAlivePeriod(time.Duration(KeepAlivePeriod))

	return c
}

// Reset the read limit back to original value.
// It is expected to be reset for every new cmd or read "session".
func (c *Client) ResetLimit() {
	c.limitRdr.N = c.limitN
}

// Return Limited Bufio Reader
// Callers should invoke ResetLimit to reset read limits to original value.
func (c *Client) Reader() *bufio.Reader {
	return c.rdr
}

// Return Writer
func (c *Client) Writer() io.Writer {
	return c.conn
}

// Return Closer
func (c *Client) Closer() io.Closer {
	return c.conn
}
