package server

import (
	"bufio"
	"errors"
	"io"
	"testing"
	"time"

	"github.com/iamduo/workq/int/prot"
	"github.com/iamduo/workq/int/testutil"
)

type TestHandler struct {
	b   []byte
	err error
}

func (h *TestHandler) Exec(cmd *prot.Cmd) ([]byte, error) {
	return h.b, h.err
}

type DelayHandler struct {
}

func (h *DelayHandler) Exec(cmd *prot.Cmd) ([]byte, error) {
	time.Sleep(10 * time.Millisecond)
	return nil, nil
}

type TestProt struct {
	parseCmd  func(rdr *bufio.Reader) (*prot.Cmd, error)
	sendReply func(w io.Writer, b []byte) error
	sendErr   func(w io.Writer, errStr string) error
}

func (p *TestProt) ParseCmd(rdr *bufio.Reader) (*prot.Cmd, error) {
	return p.parseCmd(rdr)
}

func (p *TestProt) SendReply(w io.Writer, b []byte) error {
	return p.sendReply(w, b)
}

func (p *TestProt) SendErr(w io.Writer, errStr string) error {
	return p.sendErr(w, errStr)
}

func serverListenAndServe(t *testing.T, s *Server) {
	go func() {
		err := s.ListenAndServe()
		if err != nil {
			t.Fatalf("Unexpected server err=%v", err)
		}
	}()

	time.Sleep(1 * time.Millisecond)
}
func serverStop(s *Server) {
	s.Stop()
	time.Sleep(1 * time.Millisecond)
}

func TestWithCmd(t *testing.T) {
	handlers := make(map[string]Handler)
	handlers["lease"] = &TestHandler{b: []byte("OK\r\n"), err: nil}
	router := &CmdRouter{Handlers: handlers}
	s := New(":8080", router, prot.Prot{}, &Usage{})
	serverListenAndServe(t, s)
	defer serverStop(s)

	c := testutil.NewClient(t)
	c.Exec([]byte("lease 123 1\r\n"), []byte("+OK\r\n"))
}

func TestUnknownCmd(t *testing.T) {
	handlers := make(map[string]Handler)
	handlers["unknown-lease"] = &TestHandler{b: []byte("OK\r\n"), err: nil}
	router := &CmdRouter{Handlers: handlers}
	s := New(":8080", router, prot.Prot{}, &Usage{})
	serverListenAndServe(t, s)

	c := testutil.NewClient(t)
	c.Exec([]byte("unknown-lease 123 1\r\n"), []byte("-CLIENT-ERROR Unknown command\r\n"))
	serverStop(s)
}

func TestInvalidAddr(t *testing.T) {
	handlers := make(map[string]Handler)
	router := &CmdRouter{Handlers: handlers}
	s := New(":80808080", router, prot.Prot{}, &Usage{})
	err := s.ListenAndServe()
	if err == nil {
		t.Fatalf("Start expected to fail")
	}
}

func TestCmdErr(t *testing.T) {
	handlers := make(map[string]Handler)
	handlers["lease"] = &TestHandler{b: nil, err: errors.New("CLIENT-ERROR\r\n")}
	router := &CmdRouter{Handlers: handlers}
	s := New(":8080", router, prot.Prot{}, &Usage{})
	serverListenAndServe(t, s)
	defer serverStop(s)

	c := testutil.NewClient(t)
	c.Exec([]byte("lease 123 1\r\n"), []byte("-CLIENT-ERROR\r\n"))
}

func TestDisconnect(t *testing.T) {
	handlers := make(map[string]Handler)
	handlers["lease"] = &TestHandler{b: nil, err: errors.New("CLIENT-ERROR")}
	router := &CmdRouter{Handlers: handlers}

	p := &TestProt{
		parseCmd: func(rdr *bufio.Reader) (*prot.Cmd, error) {
			return nil, prot.ErrReadErr
		},
		sendReply: func(w io.Writer, b []byte) error {
			return nil
		},
		sendErr: func(w io.Writer, errStr string) error {
			return nil
		},
	}
	s := New(":8080", router, p, &Usage{})
	serverListenAndServe(t, s)
	defer serverStop(s)

	c := testutil.NewClient(t)
	c.Conn().Write([]byte("\r\n"))
	_, err := c.Reader().ReadByte()
	if err == nil {
		t.Fatalf("Expected conn to be closed, err=%v", err)
	}
}

func TestParseCmdSendErrFailure(t *testing.T) {
	handlers := make(map[string]Handler)
	handlers["lease"] = &TestHandler{b: nil, err: errors.New("CLIENT-ERROR")}
	router := &CmdRouter{Handlers: handlers}

	p := &TestProt{
		parseCmd: func(rdr *bufio.Reader) (*prot.Cmd, error) {
			return nil, prot.ErrInvalidCmdArgs
		},
		sendReply: func(w io.Writer, b []byte) error {
			return nil
		},
		sendErr: func(w io.Writer, errStr string) error {
			return errors.New("Unable to write")
		},
	}
	s := New(":8080", router, p, &Usage{})
	serverListenAndServe(t, s)
	defer serverStop(s)

	c := testutil.NewClient(t)
	c.Conn().Write([]byte("\r\n"))
	_, err := c.Reader().ReadByte()
	if err == nil {
		t.Fatalf("Expected conn to be closed, err=%v", err)
	}
}

func TestHandlerSendErrFailure(t *testing.T) {
	handlers := make(map[string]Handler)
	handlers["lease"] = &TestHandler{b: nil, err: errors.New("CLIENT-ERROR")}
	router := &CmdRouter{Handlers: handlers}

	p := &TestProt{
		parseCmd: func(rdr *bufio.Reader) (*prot.Cmd, error) {
			return &prot.Cmd{Name: "lease"}, nil
		},
		sendReply: func(w io.Writer, b []byte) error {
			return nil
		},
		sendErr: func(w io.Writer, errStr string) error {
			return errors.New("Unable to write")
		},
	}
	s := New(":8080", router, p, &Usage{})
	serverListenAndServe(t, s)
	defer serverStop(s)

	c := testutil.NewClient(t)
	c.Conn().Write([]byte("lease\r\n"))
	_, err := c.Reader().ReadByte()
	if err == nil {
		t.Fatalf("Expected conn to be closed, err=%v", err)
	}
}

func TestHandlerSendReplyFailure(t *testing.T) {
	handlers := make(map[string]Handler)
	handlers["lease"] = &TestHandler{b: []byte("OK"), err: nil}
	router := &CmdRouter{Handlers: handlers}

	p := &TestProt{
		parseCmd: func(rdr *bufio.Reader) (*prot.Cmd, error) {
			return &prot.Cmd{Name: "lease"}, nil
		},
		sendReply: func(w io.Writer, b []byte) error {
			return errors.New("Unable to write")
		},
		sendErr: func(w io.Writer, errStr string) error {
			return nil
		},
	}
	s := New(":8080", router, p, &Usage{})
	serverListenAndServe(t, s)
	defer serverStop(s)

	c := testutil.NewClient(t)
	c.Conn().Write([]byte("lease\r\n"))
	_, err := c.Reader().ReadByte()
	if err == nil {
		t.Fatalf("Expected conn to be closed, err=%v", err)
	}
}
