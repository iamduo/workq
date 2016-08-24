package server

import (
	"net"
	"sync/atomic"
	"time"

	"github.com/iamduo/workq/int/client"
	"github.com/iamduo/workq/int/prot"
)

// Handler interface for a Command
// Handlers are responsible for executing a command
type Handler interface {
	Exec(cmd *prot.Cmd) ([]byte, error)
}

// Command Router takes in a command name and returns a handler
type Router interface {
	Handler(cmd string) Handler
}

// Command Router Implementation
type CmdRouter struct {
	Handlers       map[string]Handler
	UnknownHandler Handler
}

// Handler by command name
func (c *CmdRouter) Handler(cmd string) Handler {
	if h, ok := c.Handlers[cmd]; ok {
		return h
	}

	return c.UnknownHandler
}

// Workq Server listens on a TCP Address
// Requires a Command Router and a Protocol Implementation
type Server struct {
	Addr   string // Network Address to listen on
	Router Router
	Prot   prot.Interface
	Usage  *Usage
	ln     net.Listener
	stop   chan struct{}
}

// New returns a initialized, but unstarted  Server
func New(addr string, router Router, protocol prot.Interface, usage *Usage) *Server {
	return &Server{
		Addr:   addr,
		Router: router,
		Prot:   protocol,
		Usage:  usage,
		stop:   make(chan struct{}, 1),
	}
}

// Start a Workq Server, listening on the specified TCP address
func (s *Server) ListenAndServe() error {
	var err error
	s.ln, err = net.Listen("tcp", s.Addr)
	if err != nil {
		return err
	}
	s.Usage.Started = time.Now().UTC()

	for {
		select {
		case <-s.stop:
			return nil
		default:
			conn, err := s.ln.Accept()
			if err != nil {
				continue
			}
			// TODO: check for Accept errors
			c := client.New(conn.(*net.TCPConn), prot.MaxRead)
			go s.clientLoop(c)
		}
	}
}

// Stops listening while maintaining all active connections
func (s *Server) Stop() error {
	s.stop <- struct{}{}
	return s.ln.Close()
}

func (s *Server) clientLoop(c *client.Client) {
	atomic.AddUint64(&s.Usage.ActiveClients, 1)
	defer atomic.AddUint64(&s.Usage.ActiveClients, ^uint64(0))

	rdr := c.Reader()
	wrt := c.Writer()
	cls := c.Closer()

	for {
		c.ResetLimit()
		cmd, err := s.Prot.ParseCmd(rdr)
		if err != nil {
			// Client Conn Error, Fail Fast
			if err == prot.ErrReadErr {
				cls.Close()
				return
			}

			err = s.Prot.SendErr(wrt, err.Error())
			if err != nil {
				cls.Close()
				return
			}

			continue
		}

		handler := s.Router.Handler(cmd.Name)
		reply, err := handler.Exec(cmd)
		switch {
		case err != nil:
			err = s.Prot.SendErr(wrt, err.Error())
			if err != nil {
				cls.Close()
				return
			}
		default:
			err = s.Prot.SendReply(wrt, reply)
			if err != nil {
				cls.Close()
				return
			}
		}
	}
}

// Usage data
type Usage struct {
	ActiveClients uint64
	Started       time.Time
}
