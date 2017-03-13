package cmdlog

import (
	"sync"
	"sync/atomic"
	"time"

	"github.com/iamduo/workq/int/prot"
	"github.com/iamduo/workq/int/server"
)

const (
	stateClosed = uint32(0)
	stateOpen   = uint32(1)
)

// CircuitBreaker provides an atomic Open/Close breaker.
type CircuitBreaker struct {
	state uint32
}

// Open sets the breaker open atomically.
func (c *CircuitBreaker) Open() {
	atomic.StoreUint32(&c.state, stateOpen)
}

// Close sets the breaker closed aotmically.
func (c *CircuitBreaker) Close() {
	atomic.StoreUint32(&c.state, stateClosed)
}

// State returns the current state atomically.
func (c *CircuitBreaker) State() uint32 {
	return atomic.LoadUint32(&c.state)
}

// CircuitBreakerAppender wraps and proxies an Appender with a CircuitBreaker.
// If an append fails, the CircuitBreaker is open, and the failed append will
// be retried in a recovery loop forever. Any further Appends will be block until
// the failed one has succeeded.
type CircuitBreakerAppender struct {
	breaker *CircuitBreaker
	app     appender
	mu      sync.Mutex
}

func NewCircuitBreakerAppender(breaker *CircuitBreaker, app appender) *CircuitBreakerAppender {
	return &CircuitBreakerAppender{
		breaker: breaker,
		app:     app,
	}
}

func (c *CircuitBreakerAppender) Append(b []byte) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	err := c.app.Append(b)
	if err != nil {
		c.breaker.Open()
		c.recoverAppend(b)
	}

	return nil
}

func (c *CircuitBreakerAppender) recoverAppend(b []byte) {
	var err error
	for {
		time.Sleep(1 * time.Second)

		err = c.app.Append(b)
		if err == nil {
			break
		}
	}
}

// CircuitBreakerRouter proxies an existing Server.Router with respect for
// the breaker's state. When open, all commands respond with a SERVER-ERROR
// notifying clients that the server is in read-only mode.
// When closed, all commands are processed normally through the proxied router.
type CircuitBreakerRouter struct {
	breaker *CircuitBreaker
	handler server.Handler
	router  server.Router
}

func NewCircuitBreakerRouter(breaker *CircuitBreaker, r server.Router) *CircuitBreakerRouter {
	return &CircuitBreakerRouter{
		breaker: breaker,
		handler: &CircuitBreakerHandler{},
		router:  r,
	}
}

// Handler by command name
func (c *CircuitBreakerRouter) Handler(cmd string) server.Handler {
	if c.breaker.State() == stateOpen {
		switch cmd {
		case prot.CmdAdd:
			fallthrough
		case prot.CmdRun:
			fallthrough
		case prot.CmdSchedule:
			fallthrough
		case prot.CmdLease:
			fallthrough
		case prot.CmdComplete:
			fallthrough
		case prot.CmdFail:
			fallthrough
		case prot.CmdDelete:
			return c.handler
		}
	}

	return c.router.Handler(cmd)
}

var (
	errReadOnly = prot.NewServerErr("Read-only mode triggered by cmdlog circuit breaker")
)

// CircuitBreakerHandler implements a Handler that responds with a SERVER-ERROR
// that states the server is in read-only mode.
type CircuitBreakerHandler struct{}

func (h *CircuitBreakerHandler) Exec(cmd *prot.Cmd) ([]byte, error) {
	return nil, errReadOnly
}
