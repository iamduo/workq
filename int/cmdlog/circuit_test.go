package cmdlog

import (
	"errors"
	"testing"

	"github.com/iamduo/workq/int/prot"
	"github.com/iamduo/workq/int/server"
)

type testHandler struct{}

func (h *testHandler) Exec(cmd *prot.Cmd) ([]byte, error) {
	return nil, nil
}

func TestCircuitBreakerRouter(t *testing.T) {
	breaker := &CircuitBreaker{}
	var router server.Router
	router = &server.CmdRouter{
		Handlers: map[string]server.Handler{
			prot.CmdAdd: &testHandler{},
		},
	}
	router = NewCircuitBreakerRouter(breaker, router)

	h := router.Handler(prot.CmdAdd)
	_, ok := h.(*testHandler)
	if !ok {
		t.Fatalf("got=%T, exp=testHandler", h)
	}

	breaker.Open()
	h = router.Handler(prot.CmdAdd)
	_, ok = h.(*CircuitBreakerHandler)
	if !ok {
		t.Fatalf("got=%T, exp=CircuitBreakerHandler", h)
	}

	breaker.Close()
	h = router.Handler(prot.CmdAdd)
	_, ok = h.(*testHandler)
	if !ok {
		t.Fatalf("got=%T, exp=testHandler", h)
	}
}

func TestCircuitBreakerHandler(t *testing.T) {
	h := &CircuitBreakerHandler{}
	_, err := h.Exec(&prot.Cmd{})
	if err != errReadOnly {
		t.Fatalf("got=%v, exp=errReadOnly", err)
	}
}

type testCircuitAppender struct {
	cb func(b []byte) error
}

func (a *testCircuitAppender) Append(b []byte) error {
	return a.cb(b)
}

func TestCircuitBreakerAppender(t *testing.T) {
	breaker := &CircuitBreaker{}
	expErr := errors.New("test")
	var i int
	app := &testCircuitAppender{
		cb: func(b []byte) error {
			i++
			if i == 3 {
				return nil
			}

			return expErr
		},
	}
	breakerApp := NewCircuitBreakerAppender(breaker, app)
	err := breakerApp.Append([]byte("test"))
	if err != nil {
		t.Fatalf("Append err got=%v, want=nil", err)
	}

	if i != 3 {
		t.Fatalf("Attempts got=%d, want=3", i)
	}
}
