package handlers

import (
	"bytes"
	"fmt"
	"testing"

	"github.com/iamduo/workq/int/job"
	"github.com/iamduo/workq/int/prot"
	"github.com/iamduo/workq/int/testutil"
)

type leaseControllerMock struct {
	leaseMethod          func(names []string, timeout uint32) (*job.Job, error)
	timeoutAttemptMethod func(id job.ID)
}

func (c *leaseControllerMock) Lease(names []string, timeout uint32) (*job.Job, error) {
	return c.leaseMethod(names, timeout)
}

func (c *leaseControllerMock) StartAttempt(id job.ID) error {
	// NO-OP
	return nil
}

func (c *leaseControllerMock) TimeoutAttempt(id job.ID) {
	// NO-OP
}

func (c *leaseControllerMock) HandleTimeoutAttempt(f func(job.ID)) {
	// NO-OP
}

func (c *leaseControllerMock) TimeoutAttemptFunc() func(job.ID) {
	return c.timeoutAttemptMethod
}

func TestLeaseDefault(t *testing.T) {
	j := job.NewEmptyJob()
	j.ID = job.ID(testutil.GenID())
	j.Name = testutil.GenName()
	j.Payload = []byte("a")

	jc := &leaseControllerMock{
		leaseMethod: func(names []string, timeout uint32) (*job.Job, error) {
			return j, nil
		},
	}
	handler := NewLeaseHandler(jc)

	cmd := prot.NewCmd(
		"lease",
		[][]byte{
			[]byte(j.Name),
			[]byte("1"),
		},
		prot.CmdFlags{},
	)

	expResp := []byte(fmt.Sprintf(
		"OK 1\r\n%s %s %d\r\n%s\r\n",
		j.ID.String(),
		j.Name,
		len(j.Payload),
		j.Payload,
	))
	resp, err := handler.Exec(cmd)
	if err != nil {
		t.Fatalf("Add err=%v", err)
	}

	if !bytes.Equal(resp, expResp) {
		t.Fatalf("Add response mismatch, resp=%s", resp)
	}
}

func TestLeaseErrorResponses(t *testing.T) {
	tests := []struct {
		expErr    error
		clientErr error
	}{
		{
			expErr:    job.ErrTimeout,
			clientErr: prot.ErrTimeout,
		},
	}

	for _, tt := range tests {
		jc := &leaseControllerMock{
			leaseMethod: func(names []string, timeout uint32) (*job.Job, error) {
				return nil, tt.expErr
			},
		}
		handler := NewLeaseHandler(jc)

		cmd := prot.NewCmd(
			"lease",
			[][]byte{
				[]byte(testutil.GenName()),
				[]byte("1"),
			},
			prot.CmdFlags{},
		)

		resp, err := handler.Exec(cmd)
		if err != tt.clientErr || resp != nil {
			t.Fatalf("Lease err mismatch, exp=%s, act=%s, resp=%v", tt.clientErr, err, resp)
		}
	}
}

// Only client cmd specific args + parsing are tested.
// Job specific properties are owned by the job controller.
// Avoids heavy repeated test cases.
func TestLeaseInvalidArgs(t *testing.T) {
	jc := &leaseControllerMock{
		leaseMethod: func(names []string, timeout uint32) (*job.Job, error) {
			t.Fatalf("Unexpected call to job controller")
			return nil, nil
		},
	}
	handler := NewLeaseHandler(jc)

	tests := []struct {
		cmd  *prot.Cmd
		resp []byte
		err  error
	}{
		{
			prot.NewCmd(
				"lease",
				[][]byte{},
				prot.CmdFlags{},
			),
			nil,
			prot.ErrInvalidCmdArgs,
		},
		{
			prot.NewCmd(
				"lease",
				[][]byte{
					[]byte("q1"),
				},
				prot.CmdFlags{},
			),
			nil,
			prot.ErrInvalidCmdArgs,
		},
		{
			prot.NewCmd(
				"not-lease",
				[][]byte{
					[]byte("q1"),
					[]byte("1"),
				},
				prot.CmdFlags{},
			),
			nil,
			prot.ErrInvalidCmdArgs,
		},
		{
			prot.NewCmd(
				"lease",
				[][]byte{
					[]byte("q1"),
					[]byte("-1"),
				},
				prot.CmdFlags{},
			),
			nil,
			ErrInvalidWaitTimeout,
		},
	}

	for _, tt := range tests {
		resp, err := handler.Exec(tt.cmd)
		if !bytes.Equal(tt.resp, resp) {
			t.Fatalf("response mismatch, expResp=%v, actResp=%v", tt.resp, resp)
		}

		if err.Error() != tt.err.Error() {
			t.Fatalf("err mismatch, expErr=%v, actErr=%v", tt.err, err)
		}
	}
}

func compareRunRecord(a *job.RunRecord, b *job.RunRecord) bool {
	return a.Attempts == b.Attempts &&
		a.State == b.State &&
		a.Fails == b.Fails &&
		a.Job == b.Job &&
		bytes.Equal(a.Result, b.Result)
}
