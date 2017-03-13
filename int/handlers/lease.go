package handlers

import (
	"github.com/iamduo/workq/int/job"
	"github.com/iamduo/workq/int/prot"
)

type LeaseHandler struct {
	jc job.Leaser
}

func NewLeaseHandler(jc job.Leaser) *LeaseHandler {
	return &LeaseHandler{
		jc: jc,
	}
}

// lease <name>... <wait-timeout>
//
// Lease a job by name blocking until wait-timeout. Multiple job names can be
// specified and they will be processed uniformly by random selection.
//
// Returns:
//
// CLIENT-ERROR * on invalid input.
// TIMEOUT if no jobs were available within <wait-timeout>.
// OK if successful with leased job response.
func (h *LeaseHandler) Exec(cmd *prot.Cmd) ([]byte, error) {
	var err error
	// No flags needed, don't allow garbage
	if cmd.Name != "lease" || cmd.ArgC < 2 || cmd.FlagC != 0 {
		return nil, prot.ErrInvalidCmdArgs
	}

	// Slice names from lease name1 name2 name3 <wait-timeout>
	// -1 for wait-timeout
	nameC := cmd.ArgC - 1
	names := make([]string, nameC)
	for i := 0; i < nameC; i++ {
		names[i], err = parseName(cmd.Args[i])
		if err != nil {
			return nil, prot.NewClientErr(err.Error())
		}
	}

	timeout, err := parseTimeout(cmd.Args[cmd.ArgC-1])
	if err != nil {
		return nil, err
	}

	j, err := h.jc.Lease(names, timeout)
	if err != nil {
		if err == job.ErrTimeout {
			return nil, prot.ErrTimeout
		}

		return nil, prot.NewClientErr(err.Error())
	}

	return prot.OkJobResp(j.ID, j.Name, j.Payload), nil
}
