package handlers

import (
	"github.com/iamduo/workq/int/job"
	"github.com/iamduo/workq/int/prot"
)

const (
	// complete cmd args index
	completeArgID         = 0
	completeArgResultSize = 1
	completeArgResult     = 2
)

type CompleteHandler struct {
	jc job.Completer
}

func NewCompleteHandler(jc job.Completer) *CompleteHandler {
	return &CompleteHandler{
		jc: jc,
	}
}

// complete <id> <result-size> <result>
//
// Sucessfully complete a job with an optional result.
// Stops TTR timer.
//
// Returns:
// CLIENT-ERROR * on invalid input
// NOT-FOUND if job does not exist
// OK on success
func (h *CompleteHandler) Exec(cmd *prot.Cmd) ([]byte, error) {
	if cmd.Name != "complete" || cmd.ArgC != 3 || cmd.FlagC != 0 {
		return nil, prot.ErrInvalidCmdArgs
	}

	id, err := parseID(cmd.Args[completeArgID])
	if err != nil {
		return nil, prot.NewClientErr(err.Error())
	}

	result, err := parseResult(cmd.Args[completeArgResultSize], cmd.Args[completeArgResult])
	if err != nil {
		return nil, prot.NewClientErr(err.Error())
	}

	err = h.jc.Complete(id, result)
	if err == job.ErrNotFound {
		return nil, prot.ErrNotFound
	}

	if err != nil {
		return nil, prot.NewClientErr(err.Error())
	}

	return prot.OkResp(), nil
}
