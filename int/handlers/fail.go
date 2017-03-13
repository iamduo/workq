package handlers

import (
	"github.com/iamduo/workq/int/job"
	"github.com/iamduo/workq/int/prot"
)

const (
	// fail cmd arg indexes
	failArgID         = 0
	failArgResultSize = 1
	failArgResult     = 2
)

type FailHandler struct {
	jc job.Failer
}

func NewFailHandler(jc job.Failer) *FailHandler {
	return &FailHandler{
		jc: jc,
	}
}

// fail <id> <result-size> <result>
//
// Fails a job with an optional result.
// Stops any further TTR timer.
//
// Returns:
// CLIENT-ERROR * on invalid input
// NOT-FOUND if job does not exist
// OK on success
func (h *FailHandler) Exec(cmd *prot.Cmd) ([]byte, error) {
	if cmd.Name != "fail" || cmd.ArgC != 3 || cmd.FlagC != 0 {
		return nil, prot.ErrInvalidCmdArgs
	}

	id, err := parseID(cmd.Args[failArgID])
	if err != nil {
		return nil, prot.NewClientErr(err.Error())
	}

	result, err := parseResult(cmd.Args[failArgResultSize], cmd.Args[failArgResult])
	if err != nil {
		return nil, prot.NewClientErr(err.Error())
	}

	err = h.jc.Fail(id, result)
	if err == job.ErrNotFound {
		return nil, prot.ErrNotFound
	}
	if err != nil {
		return nil, prot.NewClientErr(err.Error())
	}

	return prot.OkResp(), nil
}
