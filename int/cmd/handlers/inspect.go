package handlers

import (
	"bytes"
	"fmt"
	"sort"
	"strconv"
	"time"

	"github.com/iamduo/workq/int/job"
	"github.com/iamduo/workq/int/prot"
	"github.com/iamduo/workq/int/server"
	"github.com/iamduo/workq/int/wqueue"
)

const (
	// cmd args index
	inspectArgObject = 0
	timeFormat       = "2006-01-02T15:04:05Z"

	inspectQueuesArgCursorOffset = 1
	inspectQueuesArgLimit        = 2

	inspectQueueArgName = 1

	inspectJobsArgName         = 1
	inspectJobsArgCursorOffset = 2
	inspectJobsArgLimit        = 3

	inspectJobArgID = 1

	MaxLimit = 100
)

var (
	ErrInvalidCursorOffset = prot.NewClientErr("Invalid cursor offset")
	ErrInvalidLimit        = prot.NewClientErr("Invalid limit")
)

type Handler interface {
	Exec(cmd *prot.Cmd) ([]byte, error)
}

// Primary Inspect Handler acts as a router to other sub inspect cmds
type InspectHandler struct {
	server Handler
	queues Handler
	queue  Handler
	jobs   Handler
	job    Handler
}

func NewInspectHandler(
	server Handler,
	queues Handler,
	queue Handler,
	jobs Handler,
	j Handler,
) *InspectHandler {
	return &InspectHandler{
		server: server,
		queues: queues,
		queue:  queue,
		jobs:   jobs,
		job:    j,
	}
}

// inspect <object>
// Routes inspect commands to their individual object commands.
//
// Returns:
// CLIENT-ERROR on invalid input
// UnknownCmd error invalid inspect object
func (h *InspectHandler) Exec(cmd *prot.Cmd) ([]byte, error) {
	if cmd.Name != "inspect" || cmd.ArgC < 1 {
		return nil, prot.ErrInvalidCmdArgs
	}

	switch string(cmd.Args[inspectArgObject]) {
	case "server":
		return h.server.Exec(cmd)
	case "queues":
		return h.queues.Exec(cmd)
	case "queue":
		return h.queue.Exec(cmd)
	case "jobs":
		return h.jobs.Exec(cmd)
	case "scheduled-jobs":
		return h.jobs.Exec(cmd)
	case "job":
		return h.job.Exec(cmd)
	}

	return nil, prot.ErrUnknownCmd
}

type InspectServerHandler struct {
	serverUsage  *server.Usage
	handlerUsage *Usage
}

func NewInspectServerHandler(serverUsage *server.Usage, handlerUsage *Usage) *InspectServerHandler {
	return &InspectServerHandler{
		serverUsage:  serverUsage,
		handlerUsage: handlerUsage,
	}
}

// inspect server
//
// Show general info on server state.
//
// Returns:
// CLIENT-ERROR in invalid input
func (h *InspectServerHandler) Exec(cmd *prot.Cmd) ([]byte, error) {
	if cmd.Name != "inspect" || cmd.ArgC != 1 || cmd.FlagC != 0 ||
		!bytes.Equal(cmd.Args[inspectArgObject], []byte("server")) {
		return nil, prot.ErrInvalidCmdArgs
	}

	b := []byte("OK 1")
	b = append(b, prot.CrnlByte...)

	b = append(b, []byte("server 2")...)
	b = append(b, prot.CrnlByte...)

	b = append(b, []byte(fmt.Sprintf("active-clients %d", h.serverUsage.ActiveClients))...)
	b = append(b, prot.CrnlByte...)

	b = append(b, []byte(fmt.Sprintf("evicted-jobs %d", h.handlerUsage.EvictedJobs))...)
	b = append(b, prot.CrnlByte...)

	b = append(b, []byte(fmt.Sprintf("started %s", h.serverUsage.Started.Format(timeFormat)))...)
	b = append(b, prot.CrnlByte...)

	return b, nil
}

type InspectQueueHandler struct {
	qc wqueue.ControllerInterface
}

func NewInspectQueueHandler(qc wqueue.ControllerInterface) *InspectQueueHandler {
	return &InspectQueueHandler{qc: qc}
}

// Inspect queue <name>
//
// Show single queue info by name.
//
// Returns:
// CLIENT-ERROR on invalid input
// NOT-FOUND when queue is not available
func (h *InspectQueueHandler) Exec(cmd *prot.Cmd) ([]byte, error) {
	if cmd.Name != "inspect" || cmd.ArgC != 2 || cmd.FlagC != 0 ||
		!bytes.Equal(cmd.Args[inspectArgObject], []byte("queue")) {
		return nil, prot.ErrInvalidCmdArgs
	}

	name, err := job.NameFromBytes(cmd.Args[inspectQueueArgName])
	if err != nil {
		return nil, prot.NewClientErr(err.Error())
	}

	queues, mu := h.qc.Queues()
	mu.RLock()
	q, ok := queues[name]
	mu.RUnlock()
	if !ok {
		return nil, prot.ErrNotFound
	}

	insp := wqueue.NewInspector(q.(*wqueue.WorkQueue))
	b := []byte("OK 1")
	b = append(b, prot.CrnlByte...)
	b = append(b, QueueResp(name, insp)...)
	return b, nil
}

type InspectQueuesHandler struct {
	qc wqueue.ControllerInterface
}

func NewInspectQueuesHandler(qc wqueue.ControllerInterface) *InspectQueuesHandler {
	return &InspectQueuesHandler{qc: qc}
}

// Inspect queues <cursor-offset> <limit>
//
// Scan available queues based on a cursor-offset and limit.
// Returns queues in aplhabetical order.
//
// Returns:
// CLIENT-ERROR on invalid input
// NOT-FOUND when queue is not available
func (h *InspectQueuesHandler) Exec(cmd *prot.Cmd) ([]byte, error) {
	if cmd.Name != "inspect" || cmd.ArgC != 3 || cmd.FlagC != 0 ||
		!bytes.Equal(cmd.Args[inspectArgObject], []byte("queues")) {
		return nil, prot.ErrInvalidCmdArgs
	}

	offset, err := CursorOffsetFromBytes(cmd.Args[inspectQueuesArgCursorOffset])
	if err != nil {
		return nil, err
	}

	limit, err := LimitFromBytes(cmd.Args[inspectQueuesArgLimit])
	if err != nil {
		return nil, err
	}

	queues, mu := h.qc.Queues()
	var keys []string
	mu.RLock()
	for k := range queues {
		keys = append(keys, k)
	}
	mu.RUnlock()
	sort.Strings(keys)

	kLen := len(keys)
	if offset >= kLen {
		b := []byte("OK 0")
		b = append(b, prot.CrnlByte...)
		return b, nil
	}

	if limit >= (kLen - offset) {
		limit = (kLen - offset)
	}

	slice := keys[offset : offset+limit]
	b := []byte(fmt.Sprintf("OK %d", len(slice)))
	b = append(b, prot.CrnlByte...)
	for _, k := range slice {
		mu.RLock()
		q := queues[k]
		mu.RUnlock()

		insp := wqueue.NewInspector(q.(*wqueue.WorkQueue))
		b = append(b, QueueResp(k, insp)...)
	}

	return b, nil
}

// Format single Queue for inspect cmds.
func QueueResp(name string, insp *wqueue.Inspector) []byte {
	var b []byte
	b = append(b, []byte(fmt.Sprintf("%s 2", name))...)
	b = append(b, prot.CrnlByte...)
	readyLen, schedLen := insp.Lens()
	b = append(b, []byte(fmt.Sprintf("ready-len %d", readyLen))...)
	b = append(b, prot.CrnlByte...)
	b = append(b, []byte(fmt.Sprintf("scheduled-len %d", schedLen))...)
	b = append(b, prot.CrnlByte...)
	return b
}

type InspectJobsHandler struct {
	reg *job.Registry
	qc  wqueue.ControllerInterface
}

func NewInspectJobsHandler(reg *job.Registry, qc wqueue.ControllerInterface) *InspectJobsHandler {
	return &InspectJobsHandler{reg: reg, qc: qc}
}

// inspect jobs <name> <cursor-offset> <limit>
// &
// inspect scheduled-jobs <name> <cursor-offset> <limit>
//
// Scan jobs by name with a cursor-offset and limit.
// <jobs> returns in priority,created-time order.
// <scheduled-jobs> returns in scheduled time, priority,created-time order.
//
// Returns:
// CLIENT-ERROR in invalid input
func (h *InspectJobsHandler) Exec(cmd *prot.Cmd) ([]byte, error) {
	if cmd.Name != "inspect" || cmd.ArgC != 4 || cmd.FlagC != 0 {
		return nil, prot.ErrInvalidCmdArgs
	}

	isScheduled := bytes.Equal(cmd.Args[inspectArgObject], []byte("scheduled-jobs"))
	if !bytes.Equal(cmd.Args[inspectArgObject], []byte("jobs")) && !isScheduled {
		return nil, prot.ErrInvalidCmdArgs
	}

	name, err := job.NameFromBytes(cmd.Args[inspectJobsArgName])
	if err != nil {
		return nil, prot.NewClientErr(err.Error())
	}

	offset, err := CursorOffsetFromBytes(cmd.Args[inspectJobsArgCursorOffset])
	if err != nil {
		return nil, err
	}

	limit, err := LimitFromBytes(cmd.Args[inspectJobsArgLimit])
	if err != nil {
		return nil, err
	}

	queues, mu := h.qc.Queues()
	mu.RLock()
	q, ok := queues[name]
	mu.RUnlock()
	if !ok {
		return nil, prot.ErrNotFound
	}

	insp := wqueue.NewInspector(q.(*wqueue.WorkQueue))
	var it *wqueue.Iterator
	if isScheduled {
		_, it = insp.Iterators()
	} else {
		it, _ = insp.Iterators()
	}
	if !it.Seek(offset) {
		b := []byte("OK 0")
		b = append(b, prot.CrnlByte...)
		return b, nil
	}

	var buf []byte
	var item interface{}
	item = it.Current()
	var i int
	var skipped int
	for i = 0; i < limit; i++ {
		if item == nil {
			break
		}

		j := item.(*job.Job)
		rec, ok := h.reg.Record(j.ID)
		if !ok {
			skipped++
			// Job expired or deleted while scanning.
			continue
		}

		buf = append(buf, JobResp(rec, j)...)
		item = it.Next()
	}

	resp := []byte(fmt.Sprintf("OK %d", i-skipped))
	resp = append(resp, prot.CrnlByte...)
	resp = append(resp, buf...)
	return resp, nil
}

type InspectJobHandler struct {
	reg *job.Registry
}

func NewInspectJobHandler(reg *job.Registry) *InspectJobHandler {
	return &InspectJobHandler{reg: reg}
}

// inspect job <id>
//
// Show a single job by id.
//
// Returns:
// CLIENT-ERROR on invalid input
// NOT-FOUND On invalid ID
func (h *InspectJobHandler) Exec(cmd *prot.Cmd) ([]byte, error) {
	if cmd.Name != "inspect" || cmd.ArgC != 2 || cmd.FlagC != 0 ||
		!bytes.Equal(cmd.Args[inspectArgObject], []byte("job")) {
		return nil, prot.ErrInvalidCmdArgs
	}

	id, err := job.IDFromBytes(cmd.Args[inspectJobArgID])
	if err != nil {
		return nil, prot.NewClientErr(err.Error())
	}

	rec, ok := h.reg.Record(id)
	if !ok {
		return nil, prot.ErrNotFound
	}

	resp := []byte("OK 1")
	resp = append(resp, prot.CrnlByte...)
	resp = append(resp, JobResp(rec, rec.Job)...)
	return resp, nil
}

// Format a single job for inspect cmds.
func JobResp(rec *job.RunRecord, j *job.Job) []byte {
	var b []byte
	isScheduled := j.Time != (time.Time{})
	keyLen := 12
	if isScheduled {
		keyLen++
	}

	b = append(b, []byte(fmt.Sprintf("%s %d", j.ID, keyLen))...)
	b = append(b, prot.CrnlByte...)

	b = append(b, []byte(fmt.Sprintf("name %s", j.Name))...)
	b = append(b, prot.CrnlByte...)

	b = append(b, []byte(fmt.Sprintf("ttr %d", j.TTR))...)
	b = append(b, prot.CrnlByte...)

	b = append(b, []byte(fmt.Sprintf("ttl %d", j.TTL))...)
	b = append(b, prot.CrnlByte...)

	b = append(b, []byte(fmt.Sprintf("payload-size %d", len(j.Payload)))...)
	b = append(b, prot.CrnlByte...)

	b = append(b, []byte(fmt.Sprintf("payload %s", j.Payload))...)
	b = append(b, prot.CrnlByte...)

	b = append(b, []byte(fmt.Sprintf("max-attempts %d", j.MaxAttempts))...)
	b = append(b, prot.CrnlByte...)

	rec.Mu.RLock()
	attempts := rec.Attempts
	fails := rec.Fails
	state := rec.State
	rec.Mu.RUnlock()

	b = append(b, []byte(fmt.Sprintf("attempts %d", attempts))...)
	b = append(b, prot.CrnlByte...)

	b = append(b, []byte(fmt.Sprintf("max-fails %d", j.MaxFails))...)
	b = append(b, prot.CrnlByte...)

	b = append(b, []byte(fmt.Sprintf("fails %d", fails))...)
	b = append(b, prot.CrnlByte...)

	b = append(b, []byte(fmt.Sprintf("priority %d", j.Priority))...)
	b = append(b, prot.CrnlByte...)

	b = append(b, []byte(fmt.Sprintf("state %d", state))...)
	b = append(b, prot.CrnlByte...)

	b = append(b, []byte(fmt.Sprintf("created %s", j.Created.Format(job.TimeFormat)))...)
	b = append(b, prot.CrnlByte...)

	if isScheduled {
		b = append(b, []byte(fmt.Sprintf("time %s", j.Time.Format(job.TimeFormat)))...)
		b = append(b, prot.CrnlByte...)
	}

	return b
}

// Return a valid cursor offset from byte slice.
// A valid cursor offset is 2^32 - 1 and non-negative.
// Returns an error if cursor offset is out of range.
func CursorOffsetFromBytes(b []byte) (int, error) {
	offset, err := strconv.ParseUint(string(b), 10, 32)
	if err != nil {
		return 0, ErrInvalidCursorOffset
	}

	return int(offset), nil
}

// Return a valid limit from byte slice.
// A valid limit is 1-1000
// Returns an error if cursor offset is out of range.
func LimitFromBytes(b []byte) (int, error) {
	limit, err := strconv.ParseUint(string(b), 10, 16)
	if err != nil || limit < 1 || limit > MaxLimit {
		return 0, ErrInvalidLimit
	}

	return int(limit), nil
}
