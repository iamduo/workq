package cmdlog

import (
	"errors"
	"log"
	"time"

	"github.com/iamduo/workq/int/captain"
	"github.com/iamduo/workq/int/job"
)

// StartCleaningCycle initiates the cleaning process in the background with the
// specified interval.
// A warmup is performed to retrieve the average job expiration times of each
// segment file. This prevents a large influx of rewrites on existing
// segment files if a "cold" cleaning was performed since we would not know
// how many records required cleaning.
func StartCleaningCycle(cleaner cleaner, fn captain.CleanFn, interval uint) error {
	t := time.NewTicker(time.Duration(interval) * time.Millisecond)
	go func() {
		for {
			select {
			case <-t.C:
				if err := captain.TimeoutLock(cleaner.Lock, 1*time.Second); err != nil {
					log.Printf("Cleaner lock err=%s", err)
					break
				}

				if err := cleaner.Clean(fn); err != nil {
					log.Printf("Cleaner clean err=%s", err)
				}

				if err := cleaner.Unlock(); err != nil {
					log.Printf("Cleaner unlock err=%s", err)
				}
			}
		}
	}()

	return nil
}

type cleaner interface {
	Clean(captain.CleanFn) error
	Lock() error
	Unlock() error
}

// CommandCleaner implements the captain.CleanFn interface under its Clean method.
// Cleans only when the average expiration of the records has been met, approximately
// halving a segment file on every clean.
type CommandCleaner struct {
	reg      *job.Registry
	stats    map[string]*avgExp
	lastPath string
}

func NewWarmedCommandCleaner(reg *job.Registry, cur segmentCursor) (*CommandCleaner, error) {
	c := newCommandCleaner(reg)
	if err := c.warmup(cur); err != nil {
		return nil, err
	}

	return c, nil
}

type segmentCursor interface {
	Segment() string
	Next() (*captain.Record, error)
}

func newCommandCleaner(reg *job.Registry) *CommandCleaner {
	return &CommandCleaner{
		reg:   reg,
		stats: make(map[string]*avgExp),
	}
}

func (c *CommandCleaner) warmup(cur segmentCursor) error {
	for {
		r, err := cur.Next()
		if err != nil {
			return err
		}

		if r == nil {
			break
		}

		seg := cur.Segment()
		_, err = c.Clean(seg, r)
		if err != nil {
			return err
		}
	}

	return nil
}

// Clean returns a bool if the related job within the record should be deleted.
// A job is targeted for deletion once it has passed its expiration.
func (c *CommandCleaner) Clean(path string, r *captain.Record) (bool, error) {
	if !c.checkPath(path) {
		return false, captain.ErrSkipSegment
	}

	return c.clean(path, r)
}

func (c *CommandCleaner) clean(path string, r *captain.Record) (bool, error) {
	var err error
	d := newDecoder(r.Payload)
	cType, err := d.ReadType()
	if err != nil {
		return false, err
	}

	var ok bool
	var exp time.Time
	switch cType {
	case cmdAdd, cmdSchedule:
		j, err := d.ReadJob()
		if err != nil {
			return false, err
		}

		ok, exp = c.cleanByJob(j)
	case cmdComplete, cmdFail, cmdDelete, cmdExpire, cmdStartAttempt, cmdTimeoutAttempt:
		id, err := d.ReadID()
		if err != nil {
			return false, err
		}

		ok, exp = c.cleanByID(id)
	default:
		return false, errors.New("unable to recognize cmd from record")
	}

	// Record the average expiration time for retained records.
	// Used in the next cycle to determine if a segment file should be cleaned or
	// skipped.
	if !ok {
		stat := c.stats[path]
		// Average out the remaining TTL in ms
		stat.add(int64(exp.Sub(time.Now().UTC()).Nanoseconds() / 1e6))
	}

	return ok, nil
}

// checkPath verifies if the path should be cleaned or skipped.
// Returns true to signify to clean and false to skip.
func (c *CommandCleaner) checkPath(path string) bool {
	if _, ok := c.stats[path]; !ok {
		c.stats[path] = new(avgExp)
	}

	stat := c.stats[path]

	// Keeping track of the last path allows for stats freezing when the cleaner
	// rotates to the next segment file. If stats are empty while being rotated,
	// the entire stat is deleted to ensure no orphaned stats for segments that
	// no longer exist.
	if c.lastPath == "" {
		c.lastPath = path
	}

	if c.lastPath != path {
		if c.stats[c.lastPath].empty() {
			delete(c.stats, c.lastPath)
		} else if !c.stats[c.lastPath].frozen() {
			// Segment switching, freeze stat.
			c.stats[c.lastPath].freeze()
		}

		c.lastPath = path
	}

	if stat.frozen() {
		if time.Now().After(stat.expiration()) {
			// Only process the file if the average TTL is older than the interval.
			// This prevents constant reprocessing.

			// Reset stats and find the new expiration average.
			c.stats[path] = new(avgExp)
			return true
		}

		// No need to process the file, time is before expiration avg.
		return false
	}

	// All non frozen paths should continue.
	return true
}

func (p *CommandCleaner) cleanByJob(j *job.Job) (bool, time.Time) {
	exp := j.Expiration()
	if err := job.ValidateTime(exp); err != nil {
		return true, time.Time{}
	}

	return false, exp
}

func (p *CommandCleaner) cleanByID(id job.ID) (bool, time.Time) {
	rec, ok := p.reg.Record(id)
	if !ok {
		return true, time.Time{}
	}

	rec.Mu.RLock()
	j := rec.Job
	rec.Mu.RUnlock()

	return false, j.Expiration()
}

type avgExp struct {
	total int64
	len   int
	done  bool
	time  time.Time
}

func (r *avgExp) add(v int64) {
	if r.done {
		return
	}

	r.total += v
	r.len++
}

func (r *avgExp) expiration() time.Time {
	return r.time
}

func (r *avgExp) empty() bool {
	return r.len == 0
}

func (r *avgExp) freeze() {
	if !r.done {
		r.done = true
		r.time = time.Now().UTC()
		if r.len > 0 {
			r.time = r.time.Add(time.Duration(r.total/int64(r.len)) * time.Millisecond)
		}
	}
}

func (r *avgExp) frozen() bool {
	return r.done
}
