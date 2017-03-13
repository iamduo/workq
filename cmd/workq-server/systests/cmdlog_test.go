package main_test

import (
	"bytes"
	"errors"
	"fmt"
	"io/ioutil"
	"os"
	"os/exec"
	"strconv"
	"testing"
	"time"

	"github.com/iamduo/go-workq"
	"github.com/iamduo/workq/int/testutil"
)

const (
	defaultTTR  = 1000
	defaultTTL  = 5000
	extendedTTL = 86400000
)

func TestCmdLog(t *testing.T) {
	buildServer()

	t.Run("1K", func(t *testing.T) {
		dir, err := ioutil.TempDir("./tmp", "workq")
		if err != nil {
			t.Fatal(err)
		}
		defer os.RemoveAll(dir)

		if _, err = workq.Connect(serverAddr()); err == nil {
			t.Fatalf("Workq server already running")
		}

		// Small size to force 1 record per log file.
		process := startCmdLogServer(dir, 15)
		defer process.Kill()

		client, err := workq.Connect(serverAddr())
		if err != nil {
			t.Fatalf("Connect err=%s", err)
		}

		checkers, err := runGenCommands(client, 1000)
		if err != nil {
			t.Fatal(err)
		}

		for i := range checkers {
			if err = checkers[i](client); err != nil {
				t.Fatal(err)
			}
		}

		if err = process.Kill(); err != nil {
			t.Fatal(err)
		}

		// Verify checkers after replay.
		process = startCmdLogServer(dir, 15)
		defer process.Kill()

		// Replay delay
		time.Sleep(1 * time.Second)

		// Sleep until after the default TTR to allow lease attempts to be revoked.
		time.Sleep(defaultTTR * time.Millisecond)

		client, err = workq.Connect(serverAddr())
		if err != nil {
			t.Fatalf("Connect err=%s", err)
		}

		// Replay validation.
		for i := range checkers {
			if err := checkers[i](client); err != nil {
				t.Fatal(err)
			}
		}

		// Sleep until after the default expirations and re-validate.
		time.Sleep(defaultTTL * time.Millisecond)

		for i := range checkers {
			if err := checkers[i](client); err != nil {
				t.Fatal(err)
			}
		}
	})

	// @TODO Replay 1k after cleaning.
}

func startCmdLogServer(path string, size uint) *os.Process {
	cmd := exec.Command(
		serverPath(),
		"-listen",
		serverAddr(),
		"-cmdlog-path",
		path,
		"-cmdlog-seg-size",
		strconv.Itoa(int(size)),
	)
	err := cmd.Start()
	if err != nil {
		panic(fmt.Sprintf("Unable to start test server, err=%s", err))
	}
	time.Sleep(100 * time.Millisecond)

	return cmd.Process
}

// Generates n jobs that expire at defaultTTL with these exceptions / canary jobs:
//
// 1 job completed
// 1 job failed
// 1 job leased, 1 attempt max, failed
// 1 job explictly deleted (has extended TTL, but should be deleted)
// 1 job scheduled,expired (has default TTL,)
// These canary jobs have special condition validtion and cover
// most of the unique cases within cmdlog.
//
// Returns a slice of checkers that will validate each job.
func runGenCommands(client *workq.Client, n int) ([]checker, error) {
	defaultBgJob := func(name string, payload []byte) *workq.BgJob {
		return &workq.BgJob{
			ID:      testutil.GenIDString(),
			Name:    name,
			TTR:     defaultTTR,
			TTL:     defaultTTL,
			Payload: payload,
		}
	}

	var checkers []checker
	for i := 1; i <= n; i++ {
		name := "ping" + strconv.Itoa(i)
		switch i {
		case 100:
			// Completed job canary.
			// Total 2 cmdlog records.
			j := defaultBgJob(name, []byte(name))
			j.TTL = extendedTTL
			if err := client.Add(j); err != nil {
				return nil, err
			}

			expResult := []byte("complete " + name)
			if err := client.Complete(j.ID, expResult); err != nil {
				return nil, err
			}

			fn := func(client *workq.Client) error {
				result, err := client.Result(j.ID, 10)
				if err != nil {
					return err
				}

				if !bytes.Equal(result.Result, expResult) {
					return fmt.Errorf("Canary mismatch, act=%s, exp=%s, canary=%s", result.Result, expResult, name)
				}

				return nil
			}
			checkers = append(checkers, fn)
		case 200:
			// Failed job canary.
			// Total 2 cmdlog records.
			j := defaultBgJob(name, []byte(name))
			j.TTL = extendedTTL
			if err := client.Add(j); err != nil {
				return nil, err
			}

			expResult := []byte("fail " + name)
			if err := client.Fail(j.ID, expResult); err != nil {
				return nil, err
			}

			fn := func(client *workq.Client) error {
				result, err := client.Result(j.ID, 10)
				if err != nil {
					return err
				}

				if result.Success {
					return errors.New("Canary success mismatch, act=true, exp=false")
				}

				if !bytes.Equal(result.Result, expResult) {
					return fmt.Errorf("Canary result mismatch, act=%s, exp=%s, canary=%s", result.Result, expResult, name)
				}

				return nil
			}
			checkers = append(checkers, fn)
		case 300:
			// Job with 1 max attempt leased, TTR timed out, expected to fail.
			// Total 3 cmdlog records.
			j := defaultBgJob(name, []byte(name))
			j.TTR = 1
			j.TTL = extendedTTL
			j.MaxAttempts = 1
			if err := client.Add(j); err != nil {
				return nil, err
			}

			// Force max-attempts failure.
			_, err := client.Lease([]string{name}, 10)
			if err != nil {
				return nil, err
			}

			fn := func(client *workq.Client) error {
				result, err := client.Result(j.ID, 10)
				if err != nil {
					return err
				}

				if result.Success {
					return errors.New("Canary success mismatch, act=true, exp=false")
				}

				if len(result.Result) != 0 {
					return fmt.Errorf("Canary result len mismatch, act=%d, exp=0, canary=%s", len(result.Result), name)
				}

				return nil
			}
			checkers = append(checkers, fn)

		case 400:
			// Job explictly deleted
			// Total 2 cmdlog records.
			j := defaultBgJob(name, []byte(name))
			j.TTL = extendedTTL
			if err := client.Add(j); err != nil {
				return nil, err
			}

			if err := client.Delete(j.ID); err != nil {
				return nil, err
			}

			fn := func(client *workq.Client) error {
				_, err := client.Result(j.ID, 10)
				rerr := err.(*workq.ResponseError)
				if rerr.Code() != "NOT-FOUND" {
					return fmt.Errorf("Job Result error mismatch, act=%s, exp=NOT-FOUND", rerr.Code())
				}

				return nil
			}
			checkers = append(checkers, fn)
		case 500:
			// Scheduled Job
			// Total 1 cmdlog record.
			j := &workq.ScheduledJob{
				ID:      testutil.GenIDString(),
				Name:    name,
				TTR:     defaultTTR,
				TTL:     defaultTTL,
				Payload: []byte(name),
				Time:    time.Now().UTC().Format(workq.TimeFormat),
			}
			if err := client.Schedule(j); err != nil {
				return nil, err
			}

			created := time.Now()
			fn := func(client *workq.Client) error {
				// Expect the job to be expired after the TTL.
				if time.Now().After(created.Add(time.Duration(j.TTL) * time.Millisecond)) {
					if _, err := client.Lease([]string{name}, 10); err == nil {
						return fmt.Errorf("Job returned after expiration, %s", name)
					}

					return nil
				}

				job, err := client.Lease([]string{name}, 10)
				if err != nil {
					return err
				}

				if !bytes.Equal(job.Payload, j.Payload) {
					return fmt.Errorf("Canary mismatch, act=%s, exp=%s, canary=%s", job.Payload, j.Payload, name)
				}

				return nil
			}
			checkers = append(checkers, fn)

		default:
			j := defaultBgJob(name, []byte(name))
			if err := client.Add(j); err != nil {
				return nil, err
			}

			created := time.Now().UTC()

			fn := func(client *workq.Client) error {
				// Expect the job to be expired after the TTL.
				if time.Now().After(created.Add(time.Duration(j.TTL) * time.Millisecond)) {
					if _, err := client.Lease([]string{name}, 10); err == nil {
						return fmt.Errorf("Job returned after expiration, %s", name)
					}

					return nil
				}

				job, err := client.Lease([]string{name}, 10)
				if err != nil {
					return err
				}

				if !bytes.Equal(job.Payload, j.Payload) {
					return fmt.Errorf("Canary mismatch, act=%s, exp=%s, canary=%s", job.Payload, j.Payload, name)
				}

				return nil
			}
			checkers = append(checkers, fn)
		}
	}

	return checkers, nil
}

type checker func(*workq.Client) error
