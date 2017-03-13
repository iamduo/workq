package main_test

import (
	"bytes"
	"fmt"
	"os"
	"os/exec"
	"testing"
	"time"

	"github.com/iamduo/go-workq"
	"github.com/iamduo/workq/int/testutil"
)

func TestSmoke(t *testing.T) {
	buildServer()
	process := startSmokeServer()

	client, err := workq.Connect(serverAddr())
	if err != nil {
		t.Fatalf("Connect err=%s", err)
	}

	t.Run("Run,Lease,Complete", func(t *testing.T) {
		expID := testutil.GenIDString()
		expResult := []byte("Pong!")
		done := make(chan struct{})
		go func() {
			defer close(done)
			client, err := workq.Connect(serverAddr())
			if err != nil {
				t.Fatalf("Connect err=%s", err)
			}

			job, err := client.Lease([]string{"ping-run"}, 1000)
			if err != nil {
				t.Fatalf("Unable to lease, err=%q", err)
			}

			if job.ID != expID {
				t.Fatalf("Job ID mismatch")
			}

			if err = client.Complete(expID, expResult); err != nil {
				t.Fatalf("Unable to complete, err=%q", err)
			}
		}()
		job := &workq.FgJob{
			ID:       expID,
			Name:     "ping-run",
			TTR:      5000,
			Timeout:  60000,
			Payload:  []byte("Ping!"),
			Priority: 10,
		}
		result, err := client.Run(job)
		if result == nil || err != nil {
			t.Fatalf("Result mismatch, result=%+v, err=%q", result, err)
		}

		if !result.Success || !bytes.Equal(expResult, result.Result) {
			t.Fatalf("Result mismatch")
		}
		<-done
	})

	t.Run("Schedule,Lease", func(t *testing.T) {
		expID := testutil.GenIDString()
		j := &workq.ScheduledJob{
			ID:          expID,
			Name:        "ping-schedule",
			Time:        time.Now().Add(2 * time.Second).UTC().Format("2006-01-02T15:04:05Z"),
			TTL:         60000,
			TTR:         5000,
			Payload:     []byte("Ping!"),
			Priority:    10,
			MaxAttempts: 3,
			MaxFails:    1,
		}
		err := client.Schedule(j)
		if err != nil {
			t.Fatalf("Unable to schedule, err=%q", err)
		}

		_, err = client.Lease([]string{"ping-schedule"}, 100)
		if err == nil || err.Error() != "TIMEOUT" {
			t.Fatalf("Lease mismatch, err=%q", err)
		}

		if j.ID != expID {
			t.Fatalf("Job ID mismatch")
		}

		job, err := client.Lease([]string{"ping-schedule"}, 1900)
		if err != nil || job.ID != expID {
			t.Fatalf("Lease mismatch, job=%+v, err=%q", job, err)
		}
	})

	t.Run("Add,Complete,Result", func(t *testing.T) {
		expID := testutil.GenIDString()
		result, err := client.Result(expID, 1000)
		if result != nil || err == nil || err.Error() != "NOT-FOUND" {
			t.Fatalf("Result mismatch, result=%+v, err=%s", result, err)
		}

		job := &workq.BgJob{
			ID:          expID,
			Name:        "ping",
			TTR:         5000,  // 5 second time-to-run limit
			TTL:         60000, // Expire after 60 seconds
			Payload:     []byte("Ping!"),
			Priority:    10, // @OPTIONAL Numeric priority, default 0.
			MaxAttempts: 3,  // @OPTIONAL Absolute max num of attempts.
			MaxFails:    1,  // @OPTIONAL Absolute max number of failures.
		}
		if err = client.Add(job); err != nil {
			t.Fatalf("Unable to add job, err=%s", err)
		}

		expResult := []byte("Pong!")
		if err = client.Complete(expID, expResult); err != nil {
			t.Fatalf("Unable to complete job, err=%s", err)
		}

		result, err = client.Result(expID, 1000)
		if err != nil {
			t.Fatalf("Result mismatch, err=%s", err)
		}

		if !result.Success || !bytes.Equal(expResult, result.Result) {
			t.Fatalf("Result mismatch, result=%+v", result)
		}
	})

	t.Run("Add,Lease", func(t *testing.T) {
		_, err := client.Lease([]string{"ping1", "ping2", "ping3"}, 100)
		if err == nil || err.Error() != "TIMEOUT" {
			t.Fatalf("Unable to lease job, err=%s", err)
		}

		j := &workq.BgJob{
			ID:          testutil.GenIDString(),
			Name:        "ping1",
			TTR:         5000,
			TTL:         60000,
			Payload:     []byte("Ping!"),
			Priority:    10,
			MaxAttempts: 3,
			MaxFails:    1,
		}
		if err = client.Add(j); err != nil {
			t.Fatalf("Unable to add job, err=%s", err)
		}

		job, err := client.Lease([]string{"ping1"}, 100)
		if job == nil || err != nil {
			t.Fatalf("Unable to lease job, job=%+v, err=%s", job, err)
		}
	})

	t.Run("Add,Fail,Result", func(t *testing.T) {
		j := &workq.BgJob{
			ID:          testutil.GenIDString(),
			Name:        "ping",
			TTR:         5000,
			TTL:         60000,
			Payload:     []byte("Ping!"),
			Priority:    10,
			MaxAttempts: 3,
			MaxFails:    1,
		}
		err := client.Add(j)
		if err != nil {
			t.Fatalf("Unable to add job, err=%s", err)
		}

		expResult := []byte("Failed-Pong!")
		if err = client.Fail(j.ID, expResult); err != nil {
			t.Fatalf("Unable to fail job, err=%q", err)
		}

		result, err := client.Result(j.ID, 1000)
		if err != nil {
			t.Fatalf("Result mismatch, err=%s", err)
		}

		if result.Success || !bytes.Equal(expResult, result.Result) {
			t.Fatalf("Result mismatch, result=%+v", result)
		}
	})

	t.Run("Add,Delete", func(t *testing.T) {
		j := &workq.BgJob{
			ID:          testutil.GenIDString(),
			Name:        "ping",
			TTR:         5000,
			TTL:         60000,
			Payload:     []byte("Ping!"),
			Priority:    10,
			MaxAttempts: 3,
			MaxFails:    1,
		}
		err := client.Add(j)
		if err != nil {
			t.Fatalf("Unable to add job, err=%s", err)
		}

		if err = client.Delete(j.ID); err != nil {
			t.Fatalf("Unable to delete")
		}
	})

	process.Kill()
}

func startSmokeServer() *os.Process {
	cmd := exec.Command(serverPath(), "-listen", serverAddr())
	err := cmd.Start()
	if err != nil {
		panic(fmt.Sprintf("Unable to start test server, err=%s", err))
	}
	time.Sleep(100 * time.Millisecond)

	return cmd.Process
}
