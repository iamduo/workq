package captain

import (
	"io/ioutil"
	"os"
	"testing"
	"time"
)

func TestFileMutexLockUnlock(t *testing.T) {
	dir, err := ioutil.TempDir("", "test-captain")
	if err != nil {
		t.Fatal(err)
	}

	defer os.RemoveAll(dir)

	path := dir + "/test.lock"
	mu, err := openFileMutex(path)
	if err != nil {
		t.Fatalf("New File Mutex err=%s", err)
	}

	err = mu.Lock()
	defer mu.Unlock()
	if err != nil {
		t.Fatalf("Lock err=%s", err)
	}

	done := make(chan struct{})
	go func() {
		mu, err := openFileMutex(path)
		if err != nil {
			t.Fatalf("New File Mutex err=%s", err)
		}
		mu.RLock()
		defer mu.RUnlock()
		close(done)
	}()

	timer := time.NewTimer(100 * time.Millisecond)
	select {
	case <-done:
		t.Fatalf("Unexpected read lock")
	case <-timer.C:
	}

	err = mu.Unlock()
	if err != nil {
		t.Fatalf("Unlock err=%s", err)
	}
}

func TestFileMutexRLockUnlock(t *testing.T) {
	dir, err := ioutil.TempDir("", "test-captain")
	if err != nil {
		t.Fatal(err)
	}

	defer os.RemoveAll(dir)

	path := dir + "/test.lock"
	mu, err := openFileMutex(path)
	if err != nil {
		t.Fatalf("New File Mutex err=%s", err)
	}

	err = mu.RLock()
	defer mu.RUnlock()
	if err != nil {
		t.Fatalf("Lock err=%s", err)
	}

	done := make(chan struct{})
	go func() {
		mu, err := openFileMutex(path)
		if err != nil {
			t.Fatalf("New File Mutex err=%s", err)
		}
		mu.Lock()
		defer mu.Unlock()
		close(done)
	}()

	timer := time.NewTimer(100 * time.Millisecond)
	select {
	case <-done:
		t.Fatalf("Unexpected exclusive lock")
	case <-timer.C:
	}

	err = mu.RUnlock()
	if err != nil {
		t.Fatalf("Unlock err=%s", err)
	}
}

func TestFileMutexLockUnlockErrors(t *testing.T) {
	dir, err := ioutil.TempDir("", "test-captain")
	if err != nil {
		t.Fatal(err)
	}

	defer os.RemoveAll(dir)

	path := dir + "/test.lock"
	mu, err := openFileMutex(path)
	mu.file.Close()

	err = mu.Lock()
	defer mu.Unlock()
	if err == nil {
		t.Fatalf("Lock on closed fd, act=nil, exp err")
	}

	err = mu.RLock()
	defer mu.RUnlock()
	if err == nil {
		t.Fatalf("RLock on closed fd, act=nil, exp err")
	}

	err = mu.Unlock()
	if err == nil {
		t.Fatalf("Unlock on closed fd lock, act=nil, exp err")
	}
}

func TestTimeoutLock(t *testing.T) {
	fn := func() error {
		return nil
	}

	err := TimeoutLock(fn, 10*time.Millisecond)
	if err != nil {
		t.Fatalf("TimeoutLock err=%s, exp nil", err)
	}

	fn = func() error {
		time.Sleep(100 * time.Millisecond)
		return nil
	}

	err = TimeoutLock(fn, 10*time.Millisecond)
	if err != ErrLockTimeout {
		t.Fatalf("TimeoutLock act=%s, exp=%s", err, ErrLockTimeout)
	}
}
