package captain

import (
	"errors"
	"os"
	"syscall"
	"time"
)

// fileMutex acts like an sync.RWMutex, but across processes.
type fileMutex struct {
	file *os.File
}

func openFileMutex(name string) (*fileMutex, error) {
	file, err := os.OpenFile(name, os.O_CREATE|os.O_RDONLY, 0644)
	if err != nil {
		return nil, err
	}

	return &fileMutex{file: file}, nil
}

func (m *fileMutex) RLock() error {
	return syscall.Flock(int(m.file.Fd()), syscall.LOCK_SH)
}

func (m *fileMutex) Lock() error {
	return syscall.Flock(int(m.file.Fd()), syscall.LOCK_EX)
}

func (m *fileMutex) Unlock() error {
	return syscall.Flock(int(m.file.Fd()), syscall.LOCK_UN)
}

func (m *fileMutex) RUnlock() error {
	return m.Unlock()
}

// ErrLockTimeout signifies the lock failed to be acquire after the specified timeout.
var ErrLockTimeout = errors.New("lock timeout")

// TimeoutLock runs a file mutex lock/unlock function and times out after specified duration.
func TimeoutLock(fn func() error, dur time.Duration) error {
	ch := make(chan error)
	go func() {
		ch <- fn()
	}()

	select {
	case err := <-ch:
		return err
	case <-time.After(dur):
		return ErrLockTimeout
	}
}
