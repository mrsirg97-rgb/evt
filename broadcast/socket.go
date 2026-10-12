package broadcast

import (
	"fmt"
	"net"
	"os"
	"path/filepath"
	"sort"
	"syscall"
)

type listener struct {
	inner *net.UnixListener
	path  string
}

func (l *listener) Accept() (net.Conn, error) {
	return l.inner.Accept()
}

func (l *listener) Close() error {
	err := l.inner.Close()
	if rerr := os.Remove(l.path); rerr != nil && err == nil {
		err = rerr
	}
	return err
}

func (l *listener) Addr() net.Addr {
	return l.inner.Addr()
}

func Listen(dir string, id int64) (net.Listener, error) {
	path := filepath.Join(dir, fmt.Sprintf("%d-%d.sock", id, os.Getpid()))
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}
	inner, err := net.ListenUnix("unix", &net.UnixAddr{Name: path, Net: "unix"})
	if err != nil {
		return nil, err
	}
	return &listener{inner: inner, path: path}, nil
}

func Scan(dir string) ([]int64, error) {
	files, err := os.ReadDir(dir)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, err
	}
	out := []int64{}
	for _, file := range files {
		id, pid, ok := socketFile(file.Name())
		if !ok {
			continue
		}
		if gone(pid) {
			if err := os.Remove(filepath.Join(dir, file.Name())); err != nil {
				return nil, err
			}
			continue
		}
		out = append(out, id)
	}
	sort.Slice(out, func(i, j int) bool { return out[i] < out[j] })
	return out, nil
}

func socketFile(name string) (int64, int64, bool) {
	var id, pid int64
	if _, err := fmt.Sscanf(name, "%d-%d.sock", &id, &pid); err != nil {
		return 0, 0, false
	}
	return id, pid, true
}

func gone(pid int64) bool {
	if pid <= 0 {
		return true
	}
	return syscall.Kill(int(pid), 0) == syscall.ESRCH
}

func Dial(dir string, id int64) (net.Conn, error) {
	files, err := os.ReadDir(dir)
	if err != nil {
		return nil, err
	}
	for _, file := range files {
		member, pid, ok := socketFile(file.Name())
		if !ok || member != id {
			continue
		}
		if gone(pid) {
			continue
		}
		path := filepath.Join(dir, file.Name())
		conn, err := net.Dial("unix", path)
		if err != nil {
			return nil, err
		}
		return conn, nil
	}
	return nil, fmt.Errorf("broadcast: member %d is not in the directory", id)
}
