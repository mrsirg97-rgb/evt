package broadcast

import (
	"fmt"
	"net"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"syscall"
)

const alphabet = "0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"

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

func EncodeID(id int64) string {
	out := make([]byte, 0, 11)
	v := uint64(id)
	for {
		out = append(out, alphabet[v%62])
		v /= 62
		if v == 0 {
			break
		}
	}
	for i, j := 0, len(out)-1; i < j; i, j = i+1, j-1 {
		out[i], out[j] = out[j], out[i]
	}
	return string(out)
}

func DecodeID(s string) (int64, error) {
	var v uint64
	for _, c := range s {
		var d uint64
		switch {
		case c >= '0' && c <= '9':
			d = uint64(c - '0')
		case c >= 'a' && c <= 'z':
			d = uint64(c-'a') + 10
		case c >= 'A' && c <= 'Z':
			d = uint64(c-'A') + 36
		default:
			return 0, fmt.Errorf("broadcast: %q is not a member id", s)
		}
		v = v*62 + d
	}
	return int64(v), nil
}

func Listen(dir string, id int64) (net.Listener, error) {
	path := filepath.Join(dir, fmt.Sprintf("%s-%d.sock", EncodeID(id), os.Getpid()))
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

func gone(pid int64) bool {
	if pid <= 0 {
		return true
	}
	return syscall.Kill(int(pid), 0) == syscall.ESRCH
}

func socketFile(name string) (int64, int64, bool) {
	name = strings.TrimSuffix(name, ".sock")
	dash := strings.LastIndex(name, "-")
	if dash < 0 {
		return 0, 0, false
	}
	id, err := DecodeID(name[:dash])
	if err != nil {
		return 0, 0, false
	}
	pid, err := strconv.ParseInt(name[dash+1:], 10, 64)
	if err != nil {
		return 0, 0, false
	}
	return id, pid, true
}
