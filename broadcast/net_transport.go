package broadcast

import (
	"bufio"
	"context"
	"encoding/binary"
	"errors"
	"io"
	"net"
	"sync"
	"time"
)

type NetTransport interface {
	Transport
	Peers() []string
	Addr() string
	Serve(ctx context.Context, onMessages func(messages ...Message) error, onSync func(since int64) []Message, onGossip func(theirSummary map[int]uint64) []Message) error
	Sync(ctx context.Context, peer string, since int64) ([]Message, error)
	Gossip(ctx context.Context, peer string, summary map[int]uint64) ([]Message, error)
}

type netTransport struct {
	id      int64
	addr    string
	peers   []string
	encoder Encoder
	timeout time.Duration
	idle    time.Duration

	mu       sync.Mutex
	listener net.Listener
	inbound  chan []Message
	closed   chan struct{}
	once     sync.Once

	poolMu sync.Mutex
	pools  map[string]chan net.Conn
}

func NewNetTransport(id int64, addr string, peers []string, timeout, idle time.Duration, encoder Encoder) NetTransport {
	return &netTransport{
		id:      id,
		addr:    addr,
		peers:   peers,
		encoder: encoder,
		timeout: timeout,
		idle:    idle,
		inbound: make(chan []Message, 1024),
		closed:  make(chan struct{}),
		pools:   make(map[string]chan net.Conn),
	}
}

func (t *netTransport) pool(peer string) chan net.Conn {
	t.poolMu.Lock()
	defer t.poolMu.Unlock()
	p, ok := t.pools[peer]
	if !ok {
		p = make(chan net.Conn, maxConnsPerPeer)
		t.pools[peer] = p
	}
	return p
}

func (t *netTransport) acquire(ctx context.Context, peer string) (net.Conn, error) {
	select {
	case conn := <-t.pool(peer):
		return conn, nil
	default:
		return (&net.Dialer{Timeout: t.timeout}).DialContext(ctx, "tcp", peer)
	}
}

func (t *netTransport) release(peer string, conn net.Conn) {
	select {
	case t.pool(peer) <- conn:
	default:
		conn.Close()
	}
}

func (t *netTransport) Id() int64       { return t.id }
func (t *netTransport) Addr() string    { return t.addr }
func (t *netTransport) Peers() []string { return t.peers }

func (t *netTransport) Send(ctx context.Context, callback func(ack error), messages ...Message) {
	body, err := t.frame(messages...)
	if err != nil {
		callback(err)
		return
	}
	frame := append([]byte{opPush}, body...)

	var wg sync.WaitGroup
	for _, peer := range t.peers {
		wg.Add(1)
		go func(peer string) {
			defer wg.Done()
			callback(t.deliver(ctx, peer, frame))
		}(peer)
	}
	wg.Wait()
}

func (t *netTransport) deliver(ctx context.Context, peer string, frame []byte) error {
	conn, err := t.acquire(ctx, peer)
	if err != nil {
		return err
	}
	conn.SetDeadline(time.Now().Add(t.timeout))
	if _, err := conn.Write(frame); err != nil {
		conn.Close()
		return err
	}
	ack := make([]byte, 1)
	if _, err := io.ReadFull(conn, ack); err != nil {
		conn.Close()
		return err
	}
	t.release(peer, conn)
	if ack[0] != 1 {
		return errors.New("peer rejected: " + peer)
	}
	return nil
}

func (t *netTransport) Serve(ctx context.Context, onMessages func(messages ...Message) error, onSync func(since int64) []Message, onGossip func(theirSummary map[int]uint64) []Message) error {
	listener, err := net.Listen("tcp", t.addr)
	if err != nil {
		return err
	}
	t.mu.Lock()
	t.listener = listener
	t.mu.Unlock()

	go func() {
		<-ctx.Done()
		listener.Close()
	}()

	for {
		conn, err := listener.Accept()
		if err != nil {
			select {
			case <-ctx.Done():
				return nil
			case <-t.closed:
				return nil
			default:
				return err
			}
		}
		go t.handle(conn, onMessages, onSync, onGossip)
	}
}

func (t *netTransport) handle(conn net.Conn, onMessages func(messages ...Message) error, onSync func(since int64) []Message, onGossip func(theirSummary map[int]uint64) []Message) {
	defer conn.Close()
	reader := bufio.NewReader(conn)

	for {
		conn.SetReadDeadline(time.Now().Add(t.idle))
		op, err := reader.ReadByte()
		if err != nil {
			return
		}

		switch op {
		case opPush:
			messages, err := t.readFrame(reader)
			if err != nil {
				return
			}
			accepted := onMessages == nil || onMessages(messages...) == nil
			if accepted {
				select {
				case t.inbound <- messages:
				default:
				}
			}
			ack := byte(0)
			if accepted {
				ack = 1
			}
			conn.SetWriteDeadline(time.Now().Add(t.timeout))
			if _, err := conn.Write([]byte{ack}); err != nil {
				return
			}

		case opGossip:
			countBuf := make([]byte, 4)
			if _, err := io.ReadFull(reader, countBuf); err != nil {
				return
			}
			n := binary.BigEndian.Uint32(countBuf)
			if n > 1<<20 {
				return
			}
			theirSummary := make(map[int]uint64, n)
			pair := make([]byte, 12)
			for range n {
				if _, err := io.ReadFull(reader, pair); err != nil {
					return
				}
				theirSummary[int(binary.BigEndian.Uint32(pair[0:4]))] = binary.BigEndian.Uint64(pair[4:12])
			}
			var log []Message
			if onGossip != nil {
				log = onGossip(theirSummary)
			}
			frame, err := t.frame(log...)
			if err != nil {
				return
			}
			conn.SetWriteDeadline(time.Now().Add(t.timeout))
			if _, err := conn.Write(frame); err != nil {
				return
			}

		case opPull:
			mark := make([]byte, 8)
			if _, err := io.ReadFull(reader, mark); err != nil {
				return
			}
			since := int64(binary.BigEndian.Uint64(mark))
			var log []Message
			if onSync != nil {
				log = onSync(since)
			}
			frame, err := t.frame(log...)
			if err != nil {
				return
			}
			conn.SetWriteDeadline(time.Now().Add(t.timeout))
			if _, err := conn.Write(frame); err != nil {
				return
			}

		default:
			return
		}
	}
}

func (t *netTransport) Recv(ctx context.Context, callback func(err error, messages ...Message)) {
	for {
		select {
		case <-ctx.Done():
			callback(ctx.Err())
			return
		case <-t.closed:
			return
		case messages := <-t.inbound:
			callback(nil, messages...)
		}
	}
}

func (t *netTransport) Sync(ctx context.Context, peer string, since int64) ([]Message, error) {
	conn, err := (&net.Dialer{Timeout: t.timeout}).DialContext(ctx, "tcp", peer)
	if err != nil {
		return nil, err
	}
	defer conn.Close()
	conn.SetDeadline(time.Now().Add(t.timeout))

	req := make([]byte, 9)
	req[0] = opPull
	binary.BigEndian.PutUint64(req[1:], uint64(since))
	if _, err := conn.Write(req); err != nil {
		return nil, err
	}
	return t.readFrame(bufio.NewReader(conn))
}

func (t *netTransport) Gossip(ctx context.Context, peer string, summary map[int]uint64) ([]Message, error) {
	conn, err := (&net.Dialer{Timeout: t.timeout}).DialContext(ctx, "tcp", peer)
	if err != nil {
		return nil, err
	}
	defer conn.Close()
	conn.SetDeadline(time.Now().Add(t.timeout))

	header := make([]byte, 5)
	header[0] = opGossip
	binary.BigEndian.PutUint32(header[1:], uint32(len(summary)))
	if _, err := conn.Write(header); err != nil {
		return nil, err
	}
	buf := make([]byte, 0, 12*len(summary))
	var pair [12]byte
	for b, h := range summary {
		binary.BigEndian.PutUint32(pair[0:4], uint32(b))
		binary.BigEndian.PutUint64(pair[4:12], h)
		buf = append(buf, pair[:]...)
	}
	if _, err := conn.Write(buf); err != nil {
		return nil, err
	}
	return t.readFrame(bufio.NewReader(conn))
}

func (t *netTransport) Close() {
	t.once.Do(func() {
		close(t.closed)
		t.mu.Lock()
		if t.listener != nil {
			t.listener.Close()
		}
		t.mu.Unlock()
		t.poolMu.Lock()
		for _, pool := range t.pools {
			for {
				select {
				case conn := <-pool:
					conn.Close()
					continue
				default:
				}
				break
			}
		}
		t.pools = make(map[string]chan net.Conn)
		t.poolMu.Unlock()
	})
}

func (t *netTransport) frame(messages ...Message) ([]byte, error) {
	var body []byte
	count := make([]byte, 4)
	binary.BigEndian.PutUint32(count, uint32(len(messages)))
	body = append(body, count...)

	for _, message := range messages {
		encoded, err := t.encoder.Encode(message)
		if err != nil {
			return nil, err
		}
		size := make([]byte, 4)
		binary.BigEndian.PutUint32(size, uint32(len(encoded)))
		body = append(body, size...)
		body = append(body, encoded...)
	}
	return body, nil
}

func (t *netTransport) readFrame(r *bufio.Reader) ([]Message, error) {
	header := make([]byte, 4)
	if _, err := io.ReadFull(r, header); err != nil {
		return nil, err
	}
	count := binary.BigEndian.Uint32(header)
	if count > 1<<16 {
		return nil, errors.New("frame count exceeds bound")
	}

	var messages []Message
	for range count {
		sizeBuf := make([]byte, 4)
		if _, err := io.ReadFull(r, sizeBuf); err != nil {
			return nil, err
		}
		size := binary.BigEndian.Uint32(sizeBuf)
		if size > maxFrameBytes {
			return nil, errors.New("frame payload exceeds bound")
		}
		payload := make([]byte, size)
		if _, err := io.ReadFull(r, payload); err != nil {
			return nil, err
		}
		message, err := t.encoder.Decode(payload)
		if err != nil {
			return nil, err
		}
		messages = append(messages, message)
	}
	return messages, nil
}

const (
	opPush   byte = 0
	opPull   byte = 1
	opGossip byte = 2

	maxConnsPerPeer = 16
	maxFrameBytes   = 1 << 20
	gossipChunk     = 4096
)
