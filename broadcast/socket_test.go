package broadcast_test

import (
	"context"
	"testing"
	"time"

	"github.com/mrsirg97-rgb/evt"
	"github.com/mrsirg97-rgb/evt/broadcast"
)

func socketPair(t *testing.T) (broadcast.Transport, broadcast.Transport, func()) {
	t.Helper()
	dir := t.TempDir()
	listener, err := broadcast.Listen(dir, 9)
	if err != nil {
		t.Fatal(err)
	}
	dialerConn, err := broadcast.Dial(dir, 9)
	if err != nil {
		t.Fatal(err)
	}
	acceptedConn, err := listener.Accept()
	if err != nil {
		t.Fatal(err)
	}
	codec := broadcast.NewJSONEncoder(wireRegistry("notice"))
	dialer := broadcast.NewPipeTransport(9, dialerConn, codec)
	acceptor := broadcast.NewPipeTransport(9, acceptedConn, codec)
	return dialer, acceptor, func() {
		dialer.Close()
		acceptor.Close()
		listener.Close()
	}
}

func TestTwoTransportsOverAUnixSocketExchangeFrames(t *testing.T) {
	dialer, acceptor, closeAll := socketPair(t)
	ear := &inbox{seen: make(chan struct{}, 8)}
	acceptor.Recv(context.Background(), ear.take)
	var ack error
	dialer.Send(context.Background(), func(e error) { ack = e },
		broadcast.Heartbeat(9, true),
		broadcast.NewMessage(9, true, evt.Notice{Source: "w9", Text: "over the socket"}))
	if ack != nil {
		t.Fatalf("ack = %v", ack)
	}
	ear.await(t, 2)
	if ear.got[0][0].Signal() != nil {
		t.Fatal("the heartbeat lands first and empty")
	}
	if n := ear.got[1][0].Signal().(evt.Notice); n.Text != "over the socket" || ear.got[1][0].Origin() != 9 {
		t.Fatalf("frame = %+v", ear.got[1][0])
	}
	closeAll()
}

func TestABadFrameClosesTheUnixTransport(t *testing.T) {
	dir := t.TempDir()
	listener, err := broadcast.Listen(dir, 9)
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	conn, err := broadcast.Dial(dir, 9)
	if err != nil {
		t.Fatal(err)
	}
	accepted, err := listener.Accept()
	if err != nil {
		t.Fatal(err)
	}
	acceptor := broadcast.NewPipeTransport(9, accepted, broadcast.NewJSONEncoder(wireRegistry("notice")))
	end := make(chan error, 1)
	acceptor.Recv(context.Background(), func(err error, messages ...broadcast.Message) {
		if err != nil {
			end <- err
		}
	})
	if _, err := conn.Write([]byte("not a frame\n")); err != nil {
		t.Fatal(err)
	}
	select {
	case <-end:
	case <-time.After(2 * time.Second):
		t.Fatal("the transport never reported the end")
	}
	conn.Close()
	acceptor.Close()
}
