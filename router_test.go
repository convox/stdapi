package stdapi

import (
	"errors"
	"io"
	"io/ioutil"
	"net"
	"net/http/httptest"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/convox/logger"
	"github.com/gorilla/websocket"
)

type closeSignalListener struct {
	net.Listener
	sendBuffer int
	closed     chan struct{}
}

func (l *closeSignalListener) Accept() (net.Conn, error) {
	c, err := l.Listener.Accept()
	if err != nil {
		return nil, err
	}

	if tc, ok := c.(*net.TCPConn); ok && l.sendBuffer > 0 {
		tc.SetWriteBuffer(l.sendBuffer)
	}

	return &closeSignalConn{Conn: c, closed: l.closed}, nil
}

type closeSignalConn struct {
	net.Conn
	once   sync.Once
	closed chan struct{}
}

func (c *closeSignalConn) Close() error {
	c.once.Do(func() { close(c.closed) })
	return c.Conn.Close()
}

func socketServer(sendBuffer int, fn HandlerFunc) (*httptest.Server, chan struct{}) {
	s := New("test", "test")
	s.Logger = logger.NewWriter("", ioutil.Discard)
	s.Route("SOCKET", "/stream", fn)

	closed := make(chan struct{})

	ts := httptest.NewUnstartedServer(s)
	ts.Listener = &closeSignalListener{Listener: ts.Listener, sendBuffer: sendBuffer, closed: closed}
	ts.Start()

	return ts, closed
}

func dialSocket(t *testing.T, ts *httptest.Server, receiveBuffer int) *websocket.Conn {
	ws, _, err := websocket.DefaultDialer.Dial(strings.Replace(ts.URL, "http", "ws", 1)+"/stream", nil)
	if err != nil {
		t.Fatal(err)
	}

	if receiveBuffer > 0 {
		ws.UnderlyingConn().(*net.TCPConn).SetReadBuffer(receiveBuffer)
	}

	return ws
}

func startPinging(ws *websocket.Conn, every time.Duration) func() {
	stop := make(chan struct{})
	done := make(chan struct{})

	go func() {
		defer close(done)

		for {
			select {
			case <-stop:
				return
			case <-time.After(every):
				ws.WriteControl(websocket.PingMessage, nil, time.Now().Add(time.Second))
			}
		}
	}()

	return func() {
		close(stop)
		<-done
	}
}

func writeLines(c *Context, total int) error {
	line := strings.Repeat("x", 1023) + "\n"

	for sent := 0; sent < total; sent += len(line) {
		if _, err := io.WriteString(c, line); err != nil {
			return err
		}
	}

	return nil
}

// readStream reads text frames until the end-of-stream frame, calling pause once
// after the first 32 KiB.
func readStream(ws *websocket.Conn, pause func()) (int, error) {
	got := 0
	paused := false

	ws.SetReadDeadline(time.Now().Add(20 * time.Second))

	for {
		mt, r, err := ws.NextReader()
		if err != nil {
			return got, err
		}

		if mt == websocket.BinaryMessage {
			return got, nil
		}

		n, _ := io.Copy(ioutil.Discard, r)
		got += int(n)

		if !paused && got > 32<<10 && pause != nil {
			paused = true
			pause()
		}
	}
}

func expectClosed(t *testing.T, closed chan struct{}, within time.Duration) {
	t.Helper()

	select {
	case <-closed:
	case <-time.After(within):
		t.Fatalf("server did not close the socket within %s", within)
	}
}

func expectPong(t *testing.T, ws *websocket.Conn, want bool) {
	t.Helper()

	pong := make(chan struct{}, 1)
	ws.SetPongHandler(func(string) error {
		pong <- struct{}{}
		return nil
	})

	go func() {
		for {
			if _, _, err := ws.NextReader(); err != nil {
				return
			}
		}
	}()

	if err := ws.WriteControl(websocket.PingMessage, nil, time.Now().Add(time.Second)); err != nil {
		t.Fatal(err)
	}

	select {
	case <-pong:
		if !want {
			t.Fatal("server answered a ping after the stream ended")
		}
	case <-time.After(300 * time.Millisecond):
		if want {
			t.Fatal("server stopped reading after the stream ended")
		}
	}
}

func TestWebsocketSlowClientReceivesWholeStream(t *testing.T) {
	const total = 192 << 10

	returned := make(chan struct{})

	ts, closed := socketServer(64<<10, func(c *Context) error {
		defer close(returned)
		return writeLines(c, total)
	})
	defer ts.Close()

	ws := dialSocket(t, ts, 64<<10)
	stop := startPinging(ws, 20*time.Millisecond)

	got, err := readStream(ws, func() {
		select {
		case <-returned:
		case <-time.After(2 * time.Second):
			t.Fatal("handler still writing, socket buffers too small for this test")
		}

		time.Sleep(100 * time.Millisecond)
		runtime.GC()
		runtime.GC()
		time.Sleep(100 * time.Millisecond)
	})

	stop()
	ws.Close()

	if err != nil || got != total {
		t.Fatalf("got %d of %d bytes, err %v", got, total, err)
	}

	expectClosed(t, closed, 2*time.Second)
}

func TestWebsocketDrainOutlastsUnwritablePongs(t *testing.T) {
	const total = 24 << 10

	ts, closed := socketServer(4096, func(c *Context) error {
		return writeLines(c, total)
	})
	defer ts.Close()

	ws := dialSocket(t, ts, 4096)

	stop := startPinging(ws, 10*time.Millisecond)
	time.Sleep(2 * time.Second)

	got, err := readStream(ws, nil)
	stop()
	ws.Close()

	if err != nil || got != total {
		t.Fatalf("got %d of %d bytes, err %v", got, total, err)
	}

	expectClosed(t, closed, 2*time.Second)
}

func TestWebsocketClosesWhenClientHangsUp(t *testing.T) {
	ts, closed := socketServer(0, func(c *Context) error {
		return writeLines(c, 4096)
	})
	defer ts.Close()

	ws := dialSocket(t, ts, 0)

	if _, err := readStream(ws, nil); err != nil {
		t.Fatal(err)
	}

	expectPong(t, ws, true)

	ws.Close()

	expectClosed(t, closed, 2*time.Second)
}

func TestWebsocketDrainStopsAtDeadline(t *testing.T) {
	defer func(d time.Duration) { websocketDrainTimeout = d }(websocketDrainTimeout)
	websocketDrainTimeout = 200 * time.Millisecond

	ts, closed := socketServer(0, func(c *Context) error {
		return writeLines(c, 4096)
	})
	defer ts.Close()

	ws := dialSocket(t, ts, 0)
	defer ws.Close()

	if _, err := readStream(ws, nil); err != nil {
		t.Fatal(err)
	}

	expectClosed(t, closed, 2*time.Second)
}

func TestWebsocketErrorBeforeWritingIsNotDrained(t *testing.T) {
	ts, _ := socketServer(0, func(c *Context) error {
		return errors.New("no such app")
	})
	defer ts.Close()

	ws := dialSocket(t, ts, 0)
	defer ws.Close()

	_, data, err := ws.ReadMessage()
	if err != nil || string(data) != "ERROR: no such app\n" {
		t.Fatalf("got %q, err %v", data, err)
	}

	mt, _, err := ws.ReadMessage()
	if err != nil || mt != websocket.BinaryMessage {
		t.Fatalf("got message type %d, err %v", mt, err)
	}

	expectPong(t, ws, false)
}

func TestWebsocketRawConnIsNotDrained(t *testing.T) {
	received := make(chan string, 1)

	ts, _ := socketServer(0, func(c *Context) error {
		ws := c.Websocket()

		if _, err := c.Write([]byte("hello\n")); err != nil {
			return err
		}

		go func() {
			_, data, err := ws.ReadMessage()
			if err == nil {
				received <- string(data)
			}
		}()

		return nil
	})
	defer ts.Close()

	ws := dialSocket(t, ts, 0)
	defer ws.Close()

	if _, err := readStream(ws, nil); err != nil {
		t.Fatal(err)
	}

	if err := ws.WriteMessage(websocket.TextMessage, []byte("after")); err != nil {
		t.Fatal(err)
	}

	select {
	case got := <-received:
		if got != "after" {
			t.Fatalf("handler read %q", got)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("handler goroutine never read the message")
	}
}

func TestWebsocketDrainWaitsForContextReaders(t *testing.T) {
	defer func(d time.Duration) { websocketDrainTimeout = d }(websocketDrainTimeout)
	websocketDrainTimeout = 200 * time.Millisecond

	for _, parked := range []bool{true, false} {
		readerDone := make(chan struct{})
		firstRead := make(chan struct{})
		parked := parked

		ts, closed := socketServer(0, func(c *Context) error {
			if _, err := c.Write([]byte("hello\n")); err != nil {
				return err
			}

			go func() {
				defer close(readerDone)

				if !parked {
					time.Sleep(50 * time.Millisecond)
				}

				buf := make([]byte, 1024)
				for i := 0; ; i++ {
					if _, err := c.Read(buf); err != nil {
						return
					}
					if i == 0 {
						close(firstRead)
					}
				}
			}()

			if parked {
				<-firstRead
				time.Sleep(20 * time.Millisecond)
			}

			return nil
		})

		ws := dialSocket(t, ts, 0)

		if parked {
			if _, _, err := ws.ReadMessage(); err != nil {
				t.Fatal(err)
			}
			if err := ws.WriteMessage(websocket.TextMessage, []byte("input")); err != nil {
				t.Fatal(err)
			}
		}

		if _, err := readStream(ws, nil); err != nil {
			t.Fatal(err)
		}

		if err := ws.WriteControl(websocket.PingMessage, nil, time.Now().Add(time.Second)); err != nil {
			t.Fatal(err)
		}

		expectClosed(t, closed, 2*time.Second)

		select {
		case <-readerDone:
		case <-time.After(2 * time.Second):
			t.Fatalf("parked %v: Context.Read goroutine never returned", parked)
		}

		ws.Close()
		ts.Close()
	}
}
