package xhttp

import (
	"context"
	"crypto/tls"
	"errors"
	"io"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/metacubex/mihomo/common/httputils"

	"github.com/metacubex/http"
)

// dialFactory — фабрика h2-транспортов XHTTP одного клиента со склейкой.
// target и delay решают за каждый dial по его номеру (с 1), куда он идёт и
// сколько ждёт перед TCP, — так изображаются потерянный SYN и блокировка.
// tlsDelay, если не nil, — сколько рукопожатие с этим номером (с 1) ждёт
// после установленного TCP, — так изображается медленный ClientHello.
func dialFactory(t *testing.T, d *dialCounter, target func(n int64) string, delay func(n int64) time.Duration, tlsDelay func(n int64) time.Duration) TransportMaker {
	t.Helper()
	var handshakes atomic.Int64
	mk := CoalesceDials(func() http.RoundTripper {
		return NewTransport(func(ctx context.Context) (net.Conn, error) {
			n := d.dials.Add(1)
			if wait := delay(n); wait > 0 {
				select {
				case <-time.After(wait):
				case <-ctx.Done():
					return nil, ctx.Err()
				}
			}
			c, err := (&net.Dialer{}).DialContext(ctx, "tcp", target(n))
			if err != nil {
				return nil, err
			}
			return d.track(c), nil
		}, func(ctx context.Context, c net.Conn, _ bool) (net.Conn, error) {
			if n := handshakes.Add(1); tlsDelay != nil {
				if wait := tlsDelay(n); wait > 0 {
					select {
					case <-time.After(wait):
					case <-ctx.Done():
						return nil, ctx.Err()
					}
				}
			}
			tc := tls.Client(c, &tls.Config{NextProtos: []string{"h2"}, InsecureSkipVerify: true})
			if err := tc.HandshakeContext(ctx); err != nil {
				return nil, err
			}
			return tc, nil
		}, nil, nil, 0)
	})
	return func() http.RoundTripper {
		rt := mk()
		if _, ok := rt.(*coalescingTransport); !ok {
			t.Fatalf("h2 transport was not wrapped: %T", rt)
		}
		t.Cleanup(func() { rt.(*coalescingTransport).CloseIdleConnections() })
		return rt
	}
}

func okServer(t *testing.T) string {
	return h2TestServer(t, 0, func(w http.ResponseWriter, r *http.Request) { w.Write([]byte("ok")) })
}

// Клиент уже дозванивался, теперь холодный залп на свежем транспорте, и SYN
// ведущего потерян: его dial стоит до TCP. Через coalescedBackupAfter
// начинается запасной, как backup connect job у Chrome, и залп идёт по нему.
func TestCoalesceDialsBackupOnLostSYN(t *testing.T) {
	addr := okServer(t)
	var d dialCounter
	mk := dialFactory(t, &d, func(int64) string { return addr }, func(n int64) time.Duration {
		if n == 2 {
			return 3 * time.Second
		}
		return 0
	}, nil)
	if err := roundTrip(mk(), context.Background()); err != nil {
		t.Fatal(err)
	}
	failed, took := burst(mk(), 30, 5*time.Second)
	if failed != 0 {
		t.Fatalf("%d of 30 requests failed", failed)
	}
	if took >= coalescedBackupAfter+500*time.Millisecond {
		t.Fatalf("burst behind a lost SYN took %v: no backup dial", took)
	}
	if got := d.dials.Load(); got != 3 {
		t.Fatalf("%d dials, want warm-up, the stuck one and one backup", got)
	}
}

// Первый dial клиента и единственный запрос: запасной не ждёт ни истории, ни
// второго запроса. У Chrome backup job тоже от них не зависит.
func TestCoalesceDialsBackupForLoneFirstRequest(t *testing.T) {
	addr := okServer(t)
	var d dialCounter
	mk := dialFactory(t, &d, func(int64) string { return addr }, func(n int64) time.Duration {
		if n == 1 {
			return 3 * time.Second
		}
		return 0
	}, nil)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	start := time.Now()
	if err := roundTrip(mk(), ctx); err != nil {
		t.Fatal(err)
	}
	if took := time.Since(start); took > time.Second {
		t.Fatalf("lone request behind a lost SYN took %v", took)
	}
	if got := d.dials.Load(); got != 2 {
		t.Fatalf("%d dials, want the stuck one and one backup", got)
	}
}

// Рабочий путь: dial укладывается в coalescedBackupAfter, запасных нет.
func TestCoalesceDialsBackupQuietOnHealthyPath(t *testing.T) {
	addr := okServer(t)
	var d dialCounter
	mk := dialFactory(t, &d, func(int64) string { return addr }, func(n int64) time.Duration {
		if n == 1 {
			return 0
		}
		return 100 * time.Millisecond
	}, nil)
	if err := roundTrip(mk(), context.Background()); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 3; i++ {
		if failed, _ := burst(mk(), 30, 5*time.Second); failed != 0 {
			t.Fatalf("burst %d: %d of 30 requests failed", i, failed)
		}
	}
	if got := d.dials.Load(); got != 4 {
		t.Fatalf("%d dials for warm-up and three cold bursts, want 4", got)
	}
}

// TCP ведущего поднялся сразу, а рукопожатие идёт 600 мс. Браузер второго
// ClientHello на медленное рукопожатие не шлёт, и мы не шлём: ни запасного,
// ни своих dial'ов у ждущих, залп ждёт ведущего и идёт по одному соединению.
func TestCoalesceDialsOneConnOnSlowHandshake(t *testing.T) {
	addr := okServer(t)
	var d dialCounter
	mk := dialFactory(t, &d, func(int64) string { return addr }, func(int64) time.Duration { return 0 }, func(n int64) time.Duration {
		if n == 2 {
			return 600 * time.Millisecond
		}
		return 0
	})
	if err := roundTrip(mk(), context.Background()); err != nil {
		t.Fatal(err)
	}
	failed, took := burst(mk(), 30, 5*time.Second)
	if failed != 0 {
		t.Fatalf("%d of 30 requests failed", failed)
	}
	if got := d.dials.Load(); got != 2 {
		t.Fatalf("%d dials, want warm-up and the slow one: no second ClientHello while a handshake runs", got)
	}
	if took < 600*time.Millisecond {
		t.Fatalf("burst took %v, faster than its only handshake", took)
	}
}

// Запасной dial сразу получает отказ (сброс, закрытый порт), а застрявший
// ведущий через 600 мс дозванивается. Отказ запасного залп не роняет: ведущий
// дозванивается, и все идут по нему.
func TestCoalesceDialsFailedBackupKeepsLeader(t *testing.T) {
	addr := okServer(t)
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	refused := ln.Addr().String()
	ln.Close()
	var d dialCounter
	mk := dialFactory(t, &d, func(n int64) string {
		if n <= 2 {
			return addr
		}
		return refused
	}, func(n int64) time.Duration {
		if n == 2 {
			return 600 * time.Millisecond
		}
		return 0
	}, nil)
	if err := roundTrip(mk(), context.Background()); err != nil {
		t.Fatal(err)
	}
	failed, took := burst(mk(), 30, 5*time.Second)
	if failed != 0 {
		t.Fatalf("%d of 30 requests failed with the backup's error", failed)
	}
	if took >= time.Second {
		t.Fatalf("burst took %v: waited past the leader's dial", took)
	}
	if got := d.dials.Load(); got != 3 {
		t.Fatalf("%d dials, want warm-up, the slow leader and the refused backup", got)
	}
}

// Блокировка после ClientHello: TCP поднимается, запасного нет, и пока попытка
// висит, второй не начинается: попытки сменяют друг друга по
// coalescedAttemptTimeout, и сокет в полёте один.
func TestCoalesceDialsOneAttemptOnBlackhole(t *testing.T) {
	addr := okServer(t)
	hole := blackholeServer(t)
	var d dialCounter
	mk := dialFactory(t, &d, func(n int64) string {
		if n == 1 {
			return addr
		}
		return hole
	}, func(int64) time.Duration { return 0 }, nil)
	warm := mk()
	if err := roundTrip(warm, context.Background()); err != nil {
		t.Fatal(err)
	}
	warm.(*coalescingTransport).CloseIdleConnections()
	waitFor(t, "the warm-up connection to close", time.Second, func() bool { return d.open.Load() == 0 })
	failed, _ := burst(mk(), 50, 2500*time.Millisecond)
	if failed != 50 {
		t.Fatalf("%d of 50 requests failed, want all", failed)
	}
	want := int64(2500*time.Millisecond/coalescedAttemptTimeout) + 1
	if got := d.dials.Load() - 1; got != want {
		t.Fatalf("%d dials into the blackhole in 2.5s, want %d: one per attempt bound", got, want)
	}
	if got := d.maxOpen.Load(); got > 1 {
		t.Fatalf("%d sockets open at once, want 1", got)
	}
	waitFor(t, "abandoned dials to close", 2*time.Second, func() bool { return d.open.Load() == 0 })
}

// lagConn — соединение канала с задержкой rtt: всё, что присылает сервер,
// приходит через rtt после отправки. Запись уходит сразу, так что круг
// «запрос — ответ» стоит одну rtt, как на настоящем канале.
type lagConn struct {
	net.Conn
	rtt    time.Duration
	chunks chan lagChunk
	buf    []byte
	err    error
}

type lagChunk struct {
	data []byte
	at   time.Time
	err  error
}

func newLagConn(c net.Conn, rtt time.Duration) *lagConn {
	l := &lagConn{Conn: c, rtt: rtt, chunks: make(chan lagChunk, 64)}
	go func() {
		for {
			b := make([]byte, 32<<10)
			n, err := c.Read(b)
			l.chunks <- lagChunk{b[:n], time.Now().Add(rtt), err}
			if err != nil {
				return
			}
		}
	}()
	return l
}

func (l *lagConn) Read(b []byte) (int, error) {
	for len(l.buf) == 0 {
		if l.err != nil {
			return 0, l.err
		}
		ch := <-l.chunks
		time.Sleep(time.Until(ch.at))
		l.buf, l.err = ch.data, ch.err
	}
	n := copy(b, l.buf)
	l.buf = l.buf[n:]
	return n, nil
}

// Медленный, но живой канал: TCP устанавливается за rtt, рукопожатие идёт
// ещё rtt, всего 2,5 и 4 с, больше coalescedAttemptTimeout. Попытка
// заменяется, а не идёт параллельно, но такую не обрывают: срок растёт со
// временем установки TCP и продлевается, пока сервер присылает байты.
func TestCoalesceDialsSlowAliveHandshake(t *testing.T) {
	addr := okServer(t)
	for _, rtt := range []time.Duration{1250 * time.Millisecond, 2 * time.Second} {
		var d dialCounter
		mk := CoalesceDials(func() http.RoundTripper {
			return NewTransport(func(ctx context.Context) (net.Conn, error) {
				d.dials.Add(1)
				select {
				case <-time.After(rtt):
				case <-ctx.Done():
					return nil, ctx.Err()
				}
				c, err := (&net.Dialer{}).DialContext(ctx, "tcp", addr)
				if err != nil {
					return nil, err
				}
				return newLagConn(d.track(c), rtt), nil
			}, func(ctx context.Context, c net.Conn, _ bool) (net.Conn, error) {
				tc := tls.Client(c, &tls.Config{NextProtos: []string{"h2"}, InsecureSkipVerify: true})
				if err := tc.HandshakeContext(ctx); err != nil {
					return nil, err
				}
				return tc, nil
			}, nil, nil, 0)
		})
		rt := mk()
		ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		start := time.Now()
		err := roundTrip(rt, ctx)
		cancel()
		rt.(*coalescingTransport).CloseIdleConnections()
		if err != nil {
			t.Fatalf("rtt %v: request over a slow live path failed after %v: %v", rtt, time.Since(start), err)
		}
		// Ведущий и, может быть, запасной: TCP идёт дольше coalescedBackupAfter.
		if got := d.dials.Load(); got > 2 {
			t.Fatalf("rtt %v: %d dials, want no attempt cut on a live path", rtt, got)
		}
	}
}

// helloCounter — прокси перед addr, который считает соединения, приславшие
// хоть один байт, то есть начавшие рукопожатие, и пускает их к серверу через
// delay: рукопожатие идёт не мгновенно.
func helloCounter(t *testing.T, addr string, delay time.Duration, hellos *atomic.Int64) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { ln.Close() })
	go func() {
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			go func() {
				defer c.Close()
				first := make([]byte, 1)
				if _, err := io.ReadFull(c, first); err != nil {
					return
				}
				hellos.Add(1)
				time.Sleep(delay)
				u, err := net.Dial("tcp", addr)
				if err != nil {
					return
				}
				defer u.Close()
				u.Write(first)
				go func() { io.Copy(u, c); u.Close() }()
				io.Copy(c, u)
			}()
		}
	}()
	return ln.Addr().String()
}

func setSpareIdle(t *testing.T, d time.Duration) {
	t.Helper()
	old := coalescedSpareIdle
	coalescedSpareIdle = d
	t.Cleanup(func() { coalescedSpareIdle = old })
}

// slowTCPFactory — фабрика, у которой TCP ставится 300 мс, а запасной dial
// начинается через 50: до TCP доходят оба, и оба проходят рукопожатие.
func slowTCPFactory(t *testing.T, d *dialCounter, handler http.HandlerFunc, hellos *atomic.Int64) TransportMaker {
	t.Helper()
	setBackupAfter(t, 50*time.Millisecond)
	addr := helloCounter(t, h2TestServer(t, 0, handler), 300*time.Millisecond, hellos)
	return dialFactory(t, d, func(int64) string { return addr }, func(int64) time.Duration { return 300 * time.Millisecond }, nil)
}

func okHandler(w http.ResponseWriter, r *http.Request) { w.Write([]byte("ok")) }

// TCP устанавливается дольше coalescedBackupAfter (DNS на первом dial'е,
// dialer-proxy, большой RTT), и запасной dial тоже до него доходит. Как у
// браузера, оба проходят рукопожатие; запросы идут по одному соединению, а
// второе простаивает coalescedSpareIdle и закрывается. Рабочее остаётся.
func TestCoalesceDialsSpareHandshakeAfterSlowTCP(t *testing.T) {
	setSpareIdle(t, time.Second)
	var mu sync.Mutex
	served := map[string]struct{}{}
	var hellos atomic.Int64
	var d dialCounter
	rt := slowTCPFactory(t, &d, func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		served[r.RemoteAddr] = struct{}{}
		mu.Unlock()
		w.Write([]byte("ok"))
	}, &hellos)()
	if failed, _ := burst(rt, 30, 5*time.Second); failed != 0 {
		t.Fatalf("%d of 30 requests failed", failed)
	}
	waitFor(t, "the handshake of the second dial", 3*time.Second, func() bool { return hellos.Load() == 2 })
	// Рукопожатие второго ещё идёт или его соединение уже простаивает: оно
	// открыто, а не оборвано.
	time.Sleep(500 * time.Millisecond)
	if got := d.open.Load(); got != 2 {
		t.Fatalf("%d conns open while the second one idles, want 2", got)
	}
	if failed, _ := burst(rt, 30, 5*time.Second); failed != 0 {
		t.Fatalf("%d of 30 requests failed with the second conn idle", failed)
	}
	waitFor(t, "the idle conn to close", 3*time.Second, func() bool { return d.open.Load() == 1 })
	time.Sleep(200 * time.Millisecond)
	if err := roundTrip(rt, context.Background()); err != nil {
		t.Fatalf("request after the idle conn closed: %v", err)
	}
	if got := d.open.Load(); got != 1 {
		t.Fatalf("%d conns open after the idle one closed, want the working one", got)
	}
	if got := d.dials.Load(); got != 2 {
		t.Fatalf("%d dials, want the leader and its backup", got)
	}
	mu.Lock()
	defer mu.Unlock()
	if len(served) != 1 {
		t.Fatalf("requests went over %d conns, want 1", len(served))
	}
}

// На транспорт простаивает не больше одного соединения: новое закрывает
// прежнее, и частые холодные dial'ы на медленном канале их не копят.
func TestCoalesceDialsOneSpareForTransport(t *testing.T) {
	var hellos atomic.Int64
	var d dialCounter
	rt := slowTCPFactory(t, &d, okHandler, &hellos)()
	for i := int64(1); i <= 3; i++ {
		if failed, _ := burst(rt, 4, 5*time.Second); failed != 0 {
			t.Fatalf("round %d: %d of 4 requests failed", i, failed)
		}
		waitFor(t, "the handshake of the second dial", 3*time.Second, func() bool { return hellos.Load() == 2*i })
		// Рабочее соединение и одно простаивающее.
		waitFor(t, "the older idle conn to close", 3*time.Second, func() bool { return d.open.Load() == 2 })
		time.Sleep(300 * time.Millisecond)
		if got := d.open.Load(); got != 2 {
			t.Fatalf("round %d: %d conns open, want the working one and one idle", i, got)
		}
		// Рабочее соединение пропало, простаивающее остаётся.
		rt.(*coalescingTransport).Transport.CloseIdleConnections()
		rt.(*coalescingTransport).Transport.CloseHttp2Connections()
		waitFor(t, "the working conn to close", 3*time.Second, func() bool { return d.open.Load() == 1 })
	}
}

// noteConn сообщает, что его закрыли.
type noteConn struct {
	net.Conn
	once   sync.Once
	closed chan struct{}
}

func newNoteConn(t *testing.T) (*noteConn, net.Conn) {
	c, peer := net.Pipe()
	t.Cleanup(func() { c.Close(); peer.Close() })
	return &noteConn{Conn: c, closed: make(chan struct{})}, peer
}

func (c *noteConn) Close() error {
	c.once.Do(func() { close(c.closed) })
	return c.Conn.Close()
}

func (c *noteConn) closedWithin(d time.Duration) bool {
	select {
	case <-c.closed:
		return true
	case <-time.After(d):
		return false
	}
}

func (c *noteConn) isClosed() bool {
	select {
	case <-c.closed:
		return true
	default:
		return false
	}
}

// spareAttempt — попытка и её контекст, как их собирает lead.
func spareAttempt(ctx context.Context, stop <-chan struct{}) (*dialAttempt, context.Context) {
	a := newDialAttempt(stop, nil)
	return a, context.WithValue(ctx, dialAttemptKey{}, a)
}

// spareDials — два dial'а попытки на net.Pipe, как XHTTP без TLS, при запасном
// через 50 мс: ведущий поднимает TCP на 300-й мс и возвращается на 500-й,
// запасной возвращается на 150-й. peer — другой конец соединения ведущего.
func spareDials(t *testing.T) (dial func(context.Context, string, string) (net.Conn, error), leader, backup *noteConn, peer net.Conn) {
	setBackupAfter(t, 50*time.Millisecond)
	var n atomic.Int64
	leader, peer = newNoteConn(t)
	backup, _ = newNoteConn(t)
	dial = func(ctx context.Context, _, _ string) (net.Conn, error) {
		if n.Add(1) == 1 {
			time.Sleep(300 * time.Millisecond)
			c := DialTCPUp(ctx, leader)
			time.Sleep(200 * time.Millisecond)
			return c, nil
		}
		time.Sleep(100 * time.Millisecond)
		return DialTCPUp(ctx, backup), nil
	}
	return dial, leader, backup, peer
}

// XHTTP без TLS: оба dial'а попытки возвращаются удачно. Попытка отдаёт
// соединение того, кто вернулся первым, а соединение другого не достаётся
// net/http, не закрывается сразу и закрывается через coalescedSpareIdle.
func TestCoalesceDialsSpareWithoutTLS(t *testing.T) {
	setSpareIdle(t, 700*time.Millisecond)
	dial, leader, backup, _ := spareDials(t)
	a, ctx := spareAttempt(context.Background(), nil)
	conn, err := dialWithBackup(ctx, a, dial, "tcp", "x")
	if err != nil {
		t.Fatal(err)
	}
	if c, ok := conn.(*countingConn); !ok || c.Conn != net.Conn(backup) {
		t.Fatalf("the attempt returned %T, want the conn of the dial that returned first", conn)
	}
	// Ведущий вернётся через 350 мс и простоит ещё 700.
	if leader.closedWithin(700 * time.Millisecond) {
		t.Fatal("the conn of the other dial was closed at once")
	}
	if !leader.closedWithin(2 * time.Second) {
		t.Fatal("the conn of the other dial was not closed after coalescedSpareIdle")
	}
	if backup.isClosed() {
		t.Fatal("the conn the attempt returned was closed")
	}
}

// Сервер закрывает простаивающее соединение раньше срока: оно закрывается в
// ответ, не дожидаясь coalescedSpareIdle.
func TestCoalesceDialsSpareClosedByServer(t *testing.T) {
	dial, leader, _, peer := spareDials(t)
	a, ctx := spareAttempt(context.Background(), nil)
	if _, err := dialWithBackup(ctx, a, dial, "tcp", "x"); err != nil {
		t.Fatal(err)
	}
	if leader.closedWithin(600 * time.Millisecond) {
		t.Fatal("the conn of the other dial was closed at once")
	}
	_ = peer.Close()
	if !leader.closedWithin(2 * time.Second) {
		t.Fatal("the idle conn outlived the server's close")
	}
}

// Контекст попытки отменён, когда один dial уже отдал соединение (обрыв,
// успевший до конца попытки, или закрытие транспорта, пока net/http ещё не
// принял соединение): простаивающее соединение другого закрывается.
func TestCoalesceDialsSpareClosedWithAttempt(t *testing.T) {
	dial, leader, _, _ := spareDials(t)
	parent, abort := context.WithCancel(context.Background())
	defer abort()
	a, ctx := spareAttempt(parent, nil)
	if _, err := dialWithBackup(ctx, a, dial, "tcp", "x"); err != nil {
		t.Fatal(err)
	}
	if leader.closedWithin(600 * time.Millisecond) {
		t.Fatal("the conn of the other dial was closed at once")
	}
	abort()
	if !leader.closedWithin(2 * time.Second) {
		t.Fatal("the idle conn outlived its attempt")
	}
}

// Транспорт закрывает соединения (клиент закрыт, соединение отставлено, сеть
// сменилась): простаивающее вне пула net/http закрывается тоже, а транспорт
// остаётся рабочим, и следующее закрытие закрывает следующее простаивающее.
func TestCoalesceDialsSpareClosedWithTransport(t *testing.T) {
	var hellos atomic.Int64
	var d dialCounter
	rt := slowTCPFactory(t, &d, okHandler, &hellos)()
	for i := int64(1); i <= 2; i++ {
		if failed, _ := burst(rt, 8, 5*time.Second); failed != 0 {
			t.Fatalf("round %d: %d of 8 requests failed", i, failed)
		}
		waitFor(t, "the handshake of the second dial", 3*time.Second, func() bool { return hellos.Load() == 2*i })
		time.Sleep(500 * time.Millisecond)
		if got := d.open.Load(); got != 2 {
			t.Fatalf("round %d: %d conns open while the second one idles, want 2", i, got)
		}
		httputils.CloseTransport(rt)
		waitFor(t, "both conns to close with the transport", 2*time.Second, func() bool { return d.open.Load() == 0 })
	}
}

// Транспорт закрыт, пока другой dial попытки не вернулся: начатое рукопожатие
// не обрывается на полпути, а его соединение закрывается сразу, как готово.
func TestCoalesceDialsSpareDialOutlivesTransportClose(t *testing.T) {
	setBackupAfter(t, 50*time.Millisecond)
	leader, _ := newNoteConn(t)
	backup, _ := newNoteConn(t)
	var n atomic.Int64
	var cut atomic.Bool
	dial := func(ctx context.Context, _, _ string) (net.Conn, error) {
		if n.Add(1) == 1 {
			select {
			case <-ctx.Done():
				cut.Store(true)
				return nil, ctx.Err()
			case <-time.After(500 * time.Millisecond):
				return leader, nil
			}
		}
		return backup, nil
	}
	stop := make(chan struct{})
	close(stop)
	a, ctx := spareAttempt(context.Background(), stop)
	if _, err := dialWithBackup(ctx, a, dial, "tcp", "x"); err != nil {
		t.Fatal(err)
	}
	if !leader.closedWithin(2 * time.Second) {
		t.Fatal("the conn of the other dial was left open after the transport closed")
	}
	if cut.Load() {
		t.Fatal("the other dial was cut in mid-handshake")
	}
	if backup.isClosed() {
		t.Fatal("the conn the attempt returned was closed")
	}
}

// Другой dial попытки не дозвонился к её сроку (coalescedDialTimeout от
// начала попытки): он обрывается тогда, а не в момент, когда первый отдал
// соединение. Соединение, которое он всё же вернул, закрывается.
func TestCoalesceDialsSpareDialBoundedByAttempt(t *testing.T) {
	setDialTimeout(t, time.Second)
	setBackupAfter(t, 50*time.Millisecond)
	late, _ := newNoteConn(t)
	backup, _ := newNoteConn(t)
	var n atomic.Int64
	cancelled := make(chan time.Duration, 1)
	start := time.Now()
	dial := func(ctx context.Context, _, _ string) (net.Conn, error) {
		if n.Add(1) == 1 {
			<-ctx.Done()
			cancelled <- time.Since(start)
			return late, nil
		}
		return backup, nil
	}
	a, ctx := spareAttempt(context.Background(), nil)
	if _, err := dialWithBackup(ctx, a, dial, "tcp", "x"); err != nil {
		t.Fatal(err)
	}
	select {
	case took := <-cancelled:
		if took < 900*time.Millisecond {
			t.Fatalf("the other dial was cut %v into the attempt, want it to run to the attempt's bound", took)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("the other dial was never cut")
	}
	if !late.closedWithin(time.Second) {
		t.Fatal("the conn returned after the cut was left open")
	}
}

// Попытку оборвали, пока другой dial ещё идёт: он обрывается сразу, а не по
// сроку попытки.
func TestCoalesceDialsSpareDialCutWithAttempt(t *testing.T) {
	setBackupAfter(t, 50*time.Millisecond)
	backup, _ := newNoteConn(t)
	var n atomic.Int64
	cut := make(chan struct{})
	dial := func(ctx context.Context, _, _ string) (net.Conn, error) {
		if n.Add(1) == 1 {
			<-ctx.Done()
			close(cut)
			return nil, ctx.Err()
		}
		return backup, nil
	}
	parent, abort := context.WithCancel(context.Background())
	defer abort()
	a, ctx := spareAttempt(parent, nil)
	if _, err := dialWithBackup(ctx, a, dial, "tcp", "x"); err != nil {
		t.Fatal(err)
	}
	select {
	case <-cut:
		t.Fatal("the other dial was cut although its attempt goes on")
	case <-time.After(300 * time.Millisecond):
	}
	abort()
	select {
	case <-cut:
	case <-time.After(2 * time.Second):
		t.Fatal("the other dial outlived its attempt")
	}
}

// Попытку оборвали раньше срока запасного: запасной не начинается.
func TestCoalesceDialsNoBackupForCutAttempt(t *testing.T) {
	setBackupAfter(t, 100*time.Millisecond)
	var n atomic.Int64
	dial := func(ctx context.Context, _, _ string) (net.Conn, error) {
		n.Add(1)
		time.Sleep(400 * time.Millisecond)
		return nil, ctx.Err()
	}
	parent, abort := context.WithCancel(context.Background())
	a, ctx := spareAttempt(parent, nil)
	abort()
	if _, err := dialWithBackup(ctx, a, dial, "tcp", "x"); err == nil {
		t.Fatal("a cut attempt returned a conn")
	}
	if got := n.Load(); got != 1 {
		t.Fatalf("%d dials, want one: the attempt was cut before its backup was due", got)
	}
}

// Ведущий отказал уже после того, как начался запасной: попытка отдаёт
// соединение запасного.
func TestCoalesceDialsBackupWinsAfterLeaderFails(t *testing.T) {
	setBackupAfter(t, 50*time.Millisecond)
	backup, _ := newNoteConn(t)
	var n atomic.Int64
	refused := errors.New("refused")
	dial := func(ctx context.Context, _, _ string) (net.Conn, error) {
		if n.Add(1) == 1 {
			time.Sleep(300 * time.Millisecond)
			return nil, refused
		}
		time.Sleep(350 * time.Millisecond)
		return backup, nil
	}
	a, ctx := spareAttempt(context.Background(), nil)
	conn, err := dialWithBackup(ctx, a, dial, "tcp", "x")
	if err != nil {
		t.Fatalf("the attempt failed with %v while its backup dial succeeded", err)
	}
	if conn != net.Conn(backup) || backup.isClosed() {
		t.Fatalf("the attempt returned %T (closed %v), want the backup's open conn", conn, backup.isClosed())
	}
}

// Обёртка соединения долго занята своим до первой записи (например, ищет
// ECH-ключи в DNS): пока серверу ничего не отправлено, его тишина попытку не
// обрывает.
func TestCoalesceDialsSilenceCountsFromFirstWrite(t *testing.T) {
	old := coalescedAttemptTimeout
	coalescedAttemptTimeout = 300 * time.Millisecond
	t.Cleanup(func() { coalescedAttemptTimeout = old })
	addr := okServer(t)
	var d dialCounter
	mk := dialFactory(t, &d, func(int64) string { return addr }, func(int64) time.Duration { return 0 }, func(int64) time.Duration {
		return 900 * time.Millisecond
	})
	if failed, _ := burst(mk(), 8, 5*time.Second); failed != 0 {
		t.Fatalf("%d of 8 requests failed", failed)
	}
	if got := d.dials.Load(); got != 1 {
		t.Fatalf("%d dials, want one: nothing was sent while the attempt was cut", got)
	}
}

// Обёртка так ничего и не отправила: тишина сервера попытку не обрывает, её
// обрывает срок попытки целиком, и следующая начинается.
func TestCoalesceDialsNothingWrittenEndsAtDialTimeout(t *testing.T) {
	setDialTimeout(t, 500*time.Millisecond)
	addr := okServer(t)
	var d dialCounter
	mk := dialFactory(t, &d, func(int64) string { return addr }, func(int64) time.Duration { return 0 }, func(n int64) time.Duration {
		if n == 1 {
			return 5 * time.Second
		}
		return 0
	})
	start := time.Now()
	if failed, _ := burst(mk(), 8, 5*time.Second); failed != 0 {
		t.Fatalf("%d of 8 requests failed", failed)
	}
	if took := time.Since(start); took < 500*time.Millisecond || took > 2*time.Second {
		t.Fatalf("burst took %v, want the first attempt cut at its bound and the next one through", took)
	}
	if got := d.dials.Load(); got != 2 {
		t.Fatalf("%d dials, want the one that wrote nothing and the next", got)
	}
}

// Соединение ведущего занято его же долгим запросом (лимит в один поток), а
// RoundTrip ведущего не вернётся до конца сессии. Запрос, пришедший, пока
// удачный ведущий держит место, получает от net/http занятое соединение,
// ищет заново и дозванивается сам сразу, а не ждёт ведущего до конца его
// сессии.
func TestCoalesceDialsFollowerOfLongLeader(t *testing.T) {
	release := make(chan struct{})
	addr := h2TestServer(t, 1, func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/long" {
			w.WriteHeader(http.StatusOK)
			w.(http.Flusher).Flush()
			select {
			case <-release:
			case <-r.Context().Done():
			}
			return
		}
		w.Write([]byte("ok"))
	})
	var d dialCounter
	rt := coalescedTestTransport(t, addr, &d, 0)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, "GET", "https://example.com/long", nil)
	if err != nil {
		t.Fatal(err)
	}
	resp, err := rt.RoundTrip(req)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		close(release)
		resp.Body.Close()
	}()

	start := time.Now()
	ctx2, cancel2 := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel2()
	if err := roundTrip(rt, ctx2); err != nil {
		t.Fatalf("request behind a busy leader connection failed after %v: %v", time.Since(start), err)
	}
	if took := time.Since(start); took > time.Second {
		t.Fatalf("request behind a busy leader connection took %v", took)
	}
	if got := d.dials.Load(); got != 2 {
		t.Fatalf("%d dials, want the leader and one for the second request", got)
	}
}
