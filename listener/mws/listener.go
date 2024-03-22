package mws

import (
	"context"
	"crypto/tls"
	"net"
	"net/http"
	"net/http/httputil"
	"time"

	"github.com/liukeqqs/core/listener"
	"github.com/liukeqqs/core/logger"
	md "github.com/liukeqqs/core/metadata"
	"github.com/gorilla/websocket"
	admission "github.com/liukeqqs/x/admission/wrapper"
	xnet "github.com/liukeqqs/x/internal/net"
	"github.com/liukeqqs/x/internal/net/proxyproto"
	"github.com/liukeqqs/x/internal/util/mux"
	ws_util "github.com/liukeqqs/x/internal/util/ws"
	climiter "github.com/liukeqqs/x/limiter/conn/wrapper"
	limiter "github.com/liukeqqs/x/limiter/traffic/wrapper"
	metrics "github.com/liukeqqs/x/metrics/wrapper"
	"github.com/liukeqqs/x/registry"
	stats "github.com/liukeqqs/x/stats/wrapper"
)

func init() {
	registry.ListenerRegistry().Register("mws", NewListener)
	registry.ListenerRegistry().Register("mwss", NewTLSListener)
}

type mwsListener struct {
	addr       net.Addr
	upgrader   *websocket.Upgrader
	srv        *http.Server
	cqueue     chan net.Conn
	errChan    chan error
	tlsEnabled bool
	logger     logger.Logger
	md         metadata
	options    listener.Options
}

func NewListener(opts ...listener.Option) listener.Listener {
	options := listener.Options{}
	for _, opt := range opts {
		opt(&options)
	}
	return &mwsListener{
		logger:  options.Logger,
		options: options,
	}
}

func NewTLSListener(opts ...listener.Option) listener.Listener {
	options := listener.Options{}
	for _, opt := range opts {
		opt(&options)
	}
	return &mwsListener{
		tlsEnabled: true,
		logger:     options.Logger,
		options:    options,
	}
}

func (l *mwsListener) Init(md md.Metadata) (err error) {
	if err = l.parseMetadata(md); err != nil {
		return
	}

	l.upgrader = &websocket.Upgrader{
		HandshakeTimeout:  l.md.handshakeTimeout,
		ReadBufferSize:    l.md.readBufferSize,
		WriteBufferSize:   l.md.writeBufferSize,
		EnableCompression: l.md.enableCompression,
		CheckOrigin:       func(r *http.Request) bool { return true },
	}

	path := l.md.path
	if path == "" {
		path = defaultPath
	}
	mux := http.NewServeMux()
	mux.Handle(path, http.HandlerFunc(l.upgrade))
	l.srv = &http.Server{
		Addr:              l.options.Addr,
		Handler:           mux,
		ReadHeaderTimeout: l.md.readHeaderTimeout,
	}

	l.cqueue = make(chan net.Conn, l.md.backlog)
	l.errChan = make(chan error, 1)

	network := "tcp"
	if xnet.IsIPv4(l.options.Addr) {
		network = "tcp4"
	}

	lc := net.ListenConfig{}
	if l.md.mptcp {
		lc.SetMultipathTCP(true)
		l.logger.Debugf("mptcp enabled: %v", lc.MultipathTCP())
	}
	ln, err := lc.Listen(context.Background(), network, l.options.Addr)
	if err != nil {
		return
	}
	ln = proxyproto.WrapListener(l.options.ProxyProtocol, ln, 10*time.Second)
	ln = metrics.WrapListener(l.options.Service, ln)
	ln = stats.WrapListener(ln, l.options.Stats)
	ln = admission.WrapListener(l.options.Admission, ln)
	ln = limiter.WrapListener(l.options.TrafficLimiter, ln)
	ln = climiter.WrapListener(l.options.ConnLimiter, ln)

	if l.tlsEnabled {
		ln = tls.NewListener(ln, l.options.TLSConfig)
	}

	l.addr = ln.Addr()

	go func() {
		err := l.srv.Serve(ln)
		if err != nil {
			l.errChan <- err
		}
		close(l.errChan)
	}()

	return
}

func (l *mwsListener) Accept() (conn net.Conn, err error) {
	var ok bool
	select {
	case conn = <-l.cqueue:
	case err, ok = <-l.errChan:
		if !ok {
			err = listener.ErrClosed
		}
	}
	return
}

func (l *mwsListener) Close() error {
	return l.srv.Close()
}

func (l *mwsListener) Addr() net.Addr {
	return l.addr
}

func (l *mwsListener) upgrade(w http.ResponseWriter, r *http.Request) {
	log := l.logger.WithFields(map[string]any{
		"local":  l.addr.String(),
		"remote": r.RemoteAddr,
	})
	if l.logger.IsLevelEnabled(logger.TraceLevel) {
		dump, _ := httputil.DumpRequest(r, false)
		log.Trace(string(dump))
	}

	conn, err := l.upgrader.Upgrade(w, r, l.md.header)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		log.Error(err)
		return
	}

	l.mux(ws_util.Conn(conn), log)
}

func (l *mwsListener) mux(conn net.Conn, log logger.Logger) {
	defer conn.Close()

	session, err := mux.ServerSession(conn, l.md.muxCfg)
	if err != nil {
		log.Error(err)
		return
	}
	defer session.Close()

	for {
		stream, err := session.Accept()
		if err != nil {
			log.Error("accept stream: ", err)
			return
		}

		select {
		case l.cqueue <- stream:
		default:
			stream.Close()
			log.Warnf("connection queue is full, client %s discarded", stream.RemoteAddr())
		}
	}
}
