package mtls

import (
	"context"
	"crypto/tls"
	"net"
	"time"

	"github.com/liukeqqs/core/listener"
	"github.com/liukeqqs/core/logger"
	md "github.com/liukeqqs/core/metadata"
	admission "github.com/liukeqqs/x/admission/wrapper"
	xnet "github.com/liukeqqs/x/internal/net"
	"github.com/liukeqqs/x/internal/net/proxyproto"
	"github.com/liukeqqs/x/internal/util/mux"
	climiter "github.com/liukeqqs/x/limiter/conn/wrapper"
	limiter "github.com/liukeqqs/x/limiter/traffic/wrapper"
	metrics "github.com/liukeqqs/x/metrics/wrapper"
	"github.com/liukeqqs/x/registry"
	stats "github.com/liukeqqs/x/stats/wrapper"
)

func init() {
	registry.ListenerRegistry().Register("mtls", NewListener)
}

type mtlsListener struct {
	net.Listener
	cqueue  chan net.Conn
	errChan chan error
	logger  logger.Logger
	md      metadata
	options listener.Options
}

func NewListener(opts ...listener.Option) listener.Listener {
	options := listener.Options{}
	for _, opt := range opts {
		opt(&options)
	}
	return &mtlsListener{
		logger:  options.Logger,
		options: options,
	}
}

func (l *mtlsListener) Init(md md.Metadata) (err error) {
	if err = l.parseMetadata(md); err != nil {
		return
	}

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
	l.Listener = tls.NewListener(ln, l.options.TLSConfig)

	l.cqueue = make(chan net.Conn, l.md.backlog)
	l.errChan = make(chan error, 1)

	go l.listenLoop()

	return
}

func (l *mtlsListener) Accept() (conn net.Conn, err error) {
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

func (l *mtlsListener) listenLoop() {
	for {
		conn, err := l.Listener.Accept()
		if err != nil {
			l.errChan <- err
			close(l.errChan)
			return
		}
		go l.mux(conn)
	}
}

func (l *mtlsListener) mux(conn net.Conn) {
	defer conn.Close()

	session, err := mux.ServerSession(conn, l.md.muxCfg)
	if err != nil {
		l.logger.Error(err)
		return
	}
	defer session.Close()

	for {
		stream, err := session.Accept()
		if err != nil {
			l.logger.Error("accept stream: ", err)
			return
		}

		select {
		case l.cqueue <- stream:
		default:
			stream.Close()
			l.logger.Warnf("connection queue is full, client %s discarded", stream.RemoteAddr())
		}
	}
}
