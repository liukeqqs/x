package tls

import (
	"context"
	"net"
	"time"

	"github.com/liukeqqs/core/listener"
	"github.com/liukeqqs/core/logger"
	md "github.com/liukeqqs/core/metadata"
	admission "github.com/liukeqqs/x/admission/wrapper"
	xnet "github.com/liukeqqs/x/internal/net"
	"github.com/liukeqqs/x/internal/net/proxyproto"
	climiter "github.com/liukeqqs/x/limiter/conn/wrapper"
	limiter "github.com/liukeqqs/x/limiter/traffic/wrapper"
	metrics "github.com/liukeqqs/x/metrics/wrapper"
	"github.com/liukeqqs/x/registry"
	stats "github.com/liukeqqs/x/stats/wrapper"
)

func init() {
	registry.ListenerRegistry().Register("otls", NewListener)
}

type obfsListener struct {
	net.Listener
	logger  logger.Logger
	md      metadata
	options listener.Options
}

func NewListener(opts ...listener.Option) listener.Listener {
	options := listener.Options{}
	for _, opt := range opts {
		opt(&options)
	}
	return &obfsListener{
		logger:  options.Logger,
		options: options,
	}
}

func (l *obfsListener) Init(md md.Metadata) (err error) {
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

	l.Listener = ln
	return
}

func (l *obfsListener) Accept() (net.Conn, error) {
	c, err := l.Listener.Accept()
	if err != nil {
		return nil, err
	}

	return &obfsTLSConn{
		Conn: c,
	}, nil
}
