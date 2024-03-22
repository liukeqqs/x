package serial

import (
	"context"
	"net"

	"github.com/liukeqqs/core/dialer"
	"github.com/liukeqqs/core/logger"
	md "github.com/liukeqqs/core/metadata"
	serial "github.com/liukeqqs/x/internal/util/serial"
	"github.com/liukeqqs/x/registry"
)

func init() {
	registry.DialerRegistry().Register("serial", NewDialer)
}

type serialDialer struct {
	md     metadata
	logger logger.Logger
}

func NewDialer(opts ...dialer.Option) dialer.Dialer {
	options := &dialer.Options{}
	for _, opt := range opts {
		opt(options)
	}

	return &serialDialer{
		logger: options.Logger,
	}
}

func (d *serialDialer) Init(md md.Metadata) (err error) {
	return d.parseMetadata(md)
}

func (d *serialDialer) Dial(ctx context.Context, addr string, opts ...dialer.DialOption) (net.Conn, error) {
	var options dialer.DialOptions
	for _, opt := range opts {
		opt(&options)
	}

	cfg := serial.ParseConfigFromAddr(addr)
	port, err := serial.OpenPort(cfg)
	if err != nil {
		return nil, err
	}

	return serial.NewConn(port, &serial.Addr{Port: cfg.Name}, nil), nil
}
