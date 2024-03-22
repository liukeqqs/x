package direct

import (
	"context"
	"net"

	"github.com/liukeqqs/core/dialer"
	"github.com/liukeqqs/core/logger"
	md "github.com/liukeqqs/core/metadata"
	"github.com/liukeqqs/x/registry"
)

func init() {
	registry.DialerRegistry().Register("direct", NewDialer)
	registry.DialerRegistry().Register("virtual", NewDialer)
}

type directDialer struct {
	logger logger.Logger
}

func NewDialer(opts ...dialer.Option) dialer.Dialer {
	options := &dialer.Options{}
	for _, opt := range opts {
		opt(options)
	}

	return &directDialer{
		logger: options.Logger,
	}
}

func (d *directDialer) Init(md md.Metadata) (err error) {
	return
}

func (d *directDialer) Dial(ctx context.Context, addr string, opts ...dialer.DialOption) (net.Conn, error) {
	return &conn{}, nil
}
