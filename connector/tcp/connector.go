package tcp

import (
	"context"
	"net"

	"github.com/liukeqqs/core/connector"
	md "github.com/liukeqqs/core/metadata"
	"github.com/liukeqqs/x/registry"
)

func init() {
	registry.ConnectorRegistry().Register("tcp", NewConnector)
}

type tcpConnector struct {
	options connector.Options
}

func NewConnector(opts ...connector.Option) connector.Connector {
	options := connector.Options{}
	for _, opt := range opts {
		opt(&options)
	}

	return &tcpConnector{
		options: options,
	}
}

func (c *tcpConnector) Init(md md.Metadata) (err error) {
	return nil
}

func (c *tcpConnector) Connect(ctx context.Context, conn net.Conn, network, address string, opts ...connector.ConnectOption) (net.Conn, error) {
	log := c.options.Logger.WithFields(map[string]any{
		"remote":  conn.RemoteAddr().String(),
		"local":   conn.LocalAddr().String(),
		"network": network,
		"address": address,
	})
	log.Debugf("connect %s/%s", address, network)

	return conn, nil
}
