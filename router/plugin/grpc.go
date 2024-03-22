package router

import (
	"context"
	"io"
	"net"

	"github.com/liukeqqs/core/logger"
	"github.com/liukeqqs/core/router"
	"github.com/go-gost/plugin/router/proto"
	"github.com/liukeqqs/x/internal/plugin"
	xrouter "github.com/liukeqqs/x/router"
	"google.golang.org/grpc"
)

type grpcPlugin struct {
	conn   grpc.ClientConnInterface
	client proto.RouterClient
	log    logger.Logger
}

// NewGRPCPlugin creates an Router plugin based on gRPC.
func NewGRPCPlugin(name string, addr string, opts ...plugin.Option) router.Router {
	var options plugin.Options
	for _, opt := range opts {
		opt(&options)
	}

	log := logger.Default().WithFields(map[string]any{
		"kind":   "router",
		"router": name,
	})
	conn, err := plugin.NewGRPCConn(addr, &options)
	if err != nil {
		log.Error(err)
	}

	p := &grpcPlugin{
		conn: conn,
		log:  log,
	}
	if conn != nil {
		p.client = proto.NewRouterClient(conn)
	}
	return p
}

func (p *grpcPlugin) GetRoute(ctx context.Context, dst net.IP, opts ...router.Option) *router.Route {
	if p.client == nil {
		return nil
	}

	r, err := p.client.GetRoute(ctx,
		&proto.GetRouteRequest{
			Dst: dst.String(),
		})
	if err != nil {
		p.log.Error(err)
		return nil
	}

	return xrouter.ParseRoute(r.Net, r.Gateway)
}

func (p *grpcPlugin) Close() error {
	if closer, ok := p.conn.(io.Closer); ok {
		return closer.Close()
	}
	return nil
}
