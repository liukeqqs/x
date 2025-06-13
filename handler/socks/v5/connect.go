package v5

import (
	"context"
	"fmt"
	"net"
	"time"

	"github.com/go-gost/gosocks5"
	"github.com/liukeqqs/core/limiter/traffic"
	"github.com/liukeqqs/core/logger"
	ctxvalue "github.com/liukeqqs/x/ctx"
	netpkg "github.com/liukeqqs/x/internal/net"
	"github.com/liukeqqs/x/limiter/traffic/wrapper"
	"github.com/liukeqqs/x/stats"
	stats_wrapper "github.com/liukeqqs/x/stats/wrapper"
)

func (h *socks5Handler) handleConnect(ctx context.Context, conn net.Conn, network, address string, log logger.Logger) error {
	log = log.WithFields(map[string]any{
		"dst": fmt.Sprintf("%s/%s", address, network),
		"cmd": "connect",
	})
	log.Debugf("%s >> %s", conn.RemoteAddr(), address)

	if h.options.Bypass != nil && h.options.Bypass.Contains(ctx, network, address) {
		resp := gosocks5.NewReply(gosocks5.NotAllowed, nil)
		log.Trace(resp)
		log.Debug("bypass: ", address)
		return resp.Write(conn)
	}

	switch h.md.hash {
	case "host":
		ctx = ctxvalue.ContextWithHash(ctx, &ctxvalue.Hash{Source: address})
	}

	cc, err := h.router.Dial(ctx, network, address)
	if err != nil {
		resp := gosocks5.NewReply(gosocks5.NetUnreachable, nil)
		log.Trace(resp)
		resp.Write(conn)
		return err
	}

	defer cc.Close()

	resp := gosocks5.NewReply(gosocks5.Succeeded, nil)
	log.Trace(resp)
	if err := resp.Write(conn); err != nil {
		log.Error(err)
		return err
	}

	clientID := ctxvalue.ClientIDFromContext(ctx)
	rw := wrapper.WrapReadWriter(h.options.Limiter, conn,
		traffic.NetworkOption(network),
		traffic.AddrOption(address),
		traffic.ClientOption(string(clientID)),
		traffic.SrcOption(conn.RemoteAddr().String()),
	)
	if h.options.Observer != nil {
		pstats := h.stats.Stats(string(clientID))
		pstats.Add(stats.KindTotalConns, 1)
		pstats.Add(stats.KindCurrentConns, 1)
		defer pstats.Add(stats.KindCurrentConns, -1)
		rw = stats_wrapper.WrapReadWriter(rw, pstats)
	}
	// 获取本地端口
/* 	localPort := 0
	if tcpAddr, ok := conn.LocalAddr().(*net.TCPAddr); ok {
		localPort = tcpAddr.Port
	} */
	t := time.Now()
	log.Infof("%s <-> %s", conn.RemoteAddr(), address)
	netpkg.Transport(conn, cc)
	//netpkg.Transport1(rw, cc, address, string(ctxvalue.SidFromContext(ctx)))
		// 使用 TransportWithStats 替代 Transport1
/*     	netpkg.TransportWithStats(
    		rw,          // 客户端连接
    		cc,          // 目标服务器连接
    		address,     // 目标地址（如 example.com:443）
    		string(ctxvalue.SidFromContext(ctx)), // 会话ID
    		localPort,   // 代理本地端口（如 1080）
    	) */

	log.WithFields(map[string]any{
		"duration": time.Since(t),
	}).Infof("%s >-< %s --->%s", conn.RemoteAddr(), address, ctxvalue.SidFromContext(ctx))

	return nil
}
