package ss

import (
	"context"
	"io"
	"net"
	"time"

	"github.com/go-gost/gosocks5"
	"github.com/liukeqqs/core/chain"
	"github.com/liukeqqs/core/handler"
	md "github.com/liukeqqs/core/metadata"
	ctxvalue "github.com/liukeqqs/x/ctx"
	netpkg "github.com/liukeqqs/x/internal/net"
	"github.com/liukeqqs/x/internal/util/ss"
	"github.com/liukeqqs/x/registry"
	"github.com/shadowsocks/go-shadowsocks2/core"
)

func (h *ssHandler) Handle(ctx context.Context, conn net.Conn, opts ...handler.HandleOption) error {
	defer conn.Close()

	start := time.Now()
	log := h.options.Logger.WithFields(map[string]any{
		"remote": conn.RemoteAddr().String(),
		"local":  conn.LocalAddr().String(),
	})

	log.Infof("%s <> %s", conn.RemoteAddr(), conn.LocalAddr())
	defer func() {
		log.WithFields(map[string]any{
			"duration": time.Since(start),
		}).Infof("%s >< %s", conn.RemoteAddr(), conn.LocalAddr())
	}()

	if !h.checkRateLimit(conn.RemoteAddr()) {
		return nil
	}

	if h.cipher != nil {
		conn = ss.ShadowConn(h.cipher.StreamConn(conn), nil)
	}

	if h.md.readTimeout > 0 {
		conn.SetReadDeadline(time.Now().Add(h.md.readTimeout))
	}

	addr := &gosocks5.Addr{}
	if _, err := addr.ReadFrom(conn); err != nil {
		log.Error(err)
		io.Copy(io.Discard, conn)
		return err
	}

	log = log.WithFields(map[string]any{
		"dst": addr.String(),
	})

	log.Debugf("%s >> %s", conn.RemoteAddr(), addr)

	if h.options.Bypass != nil && h.options.Bypass.Contains(ctx, "tcp", addr.String()) {
		log.Debug("bypass: ", addr.String())
		return nil
	}

	switch h.md.hash {
	case "host":
		ctx = ctxvalue.ContextWithHash(ctx, &ctxvalue.Hash{Source: addr.String()})
	}

	cc, err := h.router.Dial(ctx, "tcp", addr.String())
	if err != nil {
		return err
	}
	defer cc.Close()

	// 获取本地端口
	localPort := 0
	if tcpAddr, ok := conn.LocalAddr().(*net.TCPAddr); ok {
		localPort = tcpAddr.Port
	}

	t := time.Now()
	log.Infof("%s <-> %s", conn.RemoteAddr(), addr)

	// 使用精确流量统计的传输
	netpkg.TransportWithStats(
		conn,
		cc,
		addr.String(),
		string(ctxvalue.SidFromContext(ctx)),
		localPort,
	)

	log.WithFields(map[string]any{
		"duration": time.Since(t),
	}).Infof("%s >-< %s", conn.RemoteAddr(), addr)

	return nil
}