// ss包代码 - 修正版
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

func init() {
	registry.HandlerRegistry().Register("ss", NewHandler)
}

type ssHandler struct {
	cipher  core.Cipher
	router  *chain.Router
	md      metadata // 假设metadata结构体在其他文件中定义
	options handler.Options
	bypass  bool // 新增字段，标记是否启用了bypass过滤
}

func NewHandler(opts ...handler.Option) handler.Handler {
	options := handler.Options{}
	for _, opt := range opts {
		opt(&options)
	}

	return &ssHandler{
		options: options,
	}
}

func (h *ssHandler) Init(md md.Metadata) (err error) {
	if err = h.parseMetadata(md); err != nil { // 假设parseMetadata方法在其他文件中实现
		return
	}
	if h.options.Auth != nil {
		method := h.options.Auth.Username()
		password, _ := h.options.Auth.Password()
		h.cipher, err = ss.ShadowCipher(method, password, h.md.key)
		if err != nil {
			return
		}
	}

	h.router = h.options.Router
	if h.router == nil {
		h.router = chain.NewRouter(chain.LoggerRouterOption(h.options.Logger))
	}

	// 初始化bypass字段
	h.bypass = h.options.Bypass != nil

	return
}

func (h *ssHandler) Handle(ctx context.Context, conn net.Conn, opts ...handler.HandleOption) error {
	defer conn.Close()

	start := time.Now()
	log := h.options.Logger.WithFields(map[string]any{
		"remote": conn.RemoteAddr().String(),
		"local":  conn.LocalAddr().String(),
		"【SSR请求===sid===】": ctxvalue.SidFromContext(ctx),
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

	// 记录可能的bypass情况
	if h.bypass && h.options.Bypass.Contains(ctx, "tcp", addr.String()) {
		log.Debug("bypass: ", addr.String())

		// 记录被bypass的请求流量
		sid := string(ctxvalue.SidFromContext(ctx))
		netpkg.RecordBypassTraffic(addr.String(), sid, conn.LocalAddr())

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

	// 使用 TransportWithStats 替代 Transport1
	err = netpkg.TransportWithStats(
		conn,        // 客户端连接
		cc,          // 目标服务器连接
		addr.String(), // 目标地址（如 example.com:443）
		string(ctxvalue.SidFromContext(ctx)), // 会话ID
		localPort,   // 代理本地端口（如 1080）
	)

	log.WithFields(map[string]any{
		"duration": time.Since(t),
	})
	log.Infof("%s >-< %s", conn.RemoteAddr(), addr)

	return err // 返回实际的错误，而不是总是nil
}

func (h *ssHandler) checkRateLimit(addr net.Addr) bool {
	if h.options.RateLimiter == nil {
		return true
	}
	host, _, _ := net.SplitHostPort(addr.String())
	if limiter := h.options.RateLimiter.Limiter(host); limiter != nil {
		return limiter.Allow(1)
	}

	return true
}