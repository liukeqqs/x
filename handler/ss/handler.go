// ss包完整代码
package ss

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"io"
	"net"
	"sync"
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

// 新增请求缓存，防止重复请求
var (
	activeRequests = make(map[string]bool) // 记录活跃的请求
	requestMutex   sync.Mutex
)

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

// 生成唯一请求ID
func generateRequestID() string {
	b := make([]byte, 8)
	rand.Read(b)
	return hex.EncodeToString(b)
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
		// 设置读取超时，但在传输开始前取消
		conn.SetReadDeadline(time.Now().Add(h.md.readTimeout))
		defer conn.SetReadDeadline(time.Time{}) // 取消超时设置
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

	// 生成唯一请求ID
	reqID := generateRequestID()
	dstAddr := addr.String()

	// 检查是否有重复请求
	requestMutex.Lock()
	if activeRequests[dstAddr] {
		log.Printf("检测到重复请求，已过滤: %s (ReqID: %s)", dstAddr, reqID)
		requestMutex.Unlock()
		return nil
	}
	activeRequests[dstAddr] = true
	requestMutex.Unlock()

	// 请求处理完成后从活跃请求中移除
	defer func() {
		requestMutex.Lock()
		delete(activeRequests, dstAddr)
		requestMutex.Unlock()
	}()

	if h.bypass && h.options.Bypass.Contains(ctx, "tcp", dstAddr) {
		log.Debug("bypass: ", dstAddr)
		netpkg.RecordBypassTraffic(dstAddr, string(ctxvalue.SidFromContext(ctx))+"|"+reqID, conn.LocalAddr())
		return nil
	}

	switch h.md.hash {
	case "host":
		ctx = ctxvalue.ContextWithHash(ctx, &ctxvalue.Hash{Source: dstAddr})
	}

	cc, err := h.router.Dial(ctx, "tcp", dstAddr)
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
	log.Infof("%s <-> %s (ReqID: %s)", conn.RemoteAddr(), dstAddr, reqID)

	// 使用 TransportWithStats 替代 Transport1
	err = netpkg.TransportWithStats(
		conn,        // 客户端连接
		cc,          // 目标服务器连接
		dstAddr,     // 目标地址（如 example.com:443）
		string(ctxvalue.SidFromContext(ctx))+"|"+reqID, // 会话ID + 请求ID
		localPort,   // 代理本地端口（如 1080）
	)

	log.WithFields(map[string]any{
		"duration": time.Since(t),
		"req_id":   reqID,
	}).Infof("%s >-< %s", conn.RemoteAddr(), dstAddr)

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