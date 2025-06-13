// net包完整代码
package net

import (
	"bufio"
	"github.com/liukeqqs/core/common/bufpool"
	"io"
	"log"
	"net"
	"strings"
	"sync"
	"time"
)

const (
	bufferSize    = 64 * 1024
	maxQueueSize  = 100000      // 队列最大容量
	retryInterval = 100 * time.Millisecond // 重试间隔
	maxRetries    = 5           // 最大重试次数
)

var (
	RChan       = make(chan Info, 5120)
	rchanQueue  = make(chan Info, maxQueueSize)
	queueOnce   sync.Once
	processing  = make(map[string]bool) // 记录正在处理的请求
	processingMu sync.Mutex             // 保护processing map
)

// Info 增强版流量统计结构
type Info struct {
	Address    string `json:"address"`
	LocalPort  int    `json:"localport"`
	Bytes      int64  `json:"bytes"`
	Unix       int64  `json:"unix"`
	RepeatNums int64  `json:"repeatnums"`
	SessionID  string `json:"sid,omitempty"`   // 新增会话ID
	Domain     string `json:"domain,omitempty"`// 新增域名
	RequestID  string `json:"req_id,omitempty"`// 新增请求ID
}

// 初始化后台队列处理器
func init() {
	queueOnce.Do(func() {
		go processRChanQueue()
	})
}

// 可靠队列处理核心逻辑 - 防止重复处理
func processRChanQueue() {
	for info := range rchanQueue {
		// 使用SessionID和Domain作为请求的唯一标识
		reqKey := info.SessionID + "|" + info.Domain

		// 检查请求是否正在处理中
		processingMu.Lock()
		if processing[reqKey] {
			log.Printf("[流量统计] 重复请求被过滤: SessionID=%s, Domain=%s, ReqID=%s",
				info.SessionID, info.Domain, info.RequestID)
			processingMu.Unlock()
			continue
		}
		processing[reqKey] = true
		processingMu.Unlock()

		// 处理请求，带超时的重试机制
		retryDelay := retryInterval
		var success bool

		for i := 0; i < maxRetries; i++ {
			select {
			case RChan <- info:
				success = true
				break
			default:
				log.Printf("队列阻塞，等待重试 (间隔 %v): SessionID=%s, Domain=%s, ReqID=%s",
					retryDelay, info.SessionID, info.Domain, info.RequestID)
				time.Sleep(retryDelay)
				retryDelay = time.Duration(1.5 * float64(retryDelay))
				if retryDelay > 5*time.Second {
					retryDelay = 5 * time.Second
				}
			}

			if success {
				break
			}
		}

		// 无论成功失败，都标记请求处理结束
		processingMu.Lock()
		delete(processing, reqKey)
		processingMu.Unlock()

		if !success {
			log.Printf("[流量统计] 请求处理失败，达到最大重试次数: SessionID=%s, Domain=%s, ReqID=%s",
				info.SessionID, info.Domain, info.RequestID)
		}
	}
}

func Transport(rw1, rw2 io.ReadWriter) error {
	errc := make(chan error, 1)
	go func() {
		errc <- CopyBuffer(rw1, rw2, bufferSize)
	}()

	go func() {
		errc <- CopyBuffer(rw2, rw1, bufferSize)
	}()

	if err := <-errc; err != nil && err != io.EOF {
		return err
	}
	return nil
}

func CopyBuffer(dst io.Writer, src io.Reader, bufSize int) error {
	buf := bufpool.Get(bufSize)
	defer bufpool.Put(buf)

	_, err := io.CopyBuffer(dst, src, buf)
	return err
}

// 新增：记录被bypass的请求
func RecordBypassTraffic(domain, sid string, localAddr net.Addr) {
	localPort := 0
	if tcpAddr, ok := localAddr.(*net.TCPAddr); ok {
		localPort = tcpAddr.Port
	}

	// 从SessionID中提取ReqID
	sidParts := strings.Split(sid, "|")
	reqID := ""
	if len(sidParts) > 1 {
		reqID = sidParts[1]
	}

	// 记录被bypass的请求，Bytes设为0但包含其他信息
	rchanQueue <- Info{
		Address:    domain,
		LocalPort:  localPort,
		Bytes:      0,
		Unix:       time.Now().Unix(),
		RepeatNums: 1,
		SessionID:  sidParts[0],
		Domain:     domain,
		RequestID:  reqID,
	}

	log.Printf("[Bypass流量记录] SessionID=%s | Domain=%s | ReqID=%s | 流量=0 (被过滤)",
		sidParts[0], domain, reqID)
}

// TransportWithStats 新版可靠传输实现
func TransportWithStats(rw1, rw2 io.ReadWriter, domain, sid string, localPort int) error {
	var (
		bytesUp   int64
		bytesDown int64
		startTime = time.Now()
		wg        sync.WaitGroup
		errs      []error
		mu        sync.Mutex
		done      = make(chan struct{}) // 用于通知两个goroutine停止
	)

	// 从SessionID中提取ReqID
	sidParts := strings.Split(sid, "|")
	sessionID := sidParts[0]
	reqID := ""
	if len(sidParts) > 1 {
		reqID = sidParts[1]
	}

	wg.Add(2)

	// 关闭连接时使用的函数
	closeConn := func(conn net.Conn, direction string) {
		if conn != nil {
			log.Printf("[流量统计] 关闭 %s 连接: SessionID=%s | Domain=%s | ReqID=%s",
				direction, sessionID, domain, reqID)
			if err := conn.Close(); err != nil {
				log.Printf("[流量统计] 关闭 %s 连接错误: %v, SessionID=%s, Domain=%s, ReqID=%s",
					direction, err, sessionID, domain, reqID)
			}
		}
	}

	// 上行流量采集
	go func() {
		defer wg.Done()
		defer func() {
			if r := recover(); r != nil {
				log.Printf("上行协程异常: %v, SessionID=%s, Domain=%s, ReqID=%s",
					r, sessionID, domain, reqID)
				mu.Lock()
				errs = append(errs, io.ErrClosedPipe)
				mu.Unlock()
			}
		}()

		log.Printf("[流量统计] 开始上行传输: SessionID=%s | Domain=%s | ReqID=%s",
			sessionID, domain, reqID)

		// 创建带取消功能的Reader
		pr, pw := io.Pipe()
		go func() {
			// 等待传输完成或取消信号
			<-done
			pw.Close()
		}()

		// 从源读取数据并写入管道
		go func() {
			defer pw.Close()
			_, err := io.Copy(pw, rw1)
			if err != nil {
				log.Printf("上行读取错误: %v, SessionID=%s, Domain=%s, ReqID=%s",
					err, sessionID, domain, reqID)
			}
		}()

		// 从管道读取数据并写入目标
		n, err := io.Copy(rw2, pr)
		bytesUp = n

		if err != nil {
			if err != io.EOF {
				log.Printf("上行传输错误: %v, SessionID=%s, Domain=%s, ReqID=%s",
					err, sessionID, domain, reqID)
				mu.Lock()
				errs = append(errs, err)
				mu.Unlock()

				// 发生错误时关闭两个连接并通知另一个goroutine
				if conn, ok := rw1.(net.Conn); ok {
					closeConn(conn, "客户端")
				}
				if conn, ok := rw2.(net.Conn); ok {
					closeConn(conn, "服务器")
				}

				close(done)
			}
		}

		log.Printf("[流量统计] 上行传输完成: SessionID=%s | Domain=%s | ReqID=%s | 上行=%d",
			sessionID, domain, reqID, bytesUp)
	}()

	// 下行流量采集
	go func() {
		defer wg.Done()
		defer func() {
			if r := recover(); r != nil {
				log.Printf("下行协程异常: %v, SessionID=%s, Domain=%s, ReqID=%s",
					r, sessionID, domain, reqID)
				mu.Lock()
				errs = append(errs, io.ErrClosedPipe)
				mu.Unlock()
			}
		}()

		log.Printf("[流量统计] 开始下行传输: SessionID=%s | Domain=%s | ReqID=%s",
			sessionID, domain, reqID)

		// 创建带取消功能的Reader
		pr, pw := io.Pipe()
		go func() {
			// 等待传输完成或取消信号
			<-done
			pw.Close()
		}()

		// 从源读取数据并写入管道
		go func() {
			defer pw.Close()
			_, err := io.Copy(pw, rw2)
			if err != nil {
				log.Printf("下行读取错误: %v, SessionID=%s, Domain=%s, ReqID=%s",
					err, sessionID, domain, reqID)
			}
		}()

		// 从管道读取数据并写入目标
		n, err := io.Copy(rw1, pr)
		bytesDown = n

		if err != nil {
			if err != io.EOF {
				log.Printf("下行传输错误: %v, SessionID=%s, Domain=%s, ReqID=%s",
					err, sessionID, domain, reqID)
				mu.Lock()
				errs = append(errs, err)
				mu.Unlock()

				// 发生错误时关闭两个连接并通知另一个goroutine
				if conn, ok := rw1.(net.Conn); ok {
					closeConn(conn, "客户端")
				}
				if conn, ok := rw2.(net.Conn); ok {
					closeConn(conn, "服务器")
				}

				close(done)
			}
		}

		log.Printf("[流量统计] 下行传输完成: SessionID=%s | Domain=%s | ReqID=%s | 下行=%d",
			sessionID, domain, reqID, bytesDown)
	}()

	// 等待两个goroutine都完成
	wg.Wait()

	// 收集所有错误
	var err error
	if len(errs) > 0 {
		err = errs[0] // 只返回第一个错误，实际应该处理所有错误
	}

	totalBytes := bytesUp + bytesDown
	duration := time.Since(startTime)

	// 构建增强版统计信息
	rchanQueue <- Info{
		Address:    domain,
		LocalPort:  localPort,
		Bytes:      totalBytes,
		Unix:       time.Now().Unix(),
		RepeatNums: 1,
		SessionID:  sessionID,
		Domain:     domain,
		RequestID:  reqID,
	}

	log.Printf("[流量统计] SessionID=%s | Domain=%s | ReqID=%s | 上行=%d | 下行=%d | 总流量=%d | 耗时=%v | 错误=%v",
		sessionID, domain, reqID, bytesUp, bytesDown, totalBytes, duration, err)

	return err
}

// Transport1 统一传输接口
func Transport1(rw1, rw2 io.ReadWriter, address string, sid string) error {
	var (
		bytesUp   int64
		bytesDown int64
		startTime = time.Now()
	)

	// 从SessionID中提取ReqID
	sidParts := strings.Split(sid, "|")
	sessionID := sidParts[0]
	reqID := ""
	if len(sidParts) > 1 {
		reqID = sidParts[1]
	}

	defer func() {
		total := bytesUp + bytesDown
		rchanQueue <- Info{
			Address:    address,
			Bytes:      total,
			Unix:       time.Now().Unix(),
			RepeatNums: 1,
			SessionID:  sessionID,
			RequestID:  reqID,
		}
		log.Printf("[流量统计] SessionID=%s | ReqID=%s | 上行: %d | 下行: %d | 总流量: %d | 耗时=%v",
			sessionID, reqID, bytesUp, bytesDown, total, time.Since(startTime))
	}()

	errc := make(chan error, 2)

	go func() {
		n, err := io.CopyBuffer(rw2, rw1, bufpool.Get(bufferSize))
		bytesUp = n
		errc <- err
	}()

	go func() {
		n, err := io.CopyBuffer(rw1, rw2, bufpool.Get(bufferSize))
		bytesDown = n
		errc <- err
	}()

	var errCount int
	for i := 0; i < 2; i++ {
		if err := <-errc; err != nil {
			if err != io.EOF {
				errCount++
			}
		}
	}

	if errCount > 0 {
		return io.ErrUnexpectedEOF
	}
	return nil
}

// CopyBuffer1 带日志的拷贝实现
func CopyBuffer1(dst io.Writer, src io.Reader, bufSize int, address, sid string) error {
	// 从SessionID中提取ReqID
	sidParts := strings.Split(sid, "|")
	sessionID := sidParts[0]
	reqID := ""
	if len(sidParts) > 1 {
		reqID = sidParts[1]
	}

	buf := bufpool.Get(bufSize)
	defer bufpool.Put(buf)
	bytes, err := io.CopyBuffer(dst, src, buf)
	log.Printf("[消耗流量：] SessionID=%s | ReqID=%s | Address=%s | 流量=%d",
		sessionID, reqID, address, bytes)
	rchanQueue <- Info{
		Address:    address,
		Bytes:      bytes,
		Unix:       time.Now().Unix(),
		RepeatNums: 1,
		SessionID:  sessionID,
		RequestID:  reqID,
	}
	return err
}

// CopyBufferWithStats 带统计的拷贝实现
func CopyBufferWithStats(dst io.Writer, src io.Reader, bufSize int, address, sid string) error {
	// 从SessionID中提取ReqID
	sidParts := strings.Split(sid, "|")
	sessionID := sidParts[0]
	reqID := ""
	if len(sidParts) > 1 {
		reqID = sidParts[1]
	}

	buf := bufpool.Get(bufSize)
	defer bufpool.Put(buf)

	startTime := time.Now()
	bytes, err := io.CopyBuffer(dst, src, buf)

	rchanQueue <- Info{
		Address:    address,
		Bytes:      bytes,
		Unix:       time.Now().Unix(),
		RepeatNums: 1,
		SessionID:  sessionID,
		RequestID:  reqID,
	}

	log.Printf("[流量统计] SessionID=%s | ReqID=%s | Address=%s | 传输量=%d | 耗时=%v",
		sessionID, reqID, address, bytes, time.Since(startTime))
	return err
}

type bufferReaderConn struct {
	net.Conn
	br *bufio.Reader
}

func NewBufferReaderConn(conn net.Conn, br *bufio.Reader) net.Conn {
	return &bufferReaderConn{
		Conn: conn,
		br:   br,
	}
}

func (c *bufferReaderConn) Read(b []byte) (int, error) {
	return c.br.Read(b)
}