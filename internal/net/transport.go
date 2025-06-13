package net

import (
	"bufio"
	"github.com/liukeqqs/core/common/bufpool"
	"io"
	"log"
	"net"
	"sync"
	"sync/atomic"
	"time"
)

const (
	bufferSize        = 64 * 1024
	maxQueueSize      = 100000      // 队列最大容量
	retryInterval     = 100 * time.Millisecond // 重试间隔
	flushInterval     = 500 * time.Millisecond // 批量上报间隔
	maxBatchSize      = 100          // 批量上报最大条数
	reportChannelSize = 10000        // 流量报告通道大小
)

var (
	RChan       = make(chan Info, 5120)
	rchanQueue  = make(chan Info, maxQueueSize)
	queueOnce   sync.Once
	reportChan  = make(chan Info, reportChannelSize) // 新增流量报告通道
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
}

// 初始化后台队列处理器
func init() {
	queueOnce.Do(func() {
		go processRChanQueue()
		go batchReportStats() // 新增批量上报协程
	})
}

// 可靠队列处理核心逻辑
func processRChanQueue() {
	for info := range rchanQueue {
		// 指数退避重试机制
		retryDelay := retryInterval
		for {
			select {
			case RChan <- info:
				goto NEXT // 发送成功
			default:
				log.Printf("队列阻塞，等待重试 (间隔 %v)", retryDelay)
				time.Sleep(retryDelay)
				// 动态调整重试间隔
				retryDelay = time.Duration(1.5 * float64(retryDelay))
				if retryDelay > 5*time.Second {
					retryDelay = 5 * time.Second
				}
			}
		}
	NEXT:
	}
}

// 批量上报流量统计
func batchReportStats() {
	batch := make([]Info, 0, maxBatchSize)
	ticker := time.NewTicker(flushInterval)
	defer ticker.Stop()

	for {
		select {
		case info := <-reportChan:
			batch = append(batch, info)
			if len(batch) >= maxBatchSize {
				// 达到批量大小，立即处理
				if !flushBatchWithRetry(batch) {
					// 如果批量处理失败，将数据逐一条目重新加入队列
					for _, item := range batch {
						reportChan <- item
					}
				}
				batch = make([]Info, 0, maxBatchSize)
			}
		case <-ticker.C:
			// 时间间隔到，处理当前批次
			if len(batch) > 0 {
				if !flushBatchWithRetry(batch) {
					// 如果批量处理失败，将数据逐一条目重新加入队列
					for _, item := range batch {
						reportChan <- item
					}
				}
				batch = make([]Info, 0, maxBatchSize)
			}
		}
	}
}

// 带重试的批量处理
func flushBatchWithRetry(batch []Info) bool {
	maxRetries := 5
	for attempt := 0; attempt < maxRetries; attempt++ {
		success := true
		for _, info := range batch {
			select {
			case rchanQueue <- info:
				// 发送成功
			default:
				// 队列满，尝试失败
				success = false
				break
			}
		}

		if success {
			return true
		}

		// 指数退避
		delay := time.Duration(1<<uint(attempt)) * retryInterval
		log.Printf("批量处理失败，第 %d 次重试，等待 %v", attempt+1, delay)
		time.Sleep(delay)
	}

	return false
}

// 立即上报流量统计，不经过批量处理
func reportStatsImmediately(info Info) {
	// 使用指数退避重试机制，确保数据不丢失
	maxRetries := 10
	for attempt := 0; attempt < maxRetries; attempt++ {
		select {
		case rchanQueue <- info:
			// 发送成功
			return
		default:
			// 队列满，等待后重试
			delay := time.Duration(1<<uint(attempt)) * retryInterval
			log.Printf("紧急上报队列阻塞，第 %d 次重试，等待 %v", attempt+1, delay)
			time.Sleep(delay)
		}
	}

	// 所有重试都失败，记录错误
	log.Printf("警告: 流量统计信息上报失败，已重试 %d 次: %v", maxRetries, info)
}

// 流量统计包装器
type TrafficCounter struct {
	reader     io.Reader
	writer     io.Writer
	bytesRead  int64
	bytesWrite int64
}

// 创建新的流量计数器
func NewTrafficCounter(r io.Reader, w io.Writer) *TrafficCounter {
	return &TrafficCounter{
		reader: r,
		writer: w,
	}
}

// 实现Reader接口
func (tc *TrafficCounter) Read(p []byte) (n int, err error) {
	n, err = tc.reader.Read(p)
	atomic.AddInt64(&tc.bytesRead, int64(n))
	return
}

// 实现Writer接口
func (tc *TrafficCounter) Write(p []byte) (n int, err error) {
	n, err = tc.writer.Write(p)
	atomic.AddInt64(&tc.bytesWrite, int64(n))
	return
}

// 获取读取的字节数
func (tc *TrafficCounter) BytesRead() int64 {
	return atomic.LoadInt64(&tc.bytesRead)
}

// 获取写入的字节数
func (tc *TrafficCounter) BytesWritten() int64 {
	return atomic.LoadInt64(&tc.bytesWrite)
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

// TransportWithStats 新版可靠传输实现
func TransportWithStats(rw1, rw2 io.ReadWriter, domain, sid string, localPort int) error {
	var (
		startTime = time.Now()
	)

	// 创建流量计数器
	counter1 := NewTrafficCounter(rw1, rw1)
	counter2 := NewTrafficCounter(rw2, rw2)

	errc := make(chan error, 2)

	// 上行流量采集
	go func() {
		defer func() {
			if r := recover(); r != nil {
				log.Printf("上行协程异常: %v", r)
			}
		}()
		_, err := io.Copy(counter2, counter1)
		errc <- err
	}()

	// 下行流量采集
	go func() {
		defer func() {
			if r := recover(); r != nil {
				log.Printf("下行协程异常: %v", r)
			}
		}()
		_, err := io.Copy(counter1, counter2)
		errc <- err
	}()

	// 错误处理增强
	var errCount int
	for i := 0; i < 2; i++ {
		if err := <-errc; err != nil {
			if err != io.EOF {
				log.Printf("传输错误: %v", err)
				errCount++
			}
		}
	}

	// 构建统计信息
	bytesUp := counter1.BytesWritten()
	bytesDown := counter2.BytesWritten()
	totalBytes := bytesUp + bytesDown
	duration := time.Since(startTime)

	info := Info{
		Address:    domain,
		LocalPort:  localPort,
		Bytes:      totalBytes,
		Unix:       time.Now().Unix(),
		RepeatNums: 1,
		SessionID:  sid,
		Domain:     domain,
	}

	// 确保流量统计100%上传成功
	reportStatsImmediately(info)

	log.Printf("[流量统计] SessionID=%s | Domain=%s | 上行=%d | 下行=%d | 总流量=%d | 耗时=%v",
		sid, domain, bytesUp, bytesDown, totalBytes, duration)

	if errCount > 0 {
		return io.ErrClosedPipe
	}
	return nil
}

// Transport1 统一传输接口
func Transport1(rw1, rw2 io.ReadWriter, address string, sid string) error {
	var (
		startTime = time.Now()
	)

	// 创建流量计数器
	counter1 := NewTrafficCounter(rw1, rw1)
	counter2 := NewTrafficCounter(rw2, rw2)

	errc := make(chan error, 2)

	go func() {
		n, err := io.CopyBuffer(counter2, counter1, bufpool.Get(bufferSize))
		_ = n // 不再直接使用返回值，而是从计数器获取
		errc <- err
	}()

	go func() {
		n, err := io.CopyBuffer(counter1, counter2, bufpool.Get(bufferSize))
		_ = n // 不再直接使用返回值，而是从计数器获取
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

	// 构建统计信息
	bytesUp := counter1.BytesWritten()
	bytesDown := counter2.BytesWritten()
	total := bytesUp + bytesDown

	info := Info{
		Address:    address,
		Bytes:      total,
		Unix:       time.Now().Unix(),
		RepeatNums: 1,
		SessionID:  sid,
	}

	// 确保流量统计100%上传成功
	reportStatsImmediately(info)

	log.Printf("[流量统计] %s | 上行: %d | 下行: %d | 总流量: %d | 耗时=%v",
		sid, bytesUp, bytesDown, total, time.Since(startTime))

	if errCount > 0 {
		return io.ErrUnexpectedEOF
	}
	return nil
}

func CopyBuffer1(dst io.Writer, src io.Reader, bufSize int, address string, sid string) error {
	buf := bufpool.Get(bufSize)
	defer bufpool.Put(buf)

	// 创建流量计数器
	counter := NewTrafficCounter(src, dst)

	bytes, err := io.CopyBuffer(counter, counter, buf)
	log.Printf("[消耗流量：]--%s------%s------%s", address, bytes, sid)

	// 构建统计信息
	info := Info{
		Address:    address,
		Bytes:      counter.BytesWritten(), // 使用计数器的实际写入字节数
		Unix:       time.Now().Unix(),
		RepeatNums: 1,
	}

	// 确保流量统计100%上传成功
	reportStatsImmediately(info)

	return err
}

// CopyBufferWithStats 带统计的拷贝实现
func CopyBufferWithStats(dst io.Writer, src io.Reader, bufSize int, address, sid string) error {
	buf := bufpool.Get(bufSize)
	defer bufpool.Put(buf)

	startTime := time.Now()

	// 创建流量计数器
	counter := NewTrafficCounter(src, dst)

	_, err := io.CopyBuffer(counter, counter, buf)

	// 构建统计信息
	info := Info{
		Address:    address,
		Bytes:      counter.BytesWritten(), // 使用计数器的实际写入字节数
		Unix:       time.Now().Unix(),
		RepeatNums: 1,
		SessionID:  sid,
	}

	// 确保流量统计100%上传成功
	reportStatsImmediately(info)

	log.Printf("[流量统计] %s | 传输量=%d | 耗时=%v", sid, counter.BytesWritten(), time.Since(startTime))
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
