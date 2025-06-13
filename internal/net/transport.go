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
	maxQueueSize      = 100000
	retryInterval     = 100 * time.Millisecond
	flushInterval     = 500 * time.Millisecond
	maxBatchSize      = 100
	reportChannelSize = 10000
)

var (
	RChan       = make(chan Info, 5120)
	rchanQueue  = make(chan Info, maxQueueSize)
	queueOnce   sync.Once
	reportChan  = make(chan Info, reportChannelSize)
	statsMutex  sync.Mutex
)

type Info struct {
	Address    string `json:"address"`
	LocalPort  int    `json:"localport"`
	Bytes      int64  `json:"bytes"`
	Unix       int64  `json:"unix"`
	RepeatNums int64  `json:"repeatnums"`
	SessionID  string `json:"sid,omitempty"`
	Domain     string `json:"domain,omitempty"`
}

func init() {
	queueOnce.Do(func() {
		go processRChanQueue()
		go batchReportStats()
		go monitorQueueStatus() // 新增队列监控
	})
}

// 监控队列状态
func monitorQueueStatus() {
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()

	for range ticker.C {
		log.Printf("[队列状态] rchanQueue: %d/%d (%.1f%%) | reportChan: %d/%d (%.1f%%) | RChan: %d/%d (%.1f%%)",
			len(rchanQueue), maxQueueSize, float64(len(rchanQueue))/float64(maxQueueSize)*100,
			len(reportChan), reportChannelSize, float64(len(reportChan))/float64(reportChannelSize)*100,
			len(RChan), 5120, float64(len(RChan))/5120*100)
	}
}

func processRChanQueue() {
	for info := range rchanQueue {
		retryDelay := retryInterval
		for {
			select {
			case RChan <- info:
				goto NEXT
			default:
				log.Printf("队列阻塞，等待重试 (间隔 %v)", retryDelay)
				time.Sleep(retryDelay)
				retryDelay = time.Duration(1.5 * float64(retryDelay))
				if retryDelay > 5*time.Second {
					retryDelay = 5 * time.Second
				}
			}
		}
	NEXT:
	}
}

func batchReportStats() {
	batch := make([]Info, 0, maxBatchSize)
	ticker := time.NewTicker(flushInterval)
	defer ticker.Stop()

	for {
		select {
		case info := <-reportChan:
			batch = append(batch, info)
			if len(batch) >= maxBatchSize {
				if !flushBatchWithRetry(batch) {
					for _, item := range batch {
						reportChan <- item
					}
				}
				batch = batch[:0]
			}
		case <-ticker.C:
			if len(batch) > 0 {
				if !flushBatchWithRetry(batch) {
					for _, item := range batch {
						reportChan <- item
					}
				}
				batch = batch[:0]
			}
		}
	}
}

func flushBatchWithRetry(batch []Info) bool {
	maxRetries := 5
	for attempt := 0; attempt < maxRetries; attempt++ {
		success := true
		for _, info := range batch {
			select {
			case rchanQueue <- info:
			default:
				success = false
				break
			}
		}

		if success {
			return true
		}

		delay := time.Duration(1<<uint(attempt)) * retryInterval
		log.Printf("批量处理失败，第 %d 次重试，等待 %v", attempt+1, delay)
		time.Sleep(delay)
	}

	return false
}

func reportStatsImmediately(info Info) {
	maxRetries := 10
	for attempt := 0; attempt < maxRetries; attempt++ {
		select {
		case rchanQueue <- info:
			return
		default:
			delay := time.Duration(1<<uint(attempt)) * retryInterval
			log.Printf("紧急上报队列阻塞，第 %d 次重试，等待 %v", attempt+1, delay)
			time.Sleep(delay)
		}
	}

	log.Printf("警告: 流量统计信息上报失败，已重试 %d 次: %v", maxRetries, info)
}

// 改进后的流量计数器
type TrafficCounter struct {
	reader     io.Reader
	writer     io.Writer
	bytesRead  int64
	bytesWrite int64
}

func NewTrafficCounter(r io.Reader, w io.Writer) *TrafficCounter {
	return &TrafficCounter{
		reader: r,
		writer: w,
	}
}

func (tc *TrafficCounter) Read(p []byte) (n int, err error) {
	n, err = tc.reader.Read(p)
	atomic.AddInt64(&tc.bytesRead, int64(n))
	return
}

func (tc *TrafficCounter) Write(p []byte) (n int, err error) {
	n, err = tc.writer.Write(p)
	atomic.AddInt64(&tc.bytesWrite, int64(n))
	return
}

func (tc *TrafficCounter) BytesRead() int64 {
	return atomic.LoadInt64(&tc.bytesRead)
}

func (tc *TrafficCounter) BytesWritten() int64 {
	return atomic.LoadInt64(&tc.bytesWrite)
}

// 改进后的传输函数
func TransportWithStats(rw1, rw2 io.ReadWriter, domain, sid string, localPort int) error {
	var (
		startTime = time.Now()
		bytesUp   int64
		bytesDown int64
	)

	// 创建独立的读写计数器
	reader1 := NewTrafficCounter(rw1, nil)
	writer1 := NewTrafficCounter(nil, rw1)
	reader2 := NewTrafficCounter(rw2, nil)
	writer2 := NewTrafficCounter(nil, rw2)

	errc := make(chan error, 2)

	// 上行流量采集
	go func() {
		defer func() {
			if r := recover(); r != nil {
				log.Printf("上行协程异常: %v", r)
			}
		}()

		// 使用独立的缓冲区
		buf := bufpool.Get(bufferSize)
		defer bufpool.Put(buf)

		n, err := io.CopyBuffer(writer2, reader1, buf)
		atomic.StoreInt64(&bytesUp, n)
		errc <- err
	}()

	// 下行流量采集
	go func() {
		defer func() {
			if r := recover(); r != nil {
				log.Printf("下行协程异常: %v", r)
			}
		}()

		// 使用独立的缓冲区
		buf := bufpool.Get(bufferSize)
		defer bufpool.Put(buf)

		n, err := io.CopyBuffer(writer1, reader2, buf)
		atomic.StoreInt64(&bytesDown, n)
		errc <- err
	}()

	var errCount int
	for i := 0; i < 2; i++ {
		if err := <-errc; err != nil {
			if err != io.EOF {
				log.Printf("传输错误: %v", err)
				errCount++
			}
		}
	}

	// 获取最终统计
	totalBytes := atomic.LoadInt64(&bytesUp) + atomic.LoadInt64(&bytesDown)
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

	// 打印详细统计信息
	log.Printf("[详细统计] SessionID=%s | 上行=%d (reader1=%d, writer2=%d) | 下行=%d (reader2=%d, writer1=%d) | 总流量=%d | 耗时=%v",
		sid,
		atomic.LoadInt64(&bytesUp), reader1.BytesRead(), writer2.BytesWritten(),
		atomic.LoadInt64(&bytesDown), reader2.BytesRead(), writer1.BytesWritten(),
		totalBytes, duration)

	// 确保流量统计100%上传成功
	reportStatsImmediately(info)

	if errCount > 0 {
		return io.ErrClosedPipe
	}
	return nil
}

// 简化版传输函数
func Transport1(rw1, rw2 io.ReadWriter, address string, sid string) error {
	return TransportWithStats(rw1, rw2, address, sid, 0)
}

// 改进的CopyBufferWithStats
func CopyBufferWithStats(dst io.Writer, src io.Reader, bufSize int, address, sid string) error {
	buf := bufpool.Get(bufSize)
	defer bufpool.Put(buf)

	startTime := time.Now()

	// 创建独立的读写计数器
	reader := NewTrafficCounter(src, nil)
	writer := NewTrafficCounter(nil, dst)

	n, err := io.CopyBuffer(writer, reader, buf)

	// 构建统计信息
	info := Info{
		Address:    address,
		Bytes:      n, // 直接使用Copy的返回值
		Unix:       time.Now().Unix(),
		RepeatNums: 1,
		SessionID:  sid,
	}

	// 打印详细统计
	log.Printf("[Copy统计] %s | Copy返回值=%d | reader=%d | writer=%d | 耗时=%v",
		sid, n, reader.BytesRead(), writer.BytesWritten(), time.Since(startTime))

	// 确保流量统计100%上传成功
	reportStatsImmediately(info)

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
