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
	queueWorkers      = 20
)

var (
	RChan      = make(chan Info, 5120)
	rchanQueue = make(chan Info, maxQueueSize)
	queueOnce  sync.Once
	reportChan = make(chan Info, reportChannelSize)

	// 队列监控原子变量
	pendingQueueCount  int64
	pendingQueueBytes  int64
	pendingReportCount int64

	// 新增流量统计原子变量
	totalReportedBytes int64
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
		// 启动队列处理器
		for i := 0; i < queueWorkers; i++ {
			go processRChanQueue()
		}
		// 启动批量上报处理器
		go batchReportStats()
		// 启动队列监控
		go monitorQueueStats()
	})
}

// 打印队列状态
func logQueueStats() {
	qCount := atomic.LoadInt64(&pendingQueueCount)
	qBytes := atomic.LoadInt64(&pendingQueueBytes)
	rCount := atomic.LoadInt64(&pendingReportCount)
	total := atomic.LoadInt64(&totalReportedBytes)

	log.Printf("[流量队列监控] 待上传请求:%d 待上传流量:%.2f MB 批量队列积压:%d 已上报总量:%.2f MB",
		qCount,
		float64(qBytes)/(1024*1024),
		rCount,
		float64(total)/(1024*1024),
	)
}

// 队列监控协程
func monitorQueueStats() {
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()

	for range ticker.C {
		logQueueStats()
	}
}

// 修复1: 正确处理队列处理逻辑
func processRChanQueue() {
	for info := range rchanQueue {
		// 减少积压计数
		atomic.AddInt64(&pendingQueueCount, -1)
		atomic.AddInt64(&pendingQueueBytes, -info.Bytes)

		retryCount := 0
		maxRetries := 25

		for {
			select {
			case RChan <- info:
				// 修复2: 增加已上报总量统计
				atomic.AddInt64(&totalReportedBytes, info.Bytes)
				goto NEXT
			default:
				retryCount++
				if retryCount%10 == 0 {
					log.Printf("队列阻塞告警: 已重试%d次 积压流量:%.2f MB",
						retryCount,
						float64(atomic.LoadInt64(&pendingQueueBytes))/(1024*1024),
					)
				}

				// 指数退避
				delay := time.Duration(retryCount) * retryInterval
				if delay > 3*time.Second {
					delay = 3 * time.Second
				}
				time.Sleep(delay)

				if retryCount > maxRetries {
					log.Printf("致命错误: 无法上报流量数据! 地址:%s 流量:%.2f MB",
						info.Address,
						float64(info.Bytes)/(1024*1024),
					)
					goto NEXT
				}
			}
		}
	NEXT:
	}
}

// 批量上报处理器
func batchReportStats() {
	batch := make([]Info, 0, maxBatchSize)
	ticker := time.NewTicker(flushInterval)
	defer ticker.Stop()

	for {
		select {
		case info := <-reportChan:
			// 减少批量队列计数
			atomic.AddInt64(&pendingReportCount, -1)

			batch = append(batch, info)
			if len(batch) >= maxBatchSize {
				flushBatch(batch)
				batch = make([]Info, 0, maxBatchSize)
			}
		case <-ticker.C:
			if len(batch) > 0 {
				flushBatch(batch)
				batch = make([]Info, 0, maxBatchSize)
			}
		}
	}
}

// 批量刷新
func flushBatch(batch []Info) {
	for _, info := range batch {
		select {
		case rchanQueue <- info:
			// 成功加入队列时增加积压计数
			atomic.AddInt64(&pendingQueueCount, 1)
			atomic.AddInt64(&pendingQueueBytes, info.Bytes)
		default:
			log.Printf("严重警告: 流量数据丢失! 地址:%s 流量:%.2f MB",
				info.Address,
				float64(info.Bytes)/(1024*1024),
			)
		}
	}
}

// 修复3: 重写立即上报函数
func reportStatsImmediately(info Info) {
	// 加入队列前增加积压计数
	atomic.AddInt64(&pendingQueueCount, 1)
	atomic.AddInt64(&pendingQueueBytes, info.Bytes)

	select {
	case rchanQueue <- info:
		return
	default:
		// 队列满时直接尝试发送到RChan
		retryCount := 0
		maxRetries := 10

		for {
			select {
			case RChan <- info:
				atomic.AddInt64(&totalReportedBytes, info.Bytes)
				return
			default:
				retryCount++
				if retryCount > maxRetries {
					// 减少积压计数（因为最终丢弃）
					atomic.AddInt64(&pendingQueueCount, -1)
					atomic.AddInt64(&pendingQueueBytes, -info.Bytes)

					log.Printf("致命错误: 流量数据丢弃! 重试%d次 地址:%s 流量:%.2f MB 当前积压:%d条",
						retryCount,
						info.Address,
						float64(info.Bytes)/(1024*1024),
						atomic.LoadInt64(&pendingQueueCount),
					)
					return
				}

				delay := time.Duration(retryCount) * retryInterval
				if delay > 1*time.Second {
					delay = 1 * time.Second
				}

				time.Sleep(delay)
			}
		}
	}
}

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

// 修复4: 重写流量统计传输函数
func TransportWithStats(rw1, rw2 io.ReadWriter, domain, sid string, localPort int) error {
	startTime := time.Now()

	// 创建流量计数器 - 修复方向问题
	counter1 := NewTrafficCounter(rw1, rw1) // 上行：从客户端读取，向服务器写入
	counter2 := NewTrafficCounter(rw2, rw2) // 下行：从服务器读取，向客户端写入

	errc := make(chan error, 2)

	// 上行：客户端 -> 服务器
	go func() {
		defer func() {
			if r := recover(); r != nil {
				log.Printf("上行协程异常: %v", r)
			}
		}()

		// 修复5: 使用正确的复制方向
		_, err := io.Copy(counter2, counter1)
		errc <- err
	}()

	// 下行：服务器 -> 客户端
	go func() {
		defer func() {
			if r := recover(); r != nil {
				log.Printf("下行协程异常: %v", r)
			}
		}()

		// 修复6: 使用正确的复制方向
		_, err := io.Copy(counter1, counter2)
		errc <- err
	}()

	var errCount int
	for i := 0; i < 2; i++ {
		if err := <-errc; err != nil {
			// 修复7: 不打印所有错误，只记录非EOF错误
			if err != io.EOF {
				log.Printf("传输错误: %v (SID: %s)", err, sid)
				errCount++
			}
		}
	}

	// 修复8: 正确获取上下行流量
	bytesUp := counter1.BytesRead()      // 从客户端读取的字节数（上行）
	bytesDown := counter2.BytesRead()    // 从服务器读取的字节数（下行）
	totalBytes := bytesUp + bytesDown
	duration := time.Since(startTime)

	// 修复9: 添加详细日志帮助诊断
	log.Printf("[流量详情] SID:%s 上行:%d 下行:%d 总计:%d 耗时:%v",
		sid, bytesUp, bytesDown, totalBytes, duration)

	// 修复10: 确保即使有错误也上报流量
	info := Info{
		Address:    domain,
		LocalPort:  localPort,
		Bytes:      totalBytes,
		Unix:       time.Now().Unix(),
		RepeatNums: 1,
		SessionID:  sid,
		Domain:     domain,
	}

	// 使用批量上报通道避免阻塞
	select {
	case reportChan <- info:
		// 增加批量队列计数
		atomic.AddInt64(&pendingReportCount, 1)
	default:
		// 如果批量队列满，直接上报
		reportStatsImmediately(info)
	}

	if errCount > 0 {
		return io.ErrClosedPipe
	}
	return nil
}

func Transport1(rw1, rw2 io.ReadWriter, address string, sid string) error {
	startTime := time.Now()
	counter1 := NewTrafficCounter(rw1, rw1)
	counter2 := NewTrafficCounter(rw2, rw2)

	errc := make(chan error, 2)

	go func() {
		_, err := io.CopyBuffer(counter2, counter1, bufpool.Get(bufferSize))
		errc <- err
	}()

	go func() {
		_, err := io.CopyBuffer(counter1, counter2, bufpool.Get(bufferSize))
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

	// 使用批量上报通道避免阻塞
	select {
	case reportChan <- info:
		atomic.AddInt64(&pendingReportCount, 1)
	default:
		reportStatsImmediately(info)
	}

	log.Printf("[流量统计] %s | 上行: %d | 下行: %d | 总流量: %d | 耗时=%v",
		sid, bytesUp, bytesDown, total, time.Since(startTime))

	if errCount > 0 {
		return io.ErrUnexpectedEOF
	}
	return nil
}

// 修复11: 简化拷贝函数
func CopyBufferWithStats(dst io.Writer, src io.Reader, bufSize int, address, sid string) error {
	buf := bufpool.Get(bufSize)
	defer bufpool.Put(buf)

	startTime := time.Now()
	counter := NewTrafficCounter(src, dst)

	_, err := io.CopyBuffer(counter, counter, buf)

	info := Info{
		Address:    address,
		Bytes:      counter.BytesWritten(),
		Unix:       time.Now().Unix(),
		RepeatNums: 1,
		SessionID:  sid,
	}

	// 使用批量上报通道避免阻塞
	select {
	case reportChan <- info:
		atomic.AddInt64(&pendingReportCount, 1)
	default:
		reportStatsImmediately(info)
	}

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