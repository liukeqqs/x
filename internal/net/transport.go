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
	maxQueueSize      = 300000
	retryInterval     = 50 * time.Millisecond
	flushInterval     = 200 * time.Millisecond
	maxBatchSize      = 500
	reportChannelSize = 50000
	queueWorkers      = 50
)

var (
	// 新的流量统计和上传模块实例
	trafficStats *TrafficStats
	// 初始化once
	trafficStatsOnce sync.Once
)

// InitTrafficStats 初始化流量统计和上传模块
func InitTrafficStats(redisAddr, redisPassword string, redisDB int) {
	trafficStatsOnce.Do(func() {
		trafficStats = NewTrafficStats(redisAddr, redisPassword, redisDB)
	})
}

// GetTrafficStats 获取流量统计和上传模块实例
func GetTrafficStats() *TrafficStats {
	return trafficStats
}

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
	// queueOnce.Do(func() {
	// 	for i := 0; i < queueWorkers; i++ {
	// 		go processRChanQueue()
	// 	}
	// 	go batchReportStats()
	// 	go monitorQueueStats()
	// })
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

func TransportWithStats(rw1, rw2 io.ReadWriter, domain, sid string, localPort int) error {
	startTime := time.Now()

	counter1 := NewTrafficCounter(rw1, rw1)
	counter2 := NewTrafficCounter(rw2, rw2)

	errChan := make(chan error, 2)
	done := make(chan struct{})

	var totalBytes int64

	go func() {
		defer func() {
			if r := recover(); r != nil {
				log.Printf("上行PANIC: %v SID:%s", r, sid)
			}
		}()

		_, err := io.Copy(counter2, counter1)
		errChan <- err
	}()

	go func() {
		defer func() {
			if r := recover(); r != nil {
				log.Printf("下行PANIC: %v SID:%s", r, sid)
			}
		}()

		_, err := io.Copy(counter1, counter2)
		errChan <- err
	}()

	go func() {
		defer close(done)

		var errCount int
		timeout := time.After(30 * time.Second)

		for i := 0; i < 2; i++ {
			select {
			case err := <-errChan:
				if err != nil && err != io.EOF {
					log.Printf("传输错误: %v SID:%s", err, sid)
					errCount++
				}
			case <-timeout:
				log.Printf("流量上报超时 SID:%s", sid)
			}
		}

		bytesUp := counter1.BytesRead()
		bytesDown := counter2.BytesRead()
		totalBytes = bytesUp + bytesDown
		// 移除对 totalCapturedBytes 的原子操作
		// atomic.AddInt64(&totalCapturedBytes, totalBytes)
		duration := time.Since(startTime)

		log.Printf("[流量] SID:%s 上行:%d 下行:%d 总计:%d 耗时:%v",
			sid, bytesUp, bytesDown, totalBytes, duration)

		info := Info{
			Address:    domain,
			LocalPort:  localPort,
			Bytes:      totalBytes,
			Unix:       time.Now().Unix(),
			RepeatNums: 1,
			SessionID:  sid,
			Domain:     domain,
		}

		// 使用新的流量统计和上传模块
		if trafficStats != nil {
			trafficStats.ReportStats(info)
		} else {
			log.Printf("警告: 流量统计模块未初始化，数据未上报")
		}
	}()

	select {
	case <-done:
	case <-time.After(5 * time.Second):
		log.Printf("警告: 流量上报未完成 SID:%s", sid)
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

	// 使用新的流量统计和上传模块
	if trafficStats != nil {
		trafficStats.ReportStats(info)
	} else {
		log.Printf("警告: 流量统计模块未初始化，数据未上报")
	}

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

	counter := NewTrafficCounter(src, dst)

	bytes, err := io.CopyBuffer(counter, counter, buf)
	log.Printf("[消耗流量：]--%s------%d------%s", address, bytes, sid)

	info := Info{
		Address:    address,
		Bytes:      counter.BytesWritten(),
		Unix:       time.Now().Unix(),
		RepeatNums: 1,
	}

	// 使用新的流量统计和上传模块
	if trafficStats != nil {
		trafficStats.ReportStats(info)
	} else {
		log.Printf("警告: 流量统计模块未初始化，数据未上报")
	}

	return err
}

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

	// 使用新的流量统计和上传模块
	if trafficStats != nil {
		trafficStats.ReportStats(info)
	} else {
		log.Printf("警告: 流量统计模块未初始化，数据未上报")
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