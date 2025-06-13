package net

import (
	"bufio"
	"github.com/liukeqqs/core/common/bufpool"
	"io"
	"log"
	"net"
	"sync"
	"time"
)

const (
	bufferSize    = 64 * 1024
	maxQueueSize  = 100000      // 队列最大容量
	retryInterval = 100 * time.Millisecond // 重试间隔
)

var (
	RChan       = make(chan Info, 5120)
	rchanQueue  = make(chan Info, maxQueueSize)
	queueOnce   sync.Once
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
		bytesUp   int64
		bytesDown int64
		startTime = time.Now()
	)

	defer func() {
		totalBytes := bytesUp + bytesDown
		duration := time.Since(startTime)

		// 构建增强版统计信息
		rchanQueue <- Info{
			Address:    domain,
			LocalPort:  localPort,
			Bytes:      totalBytes,
			Unix:       time.Now().Unix(),
			RepeatNums: 1,
			SessionID:  sid,
			Domain:     domain,
		}

		log.Printf("[流量统计] SessionID=%s | Domain=%s | 上行=%d | 下行=%d | 总流量=%d | 耗时=%v",
			sid, domain, bytesUp, bytesDown, totalBytes, duration)
	}()

	errc := make(chan error, 2)

	// 上行流量采集
	go func() {
		defer func() {
			if r := recover(); r != nil {
				log.Printf("上行协程异常: %v", r)
			}
		}()
		n, err := io.Copy(rw2, rw1)
		bytesUp = n
		errc <- err
	}()

	// 下行流量采集
	go func() {
		defer func() {
			if r := recover(); r != nil {
				log.Printf("下行协程异常: %v", r)
			}
		}()
		n, err := io.Copy(rw1, rw2)
		bytesDown = n
		errc <- err
	}()

	// 错误处理增强
	var errCount int
	for i := 0; i < 2; i++ {
		if err := <-errc; err != nil {
			if err != io.EOF {
				log.Printf("传输错误: %v----Sid：%v，| 上行=%d | 下行=%d | 总流量=%d ", err,sid,bytesUp, bytesDown, totalBytes)
				errCount++
			}
		}
	}

	if errCount > 0 {
		return io.ErrClosedPipe
	}
	return nil
}

// Transport1 统一传输接口
func Transport1(rw1, rw2 io.ReadWriter, address string, sid string) error {
	var (
		bytesUp   int64
		bytesDown int64
		startTime = time.Now()
	)

	defer func() {
		total := bytesUp + bytesDown
		rchanQueue <- Info{
			Address:    address,
			Bytes:      total,
			Unix:       time.Now().Unix(),
			RepeatNums: 1,
			SessionID:  sid,
		}
		log.Printf("[流量统计] %s | 上行: %d | 下行: %d | 总流量: %d | 耗时=%v",
			sid, bytesUp, bytesDown, total, time.Since(startTime))
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


func CopyBuffer1(dst io.Writer, src io.Reader, bufSize int, address string, sid string) error {
	buf := bufpool.Get(bufSize)
	defer bufpool.Put(buf)
	bytes, err := io.CopyBuffer(dst, src, buf)
	log.Printf("[消耗流量：]--%s------%s------%s", address, bytes, sid)
	RChan <- Info{
		Address:    address,
		Bytes:      bytes,
		Unix:       time.Now().Unix(),
		RepeatNums: 1,
	}
	return err
}

// CopyBufferWithStats 带统计的拷贝实现
func CopyBufferWithStats(dst io.Writer, src io.Reader, bufSize int, address, sid string) error {
	buf := bufpool.Get(bufSize)
	defer bufpool.Put(buf)

	startTime := time.Now()
	bytes, err := io.CopyBuffer(dst, src, buf)

	rchanQueue <- Info{
		Address:    address,
		Bytes:      bytes,
		Unix:       time.Now().Unix(),
		RepeatNums: 1,
		SessionID:  sid,
	}

	log.Printf("[流量统计] %s | 传输量=%d | 耗时=%v", sid, bytes, time.Since(startTime))
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