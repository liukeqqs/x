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
	bufferSize = 64 * 1024
)

var (
	TrafficChan = make(chan *TrafficInfo, 1024)
	RChan       = make(chan Info, 5120)
	rchanQueue  = make(chan Info, 1e5)  // 新增缓冲队列
	once        sync.Once
)

// 初始化后台队列处理
func init() {
	once.Do(func() {
		go processRChanQueue()
	})
}

// TrafficInfo 流量统计结构
type TrafficInfo struct {
	Domain    string `json:"domain"`
	LocalPort int    `json:"localport"`
	SessionID string `json:"sid"`
	BytesUp   int64  `json:"up"`
	BytesDown int64  `json:"down"`
	StartTime int64  `json:"start"`
	EndTime   int64  `json:"end"`
}

// Info 旧流量统计结构
type Info struct {
	Address    string `json:"address"`
	LocalPort  int    `json:"localport"`
	Bytes      int64  `json:"bytes"`
	Unix       int64  `json:"unix"`
	RepeatNums int64  `json:"repeatnums"`
}
// 后台处理队列保证数据不丢失
func processRChanQueue() {
	for info := range rchanQueue {
		for {
			select {
			case RChan <- info:
				goto NEXT // 发送成功跳出重试循环
			default:
				time.Sleep(100 * time.Millisecond) // 通道满时等待
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
// TransportWithStats 带准确流量统计的传输
func TransportWithStats(rw1, rw2 io.ReadWriter, domain, sid string, localPort int) error {
	info := &TrafficInfo{
		Domain:    domain,
		LocalPort: localPort,
		SessionID: sid,
		StartTime: time.Now().Unix(),
	}
	defer func() {
		info.EndTime = time.Now().Unix()
		totalBytes := info.BytesUp + info.BytesDown

		// 非阻塞发送到缓冲队列
		select {
		case TrafficChan <- info:
		default:
			log.Printf("警告: TrafficChan队列已满，数据可能丢失")
		}

		// 通过队列发送到RChan
		rchanQueue <- Info{
			Address:    info.Domain,
			LocalPort:  info.LocalPort,
			Bytes:      totalBytes,
			Unix:       time.Now().Unix(),
			RepeatNums: 1,
		}

		log.Printf("[流量统计] SessionID=%s | Domain=%s | LocalPort=%d | 上行=%d | 下行=%d | 总流量=%d",
			info.SessionID, info.Domain, info.LocalPort, info.BytesUp, info.BytesDown, totalBytes)
	}()

	errc := make(chan error, 2)

	// 上行流量统计
	go func() {
		n, err := io.Copy(rw2, rw1)
		info.BytesUp = n
		errc <- err
	}()

	// 下行流量统计
	go func() {
		n, err := io.Copy(rw1, rw2)
		info.BytesDown = n
		errc <- err
	}()

	// 等待两个方向都完成
	err1 := <-errc
	err2 := <-errc

	// 排除EOF错误
	if err1 != nil && err1 != io.EOF {
		return err1
	}
	if err2 != nil && err2 != io.EOF {
		return err2
	}
	return nil
}

func Transport1(rw1, rw2 io.ReadWriter, address string, sid string) error {
	var (
		bytesUp   int64
		bytesDown int64
	)

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

	err1 := <-errc
	err2 := <-errc

	// 通过队列发送到RChan
	total := bytesUp + bytesDown
	rchanQueue <- Info{
		Address:    address,
		Bytes:      total,
		Unix:       time.Now().Unix(),
		RepeatNums: 1,
	}

	if err1 != nil && err1 != io.EOF {
		return err1
	}
	if err2 != nil && err2 != io.EOF {
		return err2
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