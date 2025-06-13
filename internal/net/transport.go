package net

import (
	"bufio"
	"github.com/liukeqqs/core/common/bufpool"
	"io"
	"log"
	"net"
	"time"
)

const (
	bufferSize = 64 * 1024
)

var (
	TrafficChan = make(chan *TrafficInfo, 1024) // 新通道
	RChan = make(chan Info, 5120)
)

// TrafficInfo 新流量统计结构
type TrafficInfo struct {
	Domain    string `json:"domain"`
	LocalPort int    `json:"localport"`
	SessionID string `json:"sid"`
	BytesUp   int64  `json:"up"`
	BytesDown int64  `json:"down"`
	StartTime int64  `json:"start"`
	EndTime   int64  `json:"end"`
}

// Info 旧流量统计结构（假设已存在）
type Info struct {
	Address    string `json:"address"`
	LocalPort  int    `json:"localport"`
	Bytes      int64  `json:"bytes"`
	Unix       int64  `json:"unix"`
	RepeatNums int64  `json:"repeatnums"`
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
// TransportWithStats 带流量统计的传输
// TransportWithStats 带流量统计的传输 (修复版)
func TransportWithStats(rw1, rw2 io.ReadWriter, domain, sid string, localPort int) error {
	info := &TrafficInfo{
		Domain:    domain,
		LocalPort: localPort,
		SessionID: sid,
		StartTime: time.Now().Unix(),
	}

	// 使用同步等待确保两个方向都完成
	var wg sync.WaitGroup
	wg.Add(2)

	// 上行流量统计 (rw1 -> rw2)
	go func() {
		defer wg.Done()
		info.BytesUp, _ = io.Copy(rw2, rw1)
		// 尝试关闭写方向
		if c, ok := rw2.(interface{ CloseWrite() error }); ok {
			c.CloseWrite()
		}
	}()

	// 下行流量统计 (rw2 -> rw1)
	go func() {
		defer wg.Done()
		info.BytesDown, _ = io.Copy(rw1, rw2)
		// 尝试关闭写方向
		if c, ok := rw1.(interface{ CloseWrite() error }); ok {
			c.CloseWrite()
		}
	}()

	// 等待双向传输完成
	wg.Wait()
	info.EndTime = time.Now().Unix()

	// 发送统计信息（确保只发送一次）
	totalBytes := info.BytesUp + info.BytesDown
	TrafficChan <- info
	RChan <- Info{
		Address:    info.Domain,
		LocalPort:  info.LocalPort,
		Bytes:      totalBytes,
		Unix:       time.Now().Unix(),
		RepeatNums: 1,
	}

	log.Printf(
		"[流量统计] SessionID=%s | Domain=%s | 上行=%d | 下行=%d | 总流量=%d",
		info.SessionID,
		info.Domain,
		info.BytesUp,
		info.BytesDown,
		totalBytes,
	)
	return nil
}
func Transport1(rw1, rw2 io.ReadWriter, address string, sid string) error {
    var (
        bytesUp   int64 // 上行流量（rw1 → rw2）
        bytesDown int64 // 下行流量（rw2 → rw1）
    )

    errc := make(chan error, 1)
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

    if err := <-errc; err != nil && err != io.EOF {
        return err
    }

    // 只发送一次统计信息
    RChan <- Info{
        Address:    address,
        Bytes:      bytesUp + bytesDown, // 总流量
        Unix:       time.Now().Unix(),
        RepeatNums: 1,
    }
    log.Printf("[流量统计] %s | 上行: %d | 下行: %d | 总流量: %d", sid, bytesUp, bytesDown, bytesUp+bytesDown)
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
