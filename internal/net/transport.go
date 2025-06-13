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

		// 发送到新通道
		TrafficChan <- info

		// 发送到旧通道 RChan（假设已存在）
		RChan <- Info{
			Address:    info.Domain,
			LocalPort:      info.LocalPort,
			Bytes:      totalBytes,
			Unix:       time.Now().Unix(),
			RepeatNums: 1,
		}

		log.Printf(
			"[流量统计] SessionID=%s | Domain=%s | LocalPort=%d | 上行=%d | 下行=%d | 总流量=%d",
			info.SessionID,
			info.Domain,
			info.LocalPort,
			info.BytesUp,
			info.BytesDown,
			totalBytes,
		)
	}()

	errc := make(chan error, 2)
	go func() {
		n, err := io.Copy(rw2, rw1)
		info.BytesUp = n
		errc <- err
	}()
	go func() {
		n, err := io.Copy(rw1, rw2)
		info.BytesDown = n
		errc <- err
	}()

	if err := <-errc; err != nil && err != io.EOF {
		return err
	}
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