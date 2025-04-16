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
)

type TrafficInfo struct {
	Domain    string `json:"domain"`
	LocalPort int    `json:"localport"`
	SessionID string `json:"sid"`
	BytesUp   int64  `json:"up"`
	BytesDown int64  `json:"down"`
	StartTime int64  `json:"start"`
	EndTime   int64  `json:"end"`
}

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

func TransportWithStats(rw1, rw2 io.ReadWriter, domain, sid string, localPort int) error {
	info := &TrafficInfo{
		Domain:    domain,
		LocalPort: localPort,
		SessionID: sid,
		StartTime: time.Now().Unix(),
	}

	var wg sync.WaitGroup
	wg.Add(2)
	errc := make(chan error, 2)

	go func() {
		defer wg.Done()
		n, err := io.Copy(rw2, rw1)
		info.BytesUp = n
		errc <- err
	}()

	go func() {
		defer wg.Done()
		n, err := io.Copy(rw1, rw2)
		info.BytesDown = n
		errc <- err
	}()

	wg.Wait()
	close(errc)

	for err := range errc {
		if err != nil && err != io.EOF {
			return err
		}
	}

	info.EndTime = time.Now().Unix()
	totalBytes := info.BytesUp + info.BytesDown

	select {
	case TrafficChan <- info:
	default:
		log.Println("TrafficChan 缓冲区已满，丢弃数据")
	}

	select {
	case RChan <- Info{
		Address:    info.Domain,
		LocalPort:  info.LocalPort,
		Bytes:      totalBytes,
		Unix:       time.Now().Unix(),
		RepeatNums: 1,
	}:
	default:
		log.Println("RChan 缓冲区已满，丢弃数据")
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

	return nil
}

func Transport1(rw1, rw2 io.ReadWriter, address string, sid string) error {
	var (
		bytesUp   int64
		bytesDown int64
		wg        sync.WaitGroup
	)

	wg.Add(2)
	errc := make(chan error, 2)

	go func() {
		defer wg.Done()
		n, err := io.CopyBuffer(rw2, rw1, bufpool.Get(bufferSize))
		bytesUp = n
		errc <- err
	}()

	go func() {
		defer wg.Done()
		n, err := io.CopyBuffer(rw1, rw2, bufpool.Get(bufferSize))
		bytesDown = n
		errc <- err
	}()

	wg.Wait()
	close(errc)

	for err := range errc {
		if err != nil && err != io.EOF {
			return err
		}
	}

	totalBytes := bytesUp + bytesDown
	select {
	case RChan <- Info{
		Address:    address,
		Bytes:      totalBytes,
		Unix:       time.Now().Unix(),
		RepeatNums: 1,
	}:
	default:
		log.Println("RChan 缓冲区已满，丢弃数据")
	}

	log.Printf("[流量统计] %s | 上行: %d | 下行: %d | 总流量: %d", sid, bytesUp, bytesDown, totalBytes)
	return nil
}

func CopyBuffer1(dst io.Writer, src io.Reader, bufSize int, address string, sid string) error {
	buf := bufpool.Get(bufSize)
	defer bufpool.Put(buf)
	bytes, err := io.CopyBuffer(dst, src, buf)

	select {
	case RChan <- Info{
		Address:    address,
		Bytes:      bytes,
		Unix:       time.Now().Unix(),
		RepeatNums: 1,
	}:
	default:
		log.Println("RChan 缓冲区已满，丢弃数据")
	}
	log.Printf("[消耗流量：]--%s------%s------%s", address, bytes, sid)
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
