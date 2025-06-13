package net

import (
	"io"
	"log"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/liukeqqs/core/common/bufpool"
)

const bufferSize = 64 * 1024

var (
	TrafficChan = make(chan *TrafficInfo, 1024)
	RChan       = make(chan Info, 5120)
)

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

// trafficConn 带流量统计的连接包装器
type trafficConn struct {
	net.Conn
	upCounter   *int64 // 上行计数器
	downCounter *int64 // 下行计数器
}

func (c *trafficConn) Read(b []byte) (n int, err error) {
	n, err = c.Conn.Read(b)
	atomic.AddInt64(c.downCounter, int64(n))
	return
}

func (c *trafficConn) Write(b []byte) (n int, err error) {
	n, err = c.Conn.Write(b)
	atomic.AddInt64(c.upCounter, int64(n))
	return
}

// TransportWithStats 精确流量统计的传输函数
func TransportWithStats(conn1, conn2 net.Conn, domain, sid string, localPort int) error {
	info := &TrafficInfo{
		Domain:    domain,
		LocalPort: localPort,
		SessionID: sid,
		StartTime: time.Now().Unix(),
	}

	// 使用原子计数器确保并发安全
	var upCounter, downCounter int64

	// 包装连接以进行流量统计
	tc1 := &trafficConn{
		Conn:        conn1,
		upCounter:   &upCounter,
		downCounter: &downCounter,
	}

	tc2 := &trafficConn{
		Conn:        conn2,
		upCounter:   &upCounter,
		downCounter: &downCounter,
	}

	// 使用同步等待确保双向传输完成
	var wg sync.WaitGroup
	wg.Add(2)

	// 启动双向传输
	go func() {
		defer wg.Done()
		copyStream(tc1, tc2)
	}()

	go func() {
		defer wg.Done()
		copyStream(tc2, tc1)
	}()

	wg.Wait()
	info.EndTime = time.Now().Unix()
	info.BytesUp = atomic.LoadInt64(&upCounter)
	info.BytesDown = atomic.LoadInt64(&downCounter)
	totalBytes := info.BytesUp + info.BytesDown

	// 发送统计信息
	TrafficChan <- info
	RChan <- Info{
		Address:    info.Domain,
		LocalPort:  info.LocalPort,
		Bytes:      totalBytes,
		Unix:       time.Now().Unix(),
		RepeatNums: 1,
	}

	log.Printf(
		"[精确流量] SessionID=%s | Domain=%s | 上行=%d | 下行=%d | 总流量=%d",
		info.SessionID,
		info.Domain,
		info.BytesUp,
		info.BytesDown,
		totalBytes,
	)

	return nil
}

// copyStream 精确的流复制函数
func copyStream(dst, src net.Conn) {
	buf := bufpool.Get(bufferSize)
	defer bufpool.Put(buf)

	for {
		// 设置读取超时防止永久阻塞
		src.SetReadDeadline(time.Now().Add(30 * time.Second))
		nr, err := src.Read(buf)
		if nr > 0 {
			// 设置写入超时
			dst.SetWriteDeadline(time.Now().Add(30 * time.Second))
			nw, err := dst.Write(buf[:nr])
			if err != nil {
				break
			}
			if nr != nw {
				break
			}
		}
		if err != nil {
			break
		}
	}

	// 尝试半关闭连接
	if tcpConn, ok := dst.(*net.TCPConn); ok {
		tcpConn.CloseWrite()
	}
	if tcpConn, ok := src.(*net.TCPConn); ok {
		tcpConn.CloseRead()
	}
}