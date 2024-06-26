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
	RChan = make(chan Info, 5120)
)

type Info struct {
	Address string `json:"-"`
	Bytes   int64  `json:"bytes"`
	//Sid     string `json:"sid"`
	Unix       int64 `json:"unix"`
	RepeatNums int   `json:"repeat_nums"`
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
func Transport1(rw1, rw2 io.ReadWriter, address string, sid string) error {
	errc := make(chan error, 1)
	go func() {
		errc <- CopyBuffer1(rw1, rw2, bufferSize, address, sid)
	}()

	go func() {
		errc <- CopyBuffer1(rw2, rw1, bufferSize, address, sid)
	}()

	if err := <-errc; err != nil && err != io.EOF {
		return err
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
