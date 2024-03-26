package net

import (
	"bufio"
	"io"
	"log"
	"net"

	"github.com/liukeqqs/core/common/bufpool"
)

const (
	bufferSize = 64 * 1024
)

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
		buf := bufpool.Get(bufferSize)
		defer bufpool.Put(buf)
		bytes, err := io.CopyBuffer(rw1, rw2, buf)
		log.Printf("[上行流量：]--%s------%s------%s", address, bytes, sid)
	}()

	go func() {
		buf := bufpool.Get(bufferSize)
		defer bufpool.Put(buf)
		bytes, err := io.CopyBuffer(rw2, rw1, buf)
		log.Printf("[下行流量：]--%s------%s------%s", address, bytes, sid)
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
