package wrapper

import (
	"io"
	"log"

	"github.com/liukeqqs/x/stats"
)

// readWriter is an io.ReadWriter with Stats.
type readWriter struct {
	io.ReadWriter
	stats *stats.Stats
}

func WrapReadWriter(rw io.ReadWriter, stats *stats.Stats) io.ReadWriter {
	if stats == nil {
		return rw
	}

	return &readWriter{
		ReadWriter: rw,
		stats:      stats,
	}
}

func (p *readWriter) Read(b []byte) (n int, err error) {
	n, err = p.ReadWriter.Read(b)
	log.Printf("[Read：]--%s", int64(n))
	p.stats.Add(stats.KindInputBytes, int64(n))

	return
}

func (p *readWriter) Write(b []byte) (n int, err error) {
	n, err = p.ReadWriter.Write(b)
	log.Printf("[Write：]--%s", int64(n))

	p.stats.Add(stats.KindOutputBytes, int64(n))

	return
}
