package udp

import (
	"time"

	mdata "github.com/liukeqqs/core/metadata"
	mdutil "github.com/liukeqqs/core/metadata/util"
)

const (
	defaultTTL            = 30 * time.Second
	defaultReadBufferSize = 4096
)

type metadata struct {
	ttl            time.Duration
	readBufferSize int
}

func (l *redirectListener) parseMetadata(md mdata.Metadata) (err error) {
	const (
		ttl            = "ttl"
		readBufferSize = "readBufferSize"
	)

	l.md.ttl = mdutil.GetDuration(md, ttl)
	if l.md.ttl <= 0 {
		l.md.ttl = defaultTTL
	}

	l.md.readBufferSize = mdutil.GetInt(md, readBufferSize)
	if l.md.readBufferSize <= 0 {
		l.md.readBufferSize = defaultReadBufferSize
	}

	return
}
