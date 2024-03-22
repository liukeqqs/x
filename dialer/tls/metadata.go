package tls

import (
	"time"

	mdata "github.com/liukeqqs/core/metadata"
	mdutil "github.com/liukeqqs/core/metadata/util"
)

type metadata struct {
	handshakeTimeout time.Duration
}

func (d *tlsDialer) parseMetadata(md mdata.Metadata) (err error) {
	const (
		handshakeTimeout = "handshakeTimeout"
	)

	d.md.handshakeTimeout = mdutil.GetDuration(md, handshakeTimeout)

	return
}
