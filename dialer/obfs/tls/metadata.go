package tls

import (
	mdata "github.com/liukeqqs/core/metadata"
	mdutil "github.com/liukeqqs/core/metadata/util"
)

type metadata struct {
	host string
}

func (d *obfsTLSDialer) parseMetadata(md mdata.Metadata) (err error) {
	const (
		host = "host"
	)

	d.md.host = mdutil.GetString(md, host)
	return
}
