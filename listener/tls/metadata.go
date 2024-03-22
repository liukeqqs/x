package tls

import (
	mdata "github.com/liukeqqs/core/metadata"
	mdutil "github.com/liukeqqs/core/metadata/util"
)

type metadata struct {
	mptcp bool
}

func (l *tlsListener) parseMetadata(md mdata.Metadata) (err error) {
	l.md.mptcp = mdutil.GetBool(md, "mptcp")
	return
}
