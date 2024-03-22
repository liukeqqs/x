package tcp

import (
	md "github.com/liukeqqs/core/metadata"
	mdutil "github.com/liukeqqs/core/metadata/util"
)

type metadata struct {
	mptcp bool
}

func (l *tcpListener) parseMetadata(md md.Metadata) (err error) {
	l.md.mptcp = mdutil.GetBool(md, "mptcp")
	return
}
