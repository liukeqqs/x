package tls

import (
	md "github.com/liukeqqs/core/metadata"
	mdutil "github.com/liukeqqs/core/metadata/util"
)

type metadata struct {
	mptcp bool
}

func (l *obfsListener) parseMetadata(md md.Metadata) (err error) {
	l.md.mptcp = mdutil.GetBool(md, "mptcp")
	return
}
