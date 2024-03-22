package tcp

import (
	mdata "github.com/liukeqqs/core/metadata"
	mdutil "github.com/liukeqqs/core/metadata/util"
)

type metadata struct {
	tproxy bool
	mptcp  bool
}

func (l *redirectListener) parseMetadata(md mdata.Metadata) (err error) {
	const (
		tproxy = "tproxy"
	)
	l.md.tproxy = mdutil.GetBool(md, tproxy)
	l.md.mptcp = mdutil.GetBool(md, "mptcp")
	return
}
