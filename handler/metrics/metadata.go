package file

import (
	mdata "github.com/liukeqqs/core/metadata"
	mdutil "github.com/liukeqqs/core/metadata/util"
)

const (
	DefaultPath = "/metrics"
)

type metadata struct {
	path string
}

func (h *metricsHandler) parseMetadata(md mdata.Metadata) (err error) {
	h.md.path = mdutil.GetString(md, "metrics.path", "path")
	if h.md.path == "" {
		h.md.path = DefaultPath
	}
	return
}
