package serial

import (
	"time"

	mdata "github.com/liukeqqs/core/metadata"
	mdutil "github.com/liukeqqs/core/metadata/util"
)

const (
	defaultPort     = "COM1"
	defaultBaudRate = 9600
)

type metadata struct {
	timeout time.Duration
}

func (h *serialHandler) parseMetadata(md mdata.Metadata) (err error) {
	h.md.timeout = mdutil.GetDuration(md, "timeout", "serial.timeout", "handler.serial.timeout")
	return
}
