package kcp

import (
	"encoding/json"

	mdata "github.com/liukeqqs/core/metadata"
	mdutil "github.com/liukeqqs/core/metadata/util"
	kcp_util "github.com/liukeqqs/x/internal/util/kcp"
)

const (
	defaultBacklog = 128
)

type metadata struct {
	config  *kcp_util.Config
	backlog int
}

func (l *kcpListener) parseMetadata(md mdata.Metadata) (err error) {
	const (
		backlog    = "backlog"
		configFile = "c"
	)

	if file := mdutil.GetString(md, "kcp.configFile", "configFile", "c"); file != "" {
		l.md.config, err = kcp_util.ParseFromFile(file)
		if err != nil {
			return
		}
	}

	if m := mdutil.GetStringMap(md, "kcp.config", "config"); len(m) > 0 {
		b, err := json.Marshal(m)
		if err != nil {
			return err
		}
		cfg := &kcp_util.Config{}
		if err := json.Unmarshal(b, cfg); err != nil {
			return err
		}
		l.md.config = cfg
	}

	if l.md.config == nil {
		l.md.config = kcp_util.DefaultConfig
	}
	l.md.config.TCP = mdutil.GetBool(md, "kcp.tcp", "tcp")
	l.md.config.Key = mdutil.GetString(md, "kcp.key")
	l.md.config.Crypt = mdutil.GetString(md, "kcp.crypt")
	l.md.config.Mode = mdutil.GetString(md, "kcp.mode")
	l.md.config.KeepAlive = mdutil.GetInt(md, "kcp.keepalive")
	l.md.config.Interval = mdutil.GetInt(md, "kcp.interval")
	l.md.config.MTU = mdutil.GetInt(md, "kcp.mtu")
	l.md.config.SmuxVer = mdutil.GetInt(md, "kcp.smuxVer")

	l.md.backlog = mdutil.GetInt(md, backlog)
	if l.md.backlog <= 0 {
		l.md.backlog = defaultBacklog
	}

	return
}
