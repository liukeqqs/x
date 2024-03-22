package hop

import (
	"crypto/tls"
	"strings"

	"github.com/liukeqqs/core/bypass"
	"github.com/liukeqqs/core/chain"
	"github.com/liukeqqs/core/hop"
	"github.com/liukeqqs/core/logger"
	"github.com/liukeqqs/x/config"
	bypass_parser "github.com/liukeqqs/x/config/parsing/bypass"
	node_parser "github.com/liukeqqs/x/config/parsing/node"
	selector_parser "github.com/liukeqqs/x/config/parsing/selector"
	xhop "github.com/liukeqqs/x/hop"
	hop_plugin "github.com/liukeqqs/x/hop/plugin"
	"github.com/liukeqqs/x/internal/loader"
	"github.com/liukeqqs/x/internal/plugin"
)

func ParseHop(cfg *config.HopConfig, log logger.Logger) (hop.Hop, error) {
	if cfg == nil {
		return nil, nil
	}

	if cfg.Plugin != nil {
		var tlsCfg *tls.Config
		if cfg.Plugin.TLS != nil {
			tlsCfg = &tls.Config{
				ServerName:         cfg.Plugin.TLS.ServerName,
				InsecureSkipVerify: !cfg.Plugin.TLS.Secure,
			}
		}
		switch strings.ToLower(cfg.Plugin.Type) {
		case "http":
			return hop_plugin.NewHTTPPlugin(
				cfg.Name, cfg.Plugin.Addr,
				plugin.TLSConfigOption(tlsCfg),
				plugin.TimeoutOption(cfg.Plugin.Timeout),
			), nil
		default:
			return hop_plugin.NewGRPCPlugin(
				cfg.Name, cfg.Plugin.Addr,
				plugin.TokenOption(cfg.Plugin.Token),
				plugin.TLSConfigOption(tlsCfg),
			), nil
		}
	}

	var nodes []*chain.Node
	for _, v := range cfg.Nodes {
		if v == nil {
			continue
		}

		if v.Resolver == "" {
			v.Resolver = cfg.Resolver
		}
		if v.Hosts == "" {
			v.Hosts = cfg.Hosts
		}
		if v.Interface == "" {
			v.Interface = cfg.Interface
		}
		if v.SockOpts == nil {
			v.SockOpts = cfg.SockOpts
		}

		if v.Connector == nil {
			v.Connector = &config.ConnectorConfig{
				Type: "http",
			}
		}

		if v.Dialer == nil {
			v.Dialer = &config.DialerConfig{
				Type: "tcp",
			}
		}

		node, err := node_parser.ParseNode(cfg.Name, v, log)
		if err != nil {
			return nil, err
		}
		if node != nil {
			nodes = append(nodes, node)
		}
	}

	sel := selector_parser.ParseNodeSelector(cfg.Selector)
	if sel == nil {
		sel = selector_parser.DefaultNodeSelector()
	}

	opts := []xhop.Option{
		xhop.NameOption(cfg.Name),
		xhop.NodeOption(nodes...),
		xhop.SelectorOption(sel),
		xhop.BypassOption(bypass.BypassGroup(bypass_parser.List(cfg.Bypass, cfg.Bypasses...)...)),
		xhop.ReloadPeriodOption(cfg.Reload),
		xhop.LoggerOption(log.WithFields(map[string]any{
			"kind": "hop",
			"hop":  cfg.Name,
		})),
	}

	if cfg.File != nil && cfg.File.Path != "" {
		opts = append(opts, xhop.FileLoaderOption(loader.FileLoader(cfg.File.Path)))
	}
	if cfg.Redis != nil && cfg.Redis.Addr != "" {
		opts = append(opts, xhop.RedisLoaderOption(loader.RedisStringLoader(
			cfg.Redis.Addr,
			loader.DBRedisLoaderOption(cfg.Redis.DB),
			loader.PasswordRedisLoaderOption(cfg.Redis.Password),
			loader.KeyRedisLoaderOption(cfg.Redis.Key),
		)))
	}
	if cfg.HTTP != nil && cfg.HTTP.URL != "" {
		opts = append(opts, xhop.HTTPLoaderOption(loader.HTTPLoader(
			cfg.HTTP.URL,
			loader.TimeoutHTTPLoaderOption(cfg.HTTP.Timeout),
		)))
	}
	return xhop.NewHop(opts...), nil
}
