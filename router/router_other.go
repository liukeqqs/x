//go:build !linux

package router

import (
	"github.com/liukeqqs/core/router"
)

func (*localRouter) setSysRoutes(routes ...*router.Route) error {
	return nil
}
