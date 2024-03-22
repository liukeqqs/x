package registry

import (
	"github.com/liukeqqs/core/service"
)

type serviceRegistry struct {
	registry[service.Service]
}
