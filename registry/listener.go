package registry

import (
	"github.com/liukeqqs/core/listener"
	"github.com/liukeqqs/core/logger"
)

type NewListener func(opts ...listener.Option) listener.Listener

type listenerRegistry struct {
	registry[NewListener]
}

func (r *listenerRegistry) Register(name string, v NewListener) error {
	if err := r.registry.Register(name, v); err != nil {
		logger.Default().Fatal(err)
	}
	return nil
}
