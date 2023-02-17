package apply

import (
	"context"
	"github.com/KumKeeHyun/godis/pkg/command"
	resp "github.com/KumKeeHyun/godis/pkg/resp/v2"
	"github.com/KumKeeHyun/godis/pkg/store"
)

type Applier interface {
	Apply(ctx context.Context, cmd command.Command) resp.Reply
}

type applier struct {
	s *store.Store
}

func NewApplier(s *store.Store) Applier {
	return &applier{s: s}
}

func (a *applier) Apply(ctx context.Context, cmd command.Command) resp.Reply {
	switch c := cmd.(type) {
	case command.StoreCommand:
		return c.Apply(ctx, a.s)
	case command.EmptyCommand:
		return c.Apply(ctx)
	default:
		return nil
	}
}
