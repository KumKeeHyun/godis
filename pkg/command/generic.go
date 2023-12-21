package command

import (
	"context"
	"errors"
	"fmt"
	resp "github.com/KumKeeHyun/godis/pkg/resp/v2"
	"github.com/KumKeeHyun/godis/pkg/store"
	"regexp"
)

var parseKeys cmdParseFn = func(replies []resp.Reply) Command {
	if len(replies) < 2 {
		return &invalidCommand{errors.New("missing some arguments")}
	}
	return &Keys{
		Pattern: mustString(replies[1]),
	}
}

type Keys struct {
	Pattern string
}

func (cmd *Keys) Command() string {
	return "keys"
}

func (cmd *Keys) Apply(ctx context.Context, s *store.Store) resp.Reply {
	r, err := regexp.Compile(cmd.Pattern)
	if err != nil {
		resp.NewErrorReply(fmt.Errorf("failed to compile pattern: %v", err))
	}

	res := resp.NewNilArrayReply()
	err = s.Update(func(tx *store.Tx) error {
		keys := tx.Keys()
		res.Len = len(keys)
		res.Value = make([]resp.Reply, 0, len(keys))
		for _, key := range keys {
			if r.MatchString(key) {
				res.Value = append(res.Value, resp.NewBulkStringReply(key))
			}
		}
		return nil
	})
	if err != nil {
		return resp.NewErrorReply(err)
	}
	return res
}
