package command

import (
	"context"
	"errors"
	resp "github.com/KumKeeHyun/godis/pkg/resp/v2"
	"github.com/KumKeeHyun/godis/pkg/store"
)

type (
	cmdParseFn func(replies []resp.Reply) Command
)

var (
	ErrNil             = errors.New("redis: nil")
	ErrWrongType error = errors.New("WRONGTYPE Operation against a key holding the wrong kind of value")
)

// Command temp interface for execute cmd
type Command interface {
	Command() string
}

type WriteCommand interface {
	Command
	Marshal() ([]byte, error)
	Unmarshal([]byte) error
}

type EmptyCommand interface {
	Apply(ctx context.Context) resp.Reply
}

type StoreCommand interface {
	Apply(ctx context.Context, s *store.Store) resp.Reply
}

var parserFns = map[string]cmdParseFn{
	"hello": parseHello,
	"set":   parseSet,
	"get":   parseGet,
	"mget":  parseMGet,
}

func Parse(r resp.Reply) Command {
	arrReply, ok := r.(*resp.ArrayReply)
	if !ok {
		return &invalidCommand{errors.New("invalid format")}
	}
	if arrReply.IsNil() {
		return &invalidCommand{errors.New("empty request")}
	}

	cmdName := arrReply.Value[0].(resp.StringReply).Get()
	parse, exists := parserFns[cmdName]
	if !exists {
		return &invalidCommand{errors.New("ERR unknown command")}
	}
	cmd := parse(arrReply.Value)

	return cmd
}

type invalidCommand struct {
	err error
}

func (cmd *invalidCommand) Command() string {
	return "invalid"
}

func (cmd *invalidCommand) Apply(context.Context) resp.Reply {
	return &resp.ErrorReply{
		Value: cmd.err.Error(),
	}
}
