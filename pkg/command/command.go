package command

import (
	"errors"
	resp "github.com/KumKeeHyun/godis/pkg/resp2"
	"github.com/KumKeeHyun/godis/pkg/store"
	"log"
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
	Run(s *store.Store) resp.Reply
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
	log.Println(arrReply.Value)

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

func (cmd *invalidCommand) Run(s *store.Store) resp.Reply {
	return &resp.ErrorReply{
		Value: cmd.err.Error(),
	}
}
