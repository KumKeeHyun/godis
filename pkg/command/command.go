package command

import (
	"context"
	"errors"
	resp "github.com/KumKeeHyun/godis/pkg/resp/v2"
	"github.com/KumKeeHyun/godis/pkg/store"
	"go.etcd.io/etcd/raft/v3/raftpb"
	"strconv"
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

type EmptyCommand interface {
	Command
	Apply(ctx context.Context) resp.Reply
}

type StoreCommand interface {
	Command
	Apply(ctx context.Context, s *store.Store) resp.Reply
}

var parserFns = map[string]cmdParseFn{
	"hello":   parseHello,
	"cluster": parseCluster,
	"set":     parseSet,
	"get":     parseGet,
	"mget":    parseMGet,
}

func Parse(r resp.Reply) Command {
	arrReply, ok := r.(*resp.ArrayReply)
	if !ok {
		return &invalidCommand{errors.New("invalid format")}
	}
	if arrReply.IsNil() {
		return &invalidCommand{errors.New("empty request")}
	}

	cmdName := mustString(arrReply.Value[0])
	parse, exists := parserFns[cmdName]
	if !exists {
		return &invalidCommand{errors.New("ERR unknown command")}
	}
	cmd := parse(arrReply.Value)

	return cmd
}

func mustInt(reply resp.Reply) (i int) {
	i, _ = strconv.Atoi(mustString(reply))
	return
}

func mustString(reply resp.Reply) (s string) {
	return reply.(resp.StringReply).Get()
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

// ---------------------------
// for cluster

type WriteCommand interface {
	Command
	Marshal() ([]byte, error)
	Unmarshal([]byte) error
}

type ConfChangeCommand interface {
	Command
	ConfChange() raftpb.ConfChange
}

// ---------------------------
