package command

import (
	"errors"
	resp "github.com/KumKeeHyun/godis/pkg/resp2"
	"github.com/KumKeeHyun/godis/pkg/storage"
)

type Command interface {
	Run(storage storage.Storage) resp.Reply
}

func Parse(r resp.Reply) Command {
	ar, ok := r.(*resp.ArrayReply)
	if !ok {
		return &invalidCommand{errors.New("invalid format")}
	}
	if ar.IsNil() {
		return &invalidCommand{errors.New("empty request")}
	}

	req := make([]string, ar.Len)
	for i := 0; i < ar.Len; i++ {
		req[i] = ar.Value[i].String()
	}
	switch {
	case len(req) == 2 && req[0] == "GET":
		return &getCommand{key: string(req[1])}
	case len(req) == 3 && req[0] == "SET":
		return &setCommand{key: string(req[1]), value: string(req[2])}
	case len(req) == 2 && req[0] == "DEL":
		return &delCommand{key: string(req[1])}
	default:
		return &invalidCommand{errors.New("unknown command")}
	}
}

type invalidCommand struct {
	err error
}

func (cmd *invalidCommand) Run(storage storage.Storage) resp.Reply {
	return &resp.ErrorReply{
		Value: cmd.err.Error(),
	}
}

type getCommand struct {
	key string
}

func (cmd *getCommand) Run(storage storage.Storage) resp.Reply {
	value, err := storage.Get(cmd.key)
	if err != nil {
		return &resp.ErrorReply{
			Value: err.Error(),
		}
	}
	return &resp.SimpleStringReply{Value: value}
}

type setCommand struct {
	key, value string
}

func (cmd *setCommand) Run(storage storage.Storage) resp.Reply {
	err := storage.Set(cmd.key, cmd.value)
	if err != nil {
		return &resp.ErrorReply{
			Value: err.Error(),
		}
	}
	return &resp.SimpleStringReply{Value: "OK"}
}

type delCommand struct {
	key string
}

func (cmd *delCommand) Run(storage storage.Storage) resp.Reply {
	err := storage.Del(cmd.key)
	if err != nil {
		return &resp.ErrorReply{
			Value: err.Error(),
		}
	}
	return &resp.SimpleStringReply{Value: "OK"}
}
