package command

import (
	"errors"
	resp "github.com/KumKeeHyun/godis/pkg/resp2"
	"github.com/KumKeeHyun/godis/pkg/storage"
	"strings"
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
	req[0] = strings.ToLower(req[0])

	switch {
	case req[0] == "hello":
		return &helloCommand{}
	case len(req) == 2 && req[0] == "get":
		return &getCommand{key: string(req[1])}
	case len(req) == 3 && req[0] == "set":
		return &setCommand{key: string(req[1]), value: string(req[2])}
	case len(req) == 2 && req[0] == "del":
		return &delCommand{key: string(req[1])}
	default:
		return &invalidCommand{errors.New("unknown command")}
	}
}

type helloCommand struct{}

func (cmd *helloCommand) Run(storage storage.Storage) resp.Reply {
	return &resp.ArrayReply{
		Len: 14,
		Value: []resp.Reply{
			&resp.SimpleStringReply{"server"},
			&resp.SimpleStringReply{"godis"},
			&resp.SimpleStringReply{"version"},
			&resp.SimpleStringReply{"255.255.255"},
			&resp.SimpleStringReply{"proto"},
			&resp.IntegerReply{2},
			&resp.SimpleStringReply{"id"},
			&resp.IntegerReply{5},
			&resp.SimpleStringReply{"mode"},
			&resp.SimpleStringReply{"standalone"},
			&resp.SimpleStringReply{"role"},
			&resp.SimpleStringReply{"master"},
			&resp.SimpleStringReply{"modules"},
			&resp.ArrayReply{},
		},
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
			Value: "redis: nil",
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
