package command

import (
	"github.com/KumKeeHyun/godis/pkg/storage"
)

type Command interface {
	Run(storage storage.Storage) *Response
}

func Parse(req *Request) Command {
	switch {
	case len(req.data) == 2 && req.data[0] == "GET":
		return &getCommand{key: string(req.data[1])}
	case len(req.data) == 3 && req.data[0] == "SET":
		return &setCommand{key: string(req.data[1]), value: string(req.data[2])}
	case len(req.data) == 2 && req.data[0] == "DEL":
		return &delCommand{key: string(req.data[1])}
	default:
		return &invalidCommand{}
	}
}

type invalidCommand struct{}

func (cmd *invalidCommand) Run(storage storage.Storage) *Response {
	return NewResponse(RES_ERR, []byte(""))
}

type getCommand struct {
	key string
}

func (cmd *getCommand) Run(storage storage.Storage) *Response {
	value, err := storage.Get(cmd.key)
	if err != nil {
		return NewResponse(RES_NOT_EXISTS, []byte(""))
	}
	return NewResponse(RES_OK, []byte(value))
}

type setCommand struct {
	key, value string
}

func (cmd *setCommand) Run(storage storage.Storage) *Response {
	err := storage.Set(cmd.key, cmd.value)
	if err != nil {
		return NewResponse(RES_ERR, []byte(""))
	}
	return NewResponse(RES_OK, []byte(""))
}

type delCommand struct {
	key string
}

func (cmd *delCommand) Run(storage storage.Storage) *Response {
	err := storage.Del(cmd.key)
	if err != nil {
		return NewResponse(RES_ERR, []byte(""))
	}
	return NewResponse(RES_OK, []byte(""))
}
