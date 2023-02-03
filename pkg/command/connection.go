package command

import (
	resp "github.com/KumKeeHyun/godis/pkg/resp2"
	"github.com/KumKeeHyun/godis/pkg/storage"
)

var parseHello cmdParseFn = func(replies []resp.Reply) Command {
	cmd := &Hello{}

	return cmd
}

type Hello struct{}

func (cmd *Hello) Run(storage storage.Storage) resp.Reply {
	return &resp.ArrayReply{
		Len: 14,
		Value: []resp.Reply{
			&resp.SimpleStringReply{Value: "server"},
			&resp.SimpleStringReply{Value: "godis"},
			&resp.SimpleStringReply{Value: "version"},
			&resp.SimpleStringReply{Value: "255.255.255"},
			&resp.SimpleStringReply{Value: "proto"},
			&resp.IntegerReply{Value: 2},
			&resp.SimpleStringReply{Value: "id"},
			&resp.IntegerReply{Value: 5},
			&resp.SimpleStringReply{Value: "mode"},
			&resp.SimpleStringReply{Value: "standalone"},
			&resp.SimpleStringReply{Value: "role"},
			&resp.SimpleStringReply{Value: "master"},
			&resp.SimpleStringReply{Value: "modules"},
			&resp.ArrayReply{},
		},
	}
}
