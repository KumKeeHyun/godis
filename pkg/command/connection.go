package command

import (
	"context"
	resp "github.com/KumKeeHyun/godis/pkg/resp/v2"
)

var parseHello cmdParseFn = func(replies []resp.Reply) Command {
	cmd := &Hello{}

	return cmd
}

type Hello struct{}

func (cmd *Hello) Command() string {
	return "hello"
}

func (cmd *Hello) Apply(ctx context.Context) resp.Reply {
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
