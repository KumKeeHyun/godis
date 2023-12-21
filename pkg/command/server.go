package command

import (
	"context"
	"errors"
	resp "github.com/KumKeeHyun/godis/pkg/resp/v2"
	"github.com/KumKeeHyun/godis/pkg/sliceutil"
	"strings"
)

var configParseFns = map[string]cmdParseFn{
	"get": parseConfigGet,
}

var parseConfig cmdParseFn = func(replies []resp.Reply) Command {
	cmdName := strings.ToLower(mustString(replies[1]))
	parse, exists := configParseFns[cmdName]
	if !exists {
		return &invalidCommand{errors.New("ERR unknown command")}
	}
	return parse(replies)
}

var parseConfigGet cmdParseFn = func(replies []resp.Reply) Command {
	cmd := &ConfigGet{
		Parameters: make([]string, 0, len(replies)-1),
	}

	argIter := sliceutil.NewIterator(replies[1:])
	for argIter.HasNext() {
		param := argIter.Next().(resp.StringReply).Get()
		cmd.Parameters = append(cmd.Parameters, param)
	}

	return cmd
}

type ConfigGet struct {
	Parameters []string
}

func (cmd *ConfigGet) Command() string {
	return "config get"
}

var conf = map[string]string{
	"save":       "",   // disable snapshot
	"appendonly": "no", // disable aof
}

func (cmd *ConfigGet) Apply(ctx context.Context) resp.Reply {
	res := &resp.ArrayReply{
		Len:   len(cmd.Parameters),
		Value: make([]resp.Reply, 0, len(cmd.Parameters)),
	}
	for _, param := range cmd.Parameters {
		res.Value = append(res.Value, resp.NewBulkStringReply(conf[param]))
	}
	return res
}
