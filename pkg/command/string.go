package command

import (
	resp "github.com/KumKeeHyun/godis/pkg/resp2"
	"github.com/KumKeeHyun/godis/pkg/sliceutil"
	"github.com/KumKeeHyun/godis/pkg/storage"
	"strconv"
	"time"
)

func mustInt(reply resp.Reply) (i int) {
	i, _ = strconv.Atoi(reply.(resp.StringReply).Get())
	return
}

var parseSet cmdParseFn = func(replies []resp.Reply) Command {
	cmd := &Set{
		key: replies[1].(resp.StringReply).Get(),
		val: replies[2].(resp.StringReply).Get(),
	}

	argIter := sliceutil.NewIterator(replies[3:])
	for argIter.HasNext() {
		token := argIter.Next().(resp.StringReply).Get()
		switch token {
		case "nx":
			cmd.mod = token
		case "xx":
			cmd.mod = token
		case "get":
			cmd.get = true
		case "ex":
			ex := mustInt(argIter.Next())
			cmd.ttl = time.Duration(ex) * time.Second
		case "px":
			px := mustInt(argIter.Next())
			cmd.ttl = time.Duration(px) * time.Millisecond
		case "exat":
			exat := mustInt(argIter.Next())
			cmd.expireAt = time.Unix(int64(exat), 0)
		case "pxat":
			pxat := mustInt(argIter.Next())
			cmd.expireAt = time.UnixMilli(int64(pxat))
		case "keepttl":
			cmd.keepTTL = true
		}
	}
	return cmd
}

type Set struct {
	key, val string

	// mod only set the key if ...
	// [NX | XX | ""]
	mod string

	// get return the old string stored at key
	get bool

	// ttl set the specified expire time
	// [0 | n]
	ttl time.Duration
	// expireAt set the specified Unix time at which the key will expire
	expireAt time.Time
	// keepTTL retain the time to live
	keepTTL bool
}

func (cmd *Set) Run(storage storage.Storage) resp.Reply {
	err := storage.Set(cmd.key, cmd.val)
	if err != nil {
		return &resp.ErrorReply{
			Value: err.Error(),
		}
	}
	return &resp.SimpleStringReply{Value: "OK"}
}

var parseGet cmdParseFn = func(replies []resp.Reply) Command {
	return &Get{
		key: replies[1].(resp.StringReply).Get(),
	}
}

type Get struct {
	key string
}

func (cmd *Get) Run(storage storage.Storage) resp.Reply {
	val, err := storage.Get(cmd.key)
	if err != nil {
		return &resp.ErrorReply{
			Value: "redis: nil",
		}
	}
	return &resp.BulkStringReply{Len: len(val), Value: val}
}

var parseMGet cmdParseFn = func(replies []resp.Reply) Command {
	cmd := &MGet{
		keys: make([]string, 0, len(replies)-1),
	}

	argIter := sliceutil.NewIterator(replies[1:])
	for argIter.HasNext() {
		key := argIter.Next().(resp.StringReply).Get()
		cmd.keys = append(cmd.keys, key)
	}

	return cmd
}

type MGet struct {
	keys []string
}

func (cmd *MGet) Run(storage storage.Storage) resp.Reply {
	res := &resp.ArrayReply{
		Len:   len(cmd.keys),
		Value: make([]resp.Reply, 0, len(cmd.keys)),
	}
	for _, key := range cmd.keys {
		val, err := storage.Get(key)
		if err != nil {
			res.Value = append(res.Value, &resp.BulkStringReply{Len: -1})
			continue
		}
		res.Value = append(res.Value, &resp.BulkStringReply{
			Len:   len(val),
			Value: val,
		})
	}
	return res
}
