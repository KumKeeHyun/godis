package command

import (
	resp "github.com/KumKeeHyun/godis/pkg/resp2"
	"github.com/KumKeeHyun/godis/pkg/sliceutil"
	"github.com/KumKeeHyun/godis/pkg/store"
	"strconv"
	"time"
)

func mustInt(reply resp.Reply) (i int) {
	i, _ = strconv.Atoi(reply.(resp.StringReply).Get())
	return
}

func toStringEntry(e store.Entry) (se *store.StringEntry, ok bool) {
	if e == nil {
		return nil, true
	}
	se, ok = e.(*store.StringEntry)
	return
}

var parseSet cmdParseFn = func(replies []resp.Reply) Command {
	cmd := &Set{
		key:  replies[1].(resp.StringReply).Get(),
		val:  replies[2].(resp.StringReply).Get(),
		from: time.Now(),
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

	from time.Time
	// ttl set the specified expire time
	// [0 | n]
	ttl time.Duration
	// expireAt set the specified Unix time at which the key will expire
	expireAt time.Time
	// keepTTL retain the time to live
	keepTTL bool
}

func (cmd *Set) expire() (time.Time, bool) {
	if cmd.ttl != 0 {
		return cmd.from.Add(cmd.ttl), true
	}
	empty := time.Time{}
	if cmd.expireAt != empty {
		return cmd.expireAt, true
	}
	return empty, false
}

func (cmd *Set) Run(s *store.Store) resp.Reply {
	var res resp.Reply = resp.OKReply

	err := s.Update(func(tx *store.Tx) error {
		oe, err := tx.Lookup(cmd.key)
		if err != nil {
			return err
		}
		old, ok := toStringEntry(oe)
		if !ok {
			return ErrWrongType
		}

		if (cmd.mod == "nx" && old != nil) || (cmd.mod == "xx" && old == nil) {
			res = resp.NewNilReply()
			return nil
		}

		if cmd.get {
			if old != nil {
				res = resp.NewBulkStringReply(old.Val)
			} else {
				res = resp.NewNilReply()
			}
		}

		ne := store.NewStrEntry(cmd.key, cmd.val)
		if expireAt, ok := cmd.expire(); ok {
			return tx.InsertEx(cmd.key, ne, expireAt)
		} else {
			return tx.Insert(cmd.key, ne)
		}
	})

	if err != nil {
		return resp.NewErrorReply(err)
	}
	return res
}

var parseGet cmdParseFn = func(replies []resp.Reply) Command {
	return &Get{
		key: replies[1].(resp.StringReply).Get(),
	}
}

type Get struct {
	key string
}

func (cmd *Get) Run(s *store.Store) resp.Reply {
	var val string
	err := s.Update(func(tx *store.Tx) error {
		e, err := tx.Lookup(cmd.key)
		if err != nil {
			return err
		}
		if e == nil {
			return ErrNil
		}
		if se, ok := e.(*store.StringEntry); !ok {
			return ErrWrongType
		} else {
			val = se.Val
		}
		return nil
	})
	if err != nil {
		return resp.NewErrorReply(err)
	}
	return resp.NewBulkStringReply(val)
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

func (cmd *MGet) Run(s *store.Store) resp.Reply {
	res := &resp.ArrayReply{
		Len:   len(cmd.keys),
		Value: make([]resp.Reply, 0, len(cmd.keys)),
	}
	err := s.Update(func(tx *store.Tx) error {
		for _, key := range cmd.keys {
			e, err := tx.Lookup(key)
			if err != nil {
				return err
			}
			if e == nil {
				res.Value = append(res.Value, resp.NewNilReply())
				continue
			}
			if se, ok := e.(*store.StringEntry); !ok {
				return ErrWrongType
			} else {
				res.Value = append(res.Value, resp.NewBulkStringReply(se.Val))
			}
		}
		return nil
	})
	if err != nil {
		return resp.NewErrorReply(err)
	}
	return res
}
