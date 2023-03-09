package command

import (
	"bytes"
	"context"
	"encoding/gob"
	"errors"
	resp "github.com/KumKeeHyun/godis/pkg/resp/v2"
	"github.com/KumKeeHyun/godis/pkg/sliceutil"
	"github.com/KumKeeHyun/godis/pkg/store"
	"time"
)

func toStringEntry(e store.Entry) (se *store.StringEntry, ok bool) {
	if e == nil {
		return nil, true
	}
	se, ok = e.(*store.StringEntry)
	return
}

var parseSet cmdParseFn = func(replies []resp.Reply) Command {
	cmd := &Set{
		Key:  mustString(replies[1]),
		Val:  mustString(replies[2]),
		From: time.Now(),
	}

	argIter := sliceutil.NewIterator(replies[3:])
	for argIter.HasNext() {
		token := argIter.Next().(resp.StringReply).Get()
		switch token {
		case "nx":
			cmd.Mod = token
		case "xx":
			cmd.Mod = token
		case "get":
			cmd.Get = true
		case "ex":
			ex := mustInt(argIter.Next())
			cmd.TTL = time.Duration(ex) * time.Second
		case "px":
			px := mustInt(argIter.Next())
			cmd.TTL = time.Duration(px) * time.Millisecond
		case "exat":
			exat := mustInt(argIter.Next())
			cmd.ExpireAt = time.Unix(int64(exat), 0)
		case "pxat":
			pxat := mustInt(argIter.Next())
			cmd.ExpireAt = time.UnixMilli(int64(pxat))
		case "keepttl":
			cmd.KeepTTL = true
		default:
			return &invalidCommand{errors.New("ERR illegal argument")}
		}
	}
	return cmd
}

type Set struct {
	Key, Val string

	// Mod only set the Key if ...
	// [NX | XX | ""]
	Mod string

	// Get return the old string stored at Key
	Get bool

	From time.Time
	// TTL set the specified expire time
	// [0 | n]
	TTL time.Duration
	// ExpireAt set the specified Unix time at which the Key will expire
	ExpireAt time.Time
	// KeepTTL retain the time to live
	KeepTTL bool
}

func (cmd *Set) Command() string {
	return "set"
}

func (cmd *Set) Marshal() ([]byte, error) {
	var buf bytes.Buffer
	err := gob.NewEncoder(&buf).Encode(cmd)
	return buf.Bytes(), err
}

func (cmd *Set) Unmarshal(b []byte) error {
	return gob.NewDecoder(bytes.NewBuffer(b)).Decode(cmd)
}

func (cmd *Set) Apply(ctx context.Context, s *store.Store) resp.Reply {
	var res resp.Reply = resp.OKReply

	err := s.Update(func(tx *store.Tx) error {
		oe, err := tx.Lookup(cmd.Key)
		if err != nil {
			return err
		}
		old, ok := toStringEntry(oe)
		if !ok {
			return ErrWrongType
		}

		if (cmd.Mod == "nx" && old != nil) || (cmd.Mod == "xx" && old == nil) {
			res = resp.NewNilReply()
			return nil
		}

		if cmd.Get {
			if old != nil {
				res = resp.NewBulkStringReply(old.Val)
			} else {
				res = resp.NewNilReply()
			}
		}

		ne := store.NewStrEntry(cmd.Key, cmd.Val)
		if expireAt, ok := cmd.expire(); ok {
			return tx.InsertEx(cmd.Key, ne, expireAt)
		} else {
			return tx.Insert(cmd.Key, ne)
		}
	})

	if err != nil {
		return resp.NewErrorReply(err)
	}
	return res
}

func (cmd *Set) expire() (time.Time, bool) {
	if cmd.TTL != 0 {
		return cmd.From.Add(cmd.TTL), true
	}
	empty := time.Time{}
	if cmd.ExpireAt != empty {
		return cmd.ExpireAt, true
	}
	return empty, false
}

var parseGet cmdParseFn = func(replies []resp.Reply) Command {
	return &Get{
		Key: mustString(replies[1]),
	}
}

type Get struct {
	Key string
}

func (cmd *Get) Command() string {
	return "Get"
}

func (cmd *Get) Apply(ctx context.Context, s *store.Store) resp.Reply {
	var val string
	err := s.Update(func(tx *store.Tx) error {
		e, err := tx.Lookup(cmd.Key)
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
		Keys: make([]string, 0, len(replies)-1),
	}

	argIter := sliceutil.NewIterator(replies[1:])
	for argIter.HasNext() {
		key := argIter.Next().(resp.StringReply).Get()
		cmd.Keys = append(cmd.Keys, key)
	}

	return cmd
}

type MGet struct {
	Keys []string
}

func (cmd *MGet) Command() string {
	return "mget"
}

func (cmd *MGet) Apply(ctx context.Context, s *store.Store) resp.Reply {
	res := &resp.ArrayReply{
		Len:   len(cmd.Keys),
		Value: make([]resp.Reply, 0, len(cmd.Keys)),
	}
	err := s.Update(func(tx *store.Tx) error {
		for _, key := range cmd.Keys {
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
