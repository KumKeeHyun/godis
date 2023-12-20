package command

import (
	"bytes"
	"context"
	"encoding/gob"
	"errors"
	resp "github.com/KumKeeHyun/godis/pkg/resp/v2"
	"github.com/KumKeeHyun/godis/pkg/sliceutil"
	"github.com/KumKeeHyun/godis/pkg/store"
)

var parseSAdd cmdParseFn = func(replies []resp.Reply) Command {
	if len(replies) < 3 {
		return &invalidCommand{errors.New("missing some arguments")}
	}
	cmd := &SAdd{
		Key:     mustString(replies[1]),
		Members: make([]string, 0, len(replies)-2),
	}
	argIter := sliceutil.NewIterator(replies[2:])
	for argIter.HasNext() {
		cmd.Members = append(cmd.Members, mustString(argIter.Next()))
	}
	return cmd
}

type SAdd struct {
	Key     string
	Members []string
}

func (cmd *SAdd) Command() string {
	return "sadd"
}

func (cmd *SAdd) Marshal() ([]byte, error) {
	var buf bytes.Buffer
	err := gob.NewEncoder(&buf).Encode(cmd)
	return buf.Bytes(), err
}

func (cmd *SAdd) Unmarshal(b []byte) error {
	return gob.NewDecoder(bytes.NewBuffer(b)).Decode(cmd)
}

func (cmd *SAdd) Apply(ctx context.Context, s *store.Store) resp.Reply {
	result := 0
	err := s.Update(func(tx *store.Tx) error {
		e, err := tx.Lookup(cmd.Key)
		if err != nil {
			return err
		}
		if e == nil {
			e = store.NewSetEntry(cmd.Key)
			if err := tx.Insert(cmd.Key, e); err != nil {
				return err
			}
		}
		se, ok := e.(*store.SetEntry)
		if !ok {
			return ErrWrongType
		}

		for _, member := range cmd.Members {
			if _, exists := se.Val[member]; !exists {
				result++
			}
			se.Val[member] = struct{}{}
		}
		return nil
	})
	if err != nil {
		return resp.NewErrorReply(err)
	}
	return resp.NewIntegerReply(result)
}

var parseSCard cmdParseFn = func(replies []resp.Reply) Command {
	if len(replies) < 2 {
		return &invalidCommand{errors.New("missing some arguments")}
	}
	return &SCard{Key: mustString(replies[1])}
}

type SCard struct {
	Key string
}

func (cmd *SCard) Command() string {
	return "scard"
}

func (cmd *SCard) Apply(ctx context.Context, s *store.Store) resp.Reply {
	result := 0
	err := s.Update(func(tx *store.Tx) error {
		e, err := tx.Lookup(cmd.Key)
		if err != nil {
			return err
		}
		if e == nil {
			return nil
		}

		se, ok := e.(*store.SetEntry)
		if !ok {
			return ErrWrongType
		}

		result = len(se.Val)
		return nil
	})
	if err != nil {
		return resp.NewErrorReply(err)
	}
	return resp.NewIntegerReply(result)
}

var parseSMembers cmdParseFn = func(replies []resp.Reply) Command {
	if len(replies) < 2 {
		return &invalidCommand{errors.New("missing some arguments")}
	}
	return &SMembers{Key: mustString(replies[1])}
}

type SMembers struct {
	Key string
}

func (cmd *SMembers) Command() string {
	return "smembers"
}

func (cmd *SMembers) Apply(ctx context.Context, s *store.Store) resp.Reply {
	res := resp.NewNilArrayReply()
	err := s.Update(func(tx *store.Tx) error {
		e, err := tx.Lookup(cmd.Key)
		if err != nil {
			return err
		}
		if e == nil {
			return nil
		}

		se, ok := e.(*store.SetEntry)
		if !ok {
			return ErrWrongType
		}

		res.Len = len(se.Val)
		res.Value = make([]resp.Reply, 0, len(se.Val))
		for member := range se.Val {
			res.Value = append(res.Value, resp.NewBulkStringReply(member))
		}
		return nil
	})
	if err != nil {
		return resp.NewErrorReply(err)
	}
	return res
}

var parseSRem cmdParseFn = func(replies []resp.Reply) Command {
	if len(replies) < 3 {
		return &invalidCommand{errors.New("missing some arguments")}
	}
	cmd := &SRem{
		Key:     mustString(replies[1]),
		Members: make([]string, 0, len(replies)-2),
	}
	argIter := sliceutil.NewIterator(replies[2:])
	for argIter.HasNext() {
		cmd.Members = append(cmd.Members, mustString(argIter.Next()))
	}
	return cmd
}

type SRem struct {
	Key     string
	Members []string
}

func (cmd *SRem) Command() string {
	return "srem"
}

func (cmd *SRem) Apply(ctx context.Context, s *store.Store) resp.Reply {
	result := 0
	err := s.Update(func(tx *store.Tx) error {
		e, err := tx.Lookup(cmd.Key)
		if err != nil {
			return err
		}
		if e == nil {
			return nil
		}

		se, ok := e.(*store.SetEntry)
		if !ok {
			return ErrWrongType
		}

		for _, member := range cmd.Members {
			if _, exists := se.Val[member]; exists {
				result++
				delete(se.Val, member)
			}
		}
		return nil
	})
	if err != nil {
		return resp.NewErrorReply(err)
	}
	return resp.NewIntegerReply(result)
}

func (cmd *SRem) Marshal() ([]byte, error) {
	var buf bytes.Buffer
	err := gob.NewEncoder(&buf).Encode(cmd)
	return buf.Bytes(), err
}

func (cmd *SRem) Unmarshal(b []byte) error {
	return gob.NewDecoder(bytes.NewBuffer(b)).Decode(cmd)
}
