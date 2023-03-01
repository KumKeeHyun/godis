package store

import (
	"context"
	"github.com/stretchr/testify/assert"
	"testing"
	"time"
)

func TestStore_Snapshot(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	s := New(ctx)

	s.Update(func(tx *Tx) error {
		tx.InsertEx("test1", NewStrEntry("test1", "test1"), time.Now().Add(time.Hour))
		tx.Insert("test2", NewStrEntry("test2", "test2"))
		tx.InsertEx("test3", NewStrEntry("test3", "test3"), time.Now().Add(time.Hour))
		tx.Insert("test4", NewStrEntry("test4", "test4"))
		return nil
	})

	snap, err := s.GetSnapshot()
	if err != nil {
		t.Error(err)
	}
	t.Log(string(snap))

	news := New(ctx)
	err = news.RecoverFromSnapshot(snap)
	if err != nil {
		t.Error(err)
	}

	news.Update(func(tx *Tx) error {
		e, err := tx.Lookup("test1")
		t.Log(e)
		assert.Nil(t, err)
		assert.Equal(t, 0, e.heapIdx())

		e, err = tx.Lookup("test2")
		t.Log(e)
		assert.Nil(t, err)
		assert.Equal(t, -1, e.heapIdx())

		e, err = tx.Lookup("test3")
		t.Log(e)
		assert.Nil(t, err)
		assert.Equal(t, 1, e.heapIdx())

		e, err = tx.Lookup("test4")
		t.Log(e)
		assert.Nil(t, err)
		assert.Equal(t, -1, e.heapIdx())
		return nil
	})
}
