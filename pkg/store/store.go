package store

import (
	"context"
	"log"
	"time"
)

const (
	maxWorks = 1000
	ttlTick  = time.Millisecond * 100
)

type (
	txid uint64

	txFn func(tx *Tx) error

	job struct {
		txFn txFn
		done chan error
	}
)

type Store struct {
	hmap map[string]Entry
	heap *_heap

	jobCh chan job
}

func New(ctx context.Context) *Store {
	s := &Store{
		hmap:  make(map[string]Entry, 1_000_000),
		heap:  newHeap(1_000),
		jobCh: make(chan job),
	}
	go s.run(ctx)
	return s
}

func (s *Store) run(ctx context.Context) {
	ticker := time.NewTicker(ttlTick)
	for {
		select {
		case job := <-s.jobCh:
			tx := Tx{s: s}
			job.done <- job.txFn(&tx)
			close(job.done)
		case <-ticker.C:
			expired := s.processTTL()
			if expired > 0 {
				log.Printf("count of expired: %d", expired)
			}
		case <-ctx.Done():
			return
		}
	}
}

func (s *Store) processTTL() int {
	now := time.Now()
	works := 0

	for i := s.heap.Peek(); i != nil && i.ExpireAt.Before(now); i = s.heap.Peek() {
		entry := s.heap.Pop().Ref
		delete(s.hmap, entry.Key())

		works++
		if works >= maxWorks {
			break
		}
	}

	return works
}

func (s *Store) process(txFn txFn) (err error) {
	job := job{
		txFn: txFn,
		done: make(chan error),
	}
	s.jobCh <- job
	err = <-job.done

	job.txFn = nil
	job.done = nil
	return
}

func (s *Store) Update(txFn txFn) (err error) {
	return s.process(txFn)
}

type Tx struct {
	s *Store
}

func (tx *Tx) Lookup(key string) (entry Entry, err error) {
	entry = tx.s.hmap[key]
	return
}

func (tx *Tx) Insert(key string, entry Entry) (err error) {
	old := tx.s.hmap[key]
	if old != nil && old.heapIdx() != -1 {
		tx.s.heap.Remove(old)
	}
	tx.s.hmap[key] = entry
	return
}

func (tx *Tx) InsertEx(key string, entry Entry, expireAt time.Time) (err error) {
	old := tx.s.hmap[key]
	if old != nil {
		tx.s.heap.Remove(old)
	}
	tx.s.hmap[key] = entry
	tx.s.heap.Push(entry, expireAt)
	return
}

func (tx *Tx) UpdateEx(key string, expireAt time.Time) (err error) {
	entry := tx.s.hmap[key]
	if entry != nil {
		tx.s.heap.Update(entry, expireAt)
	}
	return
}

func (tx *Tx) Delete(key string) (err error) {
	old := tx.s.hmap[key]
	if old != nil {
		tx.s.heap.Remove(old)
	}
	return
}

func (s *Store) GetSnapshot() ([]byte, error) {
	// TODO: serialize snapshot
	return []byte("not yet implemented"), nil
}

func (s *Store) RecoverFromSnapshot(snapshot []byte) error {
	// TODO: deserialize snapshot
	return nil
}
