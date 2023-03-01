package store

import (
	"container/heap"
	"errors"
	"time"
)

type item struct {
	ExpireAt time.Time `json:"expireAt"`
	Ref      Entry     `json:"-"`
}

type items []*item

func (is items) Len() int {
	return len(is)
}

func (is items) Less(i, j int) bool {
	return is[i].ExpireAt.Before(is[j].ExpireAt)
}

func (is items) Swap(i, j int) {
	is[i], is[j] = is[j], is[i]
	is[i].Ref.setHeapIdx(i)
	is[j].Ref.setHeapIdx(j)
}

func (is *items) Push(x any) {
	item := x.(*item)
	item.Ref.setHeapIdx(is.Len())
	*is = append(*is, item)
}

func (is *items) Pop() any {
	old := *is
	n := is.Len()
	item := old[n-1]
	old[n-1] = nil
	item.Ref.setHeapIdx(-1)
	*is = old[:n-1]
	return item
}

type _heap struct {
	Items *items `json:"items"`
}

func newHeap(size int) *_heap {
	is := make(items, 0, size)
	return &_heap{Items: &is}
}

func (h *_heap) Empty() bool {
	return h.Items.Len() == 0
}

func (h *_heap) Push(e Entry, expireAt time.Time) {
	e.setHeapIdx(-1)
	heap.Push(h.Items, &item{
		ExpireAt: expireAt,
		Ref:      e,
	})
}

func (h *_heap) Pop() *item {
	return heap.Pop(h.Items).(*item)
}

func (h *_heap) Peek() *item {
	if h.Empty() {
		return nil
	}
	return (*h.Items)[0]
}

func (h *_heap) Update(e Entry, expireAt time.Time) {
	idx := e.heapIdx()
	if idx != -1 {
		(*h.Items)[idx].ExpireAt = expireAt
		heap.Fix(h.Items, idx)
	}
}

func (h *_heap) Remove(e Entry) {
	idx := e.heapIdx()
	if idx != -1 {
		heap.Remove(h.Items, idx)
	}
}

func (h *_heap) GetEx(e Entry) (time.Time, error) {
	idx := e.heapIdx()
	if idx == -1 {
		return time.Time{}, errors.New("expire not set")
	}
	return (*h.Items)[idx].ExpireAt, nil
}
