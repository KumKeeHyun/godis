package store

import (
	"container/heap"
	"time"
)

type item struct {
	expireAt time.Time
	ref      Entry
}

type items []*item

func (is items) Len() int {
	return len(is)
}

func (is items) Less(i, j int) bool {
	return is[i].expireAt.Before(is[j].expireAt)
}

func (is items) Swap(i, j int) {
	is[i], is[j] = is[j], is[i]
	is[i].ref.setHeapIdx(i)
	is[j].ref.setHeapIdx(j)
}

func (is *items) Push(x any) {
	item := x.(*item)
	item.ref.setHeapIdx(is.Len())
	*is = append(*is, item)
}

func (is *items) Pop() any {
	old := *is
	n := is.Len()
	item := old[n-1]
	old[n-1] = nil
	item.ref.setHeapIdx(-1)
	*is = old[:n-1]
	return item
}

type _heap struct {
	items *items
}

func newHeap(size int) *_heap {
	is := make(items, 0, size)
	return &_heap{items: &is}
}

func (h *_heap) Empty() bool {
	return h.items.Len() == 0
}

func (h *_heap) Push(e Entry, expireAt time.Time) {
	e.setHeapIdx(-1)
	heap.Push(h.items, &item{
		expireAt: expireAt,
		ref:      e,
	})
}

func (h *_heap) Pop() *item {
	return heap.Pop(h.items).(*item)
}

func (h *_heap) Peek() *item {
	if h.Empty() {
		return nil
	}
	return (*h.items)[0]
}

func (h *_heap) Update(e Entry, expireAt time.Time) {
	idx := e.heapIdx()
	if idx != -1 {
		(*h.items)[idx].expireAt = expireAt
		heap.Fix(h.items, idx)
	}
}

func (h *_heap) Remove(e Entry) {
	idx := e.heapIdx()
	if idx != -1 {
		heap.Remove(h.items, idx)
	}
}
