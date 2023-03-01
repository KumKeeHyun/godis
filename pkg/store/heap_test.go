package store

import (
	"github.com/stretchr/testify/assert"
	"testing"
	"time"
)

func TestHeap(t *testing.T) {
	e1 := &BaseEntry{
		K:    "1",
		HIdx: 0,
	}
	e2 := &StringEntry{
		BaseEntry: BaseEntry{
			K:    "2",
			HIdx: 0,
		},
		Val: "str",
	}
	e3 := &BaseEntry{
		K:    "3",
		HIdx: 0,
	}

	h := newHeap(10)
	h.Push(e1, time.Unix(6, 0))
	h.Push(e2, time.Unix(2, 0))
	h.Push(e3, time.Unix(3, 0))
	h.Update(e2, time.Unix(4, 0))

	assert.Equal(t, "3", h.Pop().Ref.Key())
	assert.Equal(t, "2", h.Pop().Ref.Key())
	assert.Equal(t, "1", h.Pop().Ref.Key())
}
