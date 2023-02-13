package store

import (
	"github.com/stretchr/testify/assert"
	"testing"
	"time"
)

func TestHeap(t *testing.T) {
	e1 := &BaseEntry{
		key:  "1",
		hidx: 0,
	}
	e2 := &StringEntry{
		BaseEntry: BaseEntry{
			key:  "2",
			hidx: 0,
		},
		Val: "str",
	}
	e3 := &BaseEntry{
		key:  "3",
		hidx: 0,
	}

	h := newHeap(10)
	h.Push(e1, time.Unix(6, 0))
	h.Push(e2, time.Unix(2, 0))
	h.Push(e3, time.Unix(3, 0))
	h.Update(e2, time.Unix(4, 0))

	assert.Equal(t, "3", h.Pop().ref.Key())
	assert.Equal(t, "2", h.Pop().ref.Key())
	assert.Equal(t, "1", h.Pop().ref.Key())
}
