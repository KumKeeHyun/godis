package sliceutil

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func Test_iterator(t *testing.T) {
	s := []int{1, 2, 3, 4, 5}
	iter := NewIterator(s)

	for _, v := range s {
		assert.True(t, iter.HasNext())
		assert.Equal(t, v, iter.Next())
	}

	assert.False(t, iter.HasNext())
}
