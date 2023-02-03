package sliceutil

type Iterator[T any] interface {
	HasNext() bool
	Next() T
}

type iterator[T any] struct {
	off   int
	slice []T
}

func NewIterator[T any](s []T) Iterator[T] {
	return &iterator[T]{
		off:   0,
		slice: s,
	}
}

func (iter *iterator[T]) HasNext() bool {
	return iter.off < len(iter.slice)
}

func (iter *iterator[T]) Next() T {
	var res T
	if iter.HasNext() {
		res = iter.slice[iter.off]
		iter.off++
	}
	return res
}
