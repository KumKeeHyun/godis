package store

type Entry interface {
	Key() string
	heapIdx() int
	setHeapIdx(int)
}

type BaseEntry struct {
	key  string
	hidx int
}

func (e *BaseEntry) Key() string {
	return e.key
}

func (e *BaseEntry) heapIdx() int {
	return e.hidx
}

func (e *BaseEntry) setHeapIdx(i int) {
	e.hidx = i
}

type StringEntry struct {
	BaseEntry
	Val string
}

func NewStrEntry(key, val string) *StringEntry {
	return &StringEntry{
		BaseEntry: BaseEntry{
			key:  key,
			hidx: -1,
		},
		Val: val,
	}
}
