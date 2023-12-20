package store

func entryOf(t string) Entry {
	switch t {
	case "String":
		return &StringEntry{}
	default:
		return nil
	}
}

type Entry interface {
	Type() string
	Key() string
	heapIdx() int
	setHeapIdx(int)
}

type BaseEntry struct {
	K    string `json:"key"`
	HIdx int    `json:"heapIdx"`
}

func (e *BaseEntry) Type() string {
	return "Base"
}

func (e *BaseEntry) Key() string {
	return e.K
}

func (e *BaseEntry) heapIdx() int {
	return e.HIdx
}

func (e *BaseEntry) setHeapIdx(i int) {
	e.HIdx = i
}

type StringEntry struct {
	BaseEntry
	Val string `json:"val"`
}

func NewStrEntry(key, val string) *StringEntry {
	return &StringEntry{
		BaseEntry: BaseEntry{
			K:    key,
			HIdx: -1,
		},
		Val: val,
	}
}

func (e *StringEntry) Type() string {
	return "String"
}

type SetEntry struct {
	BaseEntry
	Val map[string]struct{} `json:"val"`
}

func NewSetEntry(key string) *SetEntry {
	return &SetEntry{
		BaseEntry: BaseEntry{
			K:    key,
			HIdx: -1,
		},
		Val: make(map[string]struct{}),
	}
}

func (e SetEntry) Type() string {
	return "Set"
}
