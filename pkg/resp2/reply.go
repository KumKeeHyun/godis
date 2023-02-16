package resp2

import (
	"fmt"
	"strconv"
	"strings"
)

type ReplyType string

const (
	EOF     = "EOF"
	ILLEGAL = "ILLEGAL"
	LITERAL = "LITERAL"

	SIMPLE_STRING = "+"
	ERROR         = "-"
	INTEGER       = ":"
	BULK_STRING   = "$"
	ARRAY         = "*"
)

type Token struct {
	Type    ReplyType
	Literal string
}

type Reply interface {
	Type() ReplyType
	String() string
}

type StringReply interface {
	Get() string
}

type SimpleStringReply struct {
	Value string
}

var OKReply *SimpleStringReply = &SimpleStringReply{Value: "OK"}

func (r *SimpleStringReply) Type() ReplyType {
	return SIMPLE_STRING
}

func (r *SimpleStringReply) String() string {
	return fmt.Sprintf("\"%s\"", r.Value)
}

func (r *SimpleStringReply) Get() string {
	return r.Value
}

type ErrorReply struct {
	Value string
}

func NewErrorReply(err error) *ErrorReply {
	return &ErrorReply{Value: err.Error()}
}

func (r *ErrorReply) Type() ReplyType {
	return ERROR
}

func (r *ErrorReply) String() string {
	return fmt.Sprintf("(error) %s", r.Value)
}

type IntegerReply struct {
	Value int
}

func (r *IntegerReply) Type() ReplyType {
	return INTEGER
}

func (r *IntegerReply) String() string {
	return strconv.Itoa(r.Value)
}

type BulkStringReply struct {
	Len   int
	Value string
}

func NewBulkStringReply(val string) *BulkStringReply {
	return &BulkStringReply{
		Len:   len(val),
		Value: val,
	}
}

func NewNilReply() *BulkStringReply {
	return &BulkStringReply{Len: -1}
}

func (r *BulkStringReply) Type() ReplyType {
	return BULK_STRING
}

func (r *BulkStringReply) String() string {
	if r.IsNil() {
		return "(nil)"
	}
	return fmt.Sprintf("\"%s\"", r.Value)
}

func (r *BulkStringReply) Get() string {
	if r.IsNil() {
		return ""
	}
	return r.Value
}

func (r *BulkStringReply) IsNil() bool {
	return r.Len == -1
}

type ArrayReply struct {
	Len   int
	Value []Reply
}

func (r *ArrayReply) Type() ReplyType {
	return ARRAY
}

func (r *ArrayReply) String() string {
	if r.IsNil() {
		return "(nil)"
	}

	b := strings.Builder{}
	for i, r := range r.Value {
		b.WriteString(fmt.Sprintf("%d) %s\n", i+1, r.String()))
	}
	return b.String()
}

func (r *ArrayReply) IsNil() bool {
	return r.Len == -1
}
