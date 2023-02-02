package resp2

import (
	"bufio"
	"errors"
	"io"
	"net/textproto"
)

type ReplyWriter struct {
	w *textproto.Writer
}

func NewReplyWriter(w io.Writer) *ReplyWriter {
	return &ReplyWriter{
		w: textproto.NewWriter(bufio.NewWriter(w)),
	}
}

func (rw *ReplyWriter) Write(r Reply) error {
	switch v := r.(type) {
	case *SimpleStringReply:
		return rw.w.PrintfLine("+%s", v.Value)
	case *ErrorReply:
		return rw.w.PrintfLine("-%s", v.Value)
	case *IntegerReply:
		return rw.w.PrintfLine(":%d", v.Value)
	case *BulkStringReply:
		if err := rw.w.PrintfLine("$%d", v.Len); err != nil {
			return err
		}
		if !v.IsNil() {
			return rw.w.PrintfLine("%s", v.Value)
		}
	case *ArrayReply:
		if err := rw.w.PrintfLine("*%d", v.Len); err != nil {
			return err
		}
		if !v.IsNil() {
			for _, rr := range v.Value {
				if err := rw.Write(rr); err != nil {
					return err
				}
			}
		}
	default:
		return errors.New("unexpected reply type")
	}

	return rw.w.W.Flush()
}
