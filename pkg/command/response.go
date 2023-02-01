package command

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"net/textproto"
)

type RespCode uint32

const (
	RES_OK RespCode = iota
	RES_ERR
	RES_NOT_EXISTS
)

// Response ...
//
// +-----+---------+
// | res | data... |
// +-----+---------+
type Response struct {
	resCode RespCode
	data    []byte
}

func NewResponse(resCode RespCode, data []byte) *Response {
	return &Response{
		resCode: resCode,
		data:    data,
	}
}

func (resp *Response) String() string {
	return fmt.Sprintf("%d %s", resp.resCode, string(resp.data))
}

func (resp *Response) ReadFrom(r io.Reader) (n int64, err error) {
	var buf []byte

	buf, err = textproto.NewReader(bufio.NewReader(r)).ReadLineBytes()
	if err != nil {
		return
	}

	r = bytes.NewReader(buf)
	if err = binary.Read(r, binary.LittleEndian, &resp.resCode); err != nil {
		return
	}
	n += 4

	resp.data, err = io.ReadAll(r)
	if err != nil {
		return
	}
	n += int64(len(resp.data))

	return
}

func (resp *Response) WriteTo(w io.Writer) (n int64, err error) {
	var wn int

	if err = binary.Write(w, binary.LittleEndian, resp.resCode); err != nil {
		return
	}
	n += 4

	wn, err = w.Write(resp.data)
	if err != nil {
		return
	}
	n += int64(wn)

	wn, err = w.Write([]byte("\r\n"))
	if err != nil {
		return
	}
	n += int64(wn)

	return
}
