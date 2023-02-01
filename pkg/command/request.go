package command

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"errors"
	"io"
	"net/textproto"
	"strings"
)

type CmdType int

// Request ...
//
// +------+-----+------+-----+------+-----+-----+------+
// | nstr | len | str1 | len | str2 | ... | len | strn |
// +------+-----+------+-----+------+-----+-----+------+
type Request struct {
	data []string
}

func NewRequest(text string) *Request {
	return &Request{
		data: strings.Split(text, " "),
	}
}

func (req *Request) ReadFrom(r io.Reader) (n int64, err error) {
	var (
		buf        []byte
		nstr, slen uint32
	)

	buf, err = textproto.NewReader(bufio.NewReader(r)).ReadLineBytes()
	if err != nil {
		return
	}

	n = int64(len(buf))
	r = bytes.NewReader(buf)

	if err = binary.Read(r, binary.LittleEndian, &nstr); err != nil {
		return
	}
	n += 4
	strs := make([]string, 0, nstr)

	for i := 0; i < int(nstr); i++ {
		if err = binary.Read(r, binary.LittleEndian, &slen); err != nil {
			return
		}
		n += 4

		buf := make([]byte, slen)
		if _, err = io.ReadFull(r, buf); err != nil {
			if !errors.Is(err, io.EOF) {
				return
			}
		}
		n += int64(slen)
		strs = append(strs, string(buf))
	}

	req.data = strs
	return
}

func (req *Request) WriteTo(w io.Writer) (n int64, err error) {
	var wn int

	if err = binary.Write(w, binary.LittleEndian, uint32(len(req.data))); err != nil {
		return
	}
	n += 4

	for _, str := range req.data {
		if err = binary.Write(w, binary.LittleEndian, uint32(len(str))); err != nil {
			break
		}
		n += 4

		if wn, err = io.WriteString(w, str); err != nil {
			break
		}
		n += int64(wn)
	}

	wn, err = w.Write([]byte("\r\n"))
	if err != nil {
		return
	}
	n += int64(wn)

	return
}
