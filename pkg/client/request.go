package client

import (
	"errors"
	resp "github.com/KumKeeHyun/godis/pkg/resp/v2"
	"net"
)

type Response struct {
	reply resp.Reply
	err   error
}

func (res *Response) Result() interface{} {
	if res.Err() != nil {
		return nil
	}
	return res.reply.String()
}

func (res *Response) Err() error {
	if res.err != nil {
		return res.err
	}
	if res.reply.Type() == resp.ERROR {
		return errors.New(res.reply.String())
	}
	return nil
}

func SendRequest(conn net.Conn, reply resp.Reply) *Response {
	writer := resp.NewReplyWriter(conn)
	parser := resp.NewParser(conn)
	res := &Response{}

	if err := writer.Write(reply); err != nil {
		res.err = err
		return res
	}
	res.reply, res.err = parser.Parse()

	return res
}
