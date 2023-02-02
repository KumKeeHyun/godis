package client

import (
	"bufio"
	"context"
	"fmt"
	resp "github.com/KumKeeHyun/godis/pkg/resp2"
	"net"
	"os"
	"strings"
)

type Client struct {
	host string
	port string

	ctx context.Context
}

func New(host, port string) *Client {
	return &Client{
		host: host,
		port: port,
	}
}

func (c *Client) Run() error {
	ctx := context.Background()
	if c.ctx != nil {
		ctx = c.ctx
	}

	addr := fmt.Sprintf("%s:%s", c.host, c.port)
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		return err
	}
	defer conn.Close()

	p := resp.NewParser(conn)
	w := resp.NewReplyWriter(conn)

	scanner := bufio.NewScanner(os.Stdin)
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		fmt.Printf("%s> ", addr)
		scanner.Scan()
		text := scanner.Text()
		if text == "exit" {
			break
		}

		if err := w.Write(newReply(text)); err != nil {
			return err
		}

		r, err := p.Parse()
		if err != nil {
			return err
		}
		fmt.Println(r)
	}

	return scanner.Err()
}

func newReply(text string) resp.Reply {
	s := strings.Split(text, " ")
	r := &resp.ArrayReply{
		Len:   len(s),
		Value: make([]resp.Reply, len(s)),
	}
	for i, ss := range s {
		r.Value[i] = &resp.SimpleStringReply{Value: ss}
	}

	return r
}
