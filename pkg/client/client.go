package client

import (
	"bufio"
	"context"
	"fmt"
	"github.com/KumKeeHyun/godis/pkg/command"
	"net"
	"os"
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

	reader := bufio.NewReader(conn)
	writer := bufio.NewWriter(conn)

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

		cmd := command.NewRequest(text)
		if _, err := cmd.WriteTo(writer); err != nil {
			return err
		}
		if err := writer.Flush(); err != nil {
			return err
		}

		resp := new(command.Response)
		if _, err := resp.ReadFrom(reader); err != nil {
			return err
		}
		fmt.Println(resp)
	}

	return scanner.Err()
}
