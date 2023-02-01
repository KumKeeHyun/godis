package server

import (
	"context"
	"errors"
	"fmt"
	"github.com/KumKeeHyun/godis/pkg/command"
	"github.com/KumKeeHyun/godis/pkg/storage"
	"io"
	"log"
	"net"
)

type Server struct {
	host string
	port string

	storage storage.Storage

	ctx context.Context
}

func New(host, port string) *Server {
	s := &Server{
		host: host,
		port: port,
	}
	s.storage = storage.New()
	return s
}

func (s *Server) Run() error {
	if s.ctx == nil {
		s.ctx = context.Background()
	}

	addr := fmt.Sprintf("%s:%s", s.host, s.port)
	l, err := net.Listen("tcp", addr)
	if err != nil {
		return err
	}
	defer l.Close()

	for {
		conn, err := l.Accept()
		if err != nil {
			return err
		}

		go s.handle(conn)
	}
}

func (s *Server) handle(conn net.Conn) {
	defer conn.Close()

	raddr := conn.RemoteAddr().String()
	for {
		select {
		case <-s.ctx.Done():
			break
		default:
		}

		req := new(command.Request)
		_, err := req.ReadFrom(conn)
		if err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			log.Printf("failed to read request from %s: %v\n", raddr, err)
			return
		}
		log.Printf("%s: %v\n", raddr, req)

		if _, err := command.Parse(req).Run(s.storage).WriteTo(conn); err != nil {
			log.Printf("failed to write to %s: %v\n", raddr, err)
			return
		}
	}
}
