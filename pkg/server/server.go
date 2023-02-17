package server

import (
	"context"
	"errors"
	"fmt"
	"github.com/KumKeeHyun/godis/pkg/apply"
	"github.com/KumKeeHyun/godis/pkg/command"
	resp "github.com/KumKeeHyun/godis/pkg/resp/v2"
	"github.com/KumKeeHyun/godis/pkg/store"
	"io"
	"log"
	"net"
	"os"
)

type Server struct {
	host string
	port string

	store   *store.Store
	applier apply.Applier

	ctx context.Context
}

func New(host, port string) *Server {
	s := &Server{
		host: host,
		port: port,
	}
	return s
}

func (s *Server) Run() error {
	if s.ctx == nil {
		s.ctx = context.Background()
	}

	s.store = store.New(s.ctx)
	s.applier = apply.NewApplier(s.store)

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
	//p := resp.NewParser(conn)
	p := resp.NewParser(io.TeeReader(conn, os.Stdout))
	w := resp.NewReplyWriter(conn)

	for {
		select {
		case <-s.ctx.Done():
			break
		default:
		}

		req, err := p.Parse()
		if err != nil {
			if errors.Is(err, io.EOF) {
				log.Printf("close %s\n", raddr)
				break
			}
			log.Printf("failed to read request from %s: %v\n", raddr, err)
			return
		}

		cmd := command.Parse(req)
		res := s.applier.Apply(s.ctx, cmd)
		if err := w.Write(res); err != nil {
			log.Printf("failed to write to %s: %v\n", raddr, err)
			return
		}
	}
}
