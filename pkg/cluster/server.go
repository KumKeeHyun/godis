package cluster

import (
	"context"
	"errors"
	"fmt"
	"github.com/KumKeeHyun/godis/pkg/apply"
	"github.com/KumKeeHyun/godis/pkg/command"
	resp "github.com/KumKeeHyun/godis/pkg/resp/v2"
	"github.com/KumKeeHyun/godis/pkg/store"
	"go.etcd.io/etcd/pkg/v3/idutil"
	"go.etcd.io/etcd/pkg/v3/wait"
	"go.etcd.io/etcd/raft/v3/raftpb"
	"io"
	"log"
	"net"
	"time"
)

type server struct {
	id   int
	host string
	port string

	store   *store.Store
	applier apply.Applier

	w     wait.Wait
	idGen *idutil.Generator

	ctx    context.Context
	cancel context.CancelFunc
}

func New(id int, host, port string) *server {
	return &server{
		id:   id,
		host: host,
		port: port,
	}
}

func (s *server) Start(ctx context.Context, peers []string, join bool, walDir string) {
	s.ctx, s.cancel = context.WithCancel(ctx)

	w := wait.New()
	idGen := idutil.NewGenerator(uint16(s.id), time.Now())
	st := store.New(s.ctx)

	proposeCh := make(chan []byte)
	confChangeCh := make(chan raftpb.ConfChange)
	commitCh, stopRaftNode := newRaftNode(s.ctx, s.id, peers, join, walDir, proposeCh, confChangeCh)

	s.applier = newClusterApplier(st, proposeCh, confChangeCh, commitCh, w, idGen)
	s.serveClient()

	stopRaftNode()
}

func (s *server) serveClient() {
	addr := fmt.Sprintf("%s:%s", s.host, s.port)
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		log.Fatal(err)
	}
	defer ln.Close()

	for {
		select {
		case <-s.ctx.Done():
			break
		default:
		}

		conn, err := ln.Accept()
		if err != nil {
			log.Println(err)
			break
		}

		go s.handleClientRequest(conn)
	}
}

func (s *server) handleClientRequest(conn net.Conn) {
	defer conn.Close()

	raddr := conn.RemoteAddr().String()
	p := resp.NewParser(conn)
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
