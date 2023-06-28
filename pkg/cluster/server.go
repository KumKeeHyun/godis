package cluster

import (
	"context"
	"errors"
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
	"net/url"
	"time"
)

type server struct {
	id         int
	clientAddr string

	store   *store.Store
	applier apply.Applier

	w     wait.Wait
	idGen *idutil.Generator

	ctx    context.Context
	cancel context.CancelFunc
}

func New(id int, clientURL string) *server {
	URL, err := url.Parse(clientURL)
	if err != nil {
		log.Fatal(err)
	}
	return &server{
		id:         id,
		clientAddr: URL.Host,
	}
}

func (s *server) Start(ctx context.Context, peerURL string, initialCluster []string, join bool, walDir, snapDir string) {
	s.ctx, s.cancel = context.WithCancel(ctx)

	w := wait.New()
	idGen := idutil.NewGenerator(uint16(s.id), time.Now())
	st := store.New(s.ctx)

	proposeCh := make(chan []byte)
	confChangeCh := make(chan raftpb.ConfChange)
	commitCh, snapshotterCh, errorCh := newRaftNode(
		s.ctx,
		s.id,
		peerURL,
		initialCluster,
		join,
		walDir,
		snapDir,
		st.GetSnapshot,
		w,
		proposeCh,
		confChangeCh,
	)

	s.applier = newClusterApplier(
		st,
		proposeCh,
		confChangeCh,
		commitCh,
		<-snapshotterCh,
		w,
		idGen,
	)
	go s.serveClient()

	select {
	case <-s.ctx.Done():
		log.Println("cluster server canceled by root context")
		if err, ok := <-errorCh; ok {
			log.Printf("raft error: %v", err)
		}
	case err, ok := <-errorCh:
		if ok {
			log.Printf("raft error: %v", err)
		}
	}
	s.cancel()
}

func (s *server) serveClient() {
	ln, err := net.Listen("tcp", s.clientAddr)
	if err != nil {
		log.Fatal(err)
	}
	defer ln.Close()

	for {
		select {
		case <-s.ctx.Done():
			log.Println("stopped serve client")
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
				log.Printf("close handler %s\n", raddr)
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
