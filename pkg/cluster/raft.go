package cluster

import (
	"context"
	"go.etcd.io/etcd/client/pkg/v3/types"
	"go.etcd.io/etcd/raft/v3"
	"go.etcd.io/etcd/raft/v3/raftpb"
	"go.etcd.io/etcd/server/v3/etcdserver/api/rafthttp"
	stats "go.etcd.io/etcd/server/v3/etcdserver/api/v2stats"
	"go.uber.org/zap"
	"log"
	"net/http"
	"net/url"
	"strconv"
	"sync"
	"time"
)

type commit struct {
	data       [][]byte
	applyDoneC chan<- struct{}
}

type raftNode struct {
	id    int
	peers []string
	join  bool

	proposeCh    <-chan []byte
	confChangeCh <-chan raftpb.ConfChange
	commitCh     chan<- *commit
	commitConfCh chan<- uint64

	confState    raftpb.ConfState
	appliedIndex uint64

	node        raft.Node
	raftStorage *raft.MemoryStorage

	transport *rafthttp.Transport

	ctx    context.Context
	cancel context.CancelFunc
	wg     *sync.WaitGroup
}

func newRaftNode(ctx context.Context, id int, peers []string, join bool, proposeCh <-chan []byte, confChangeCh <-chan raftpb.ConfChange) (<-chan *commit, <-chan uint64, func()) {
	commitCh := make(chan *commit)
	commitConfCh := make(chan uint64, 1)

	rn := &raftNode{
		id:    id,
		peers: peers,
		join:  join,

		proposeCh:    proposeCh,
		confChangeCh: confChangeCh,
		commitCh:     commitCh,
		commitConfCh: commitConfCh,

		wg: &sync.WaitGroup{},
	}
	rn.ctx, rn.cancel = context.WithCancel(ctx)
	rn.raftStorage = raft.NewMemoryStorage()

	go rn.start()
	return commitCh, commitConfCh, rn.stop
}

func (rn *raftNode) start() {
	rpeers := make([]raft.Peer, len(rn.peers))
	for i := range rpeers {
		rpeers[i] = raft.Peer{ID: uint64(i + 1)}
	}

	cfg := &raft.Config{
		ID:                        uint64(rn.id),
		ElectionTick:              10,
		HeartbeatTick:             1,
		Storage:                   rn.raftStorage,
		MaxSizePerMsg:             1024 * 1024,
		MaxInflightMsgs:           256,
		MaxUncommittedEntriesSize: 1 << 30,
	}

	if rn.join {
		rn.node = raft.RestartNode(cfg)
	} else {
		rn.node = raft.StartNode(cfg, rpeers)
	}

	rn.transport = &rafthttp.Transport{
		Logger:      nil,
		ID:          types.ID(rn.id),
		ClusterID:   0x1000,
		Raft:        rn,
		ServerStats: stats.NewServerStats("", ""),
		LeaderStats: stats.NewLeaderStats(zap.NewExample(), strconv.Itoa(rn.id)),
		ErrorC:      make(chan error),
	}
	rn.transport.Start()
	for i := range rn.peers {
		if i+1 != rn.id {
			rn.transport.AddPeer(types.ID(i+1), []string{rn.peers[i]})
		}
	}

	rn.wg.Add(2)
	go rn.serveRaftHttp()
	go rn.serveChannels()
}

func (rn *raftNode) serveRaftHttp() {
	defer rn.wg.Done()

	rnUrl, err := url.Parse(rn.peers[rn.id-1])
	if err != nil {
		log.Fatalf("raftexample: Failed parsing URL (%v)", err)
	}

	ln, err := newKeepAliveListener(rn.ctx, rnUrl.Host)
	if err != nil {
		log.Fatalf("raftexample: Failed to listen rafthttp (%v)", err)
	}

	err = (&http.Server{Handler: rn.transport.Handler()}).Serve(ln)
	log.Fatalf("failed to serve http: %v", err)
}

func (rn *raftNode) serveChannels() {
	defer rn.wg.Done()

	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()

	go func() {
		confChangeCnt := uint64(0)

		for rn.proposeCh != nil && rn.confChangeCh != nil {
			select {
			case prop, ok := <-rn.proposeCh:
				if !ok {
					rn.proposeCh = nil
				} else {
					rn.node.Propose(context.TODO(), prop)
				}
			case cc, ok := <-rn.confChangeCh:
				if !ok {
					rn.proposeCh = nil
				} else {
					confChangeCnt++
					cc.ID = confChangeCnt
					rn.node.ProposeConfChange(context.TODO(), cc)
				}
			}
		}
		rn.cancel()
	}()

	for {
		select {
		case <-ticker.C:
			rn.node.Tick()

		case rd := <-rn.node.Ready():
			rn.raftStorage.Append(rd.Entries)
			rn.transport.Send(rn.processMessage(rd.Messages))
			_, ok := rn.publishEntries(rn.entriesToApply(rd.CommittedEntries))
			if !ok {
				return
			}
			rn.node.Advance()

		case <-rn.transport.ErrorC:
			return
		case <-rn.ctx.Done():
			return
		}
	}

}

func (rn *raftNode) processMessage(msg []raftpb.Message) []raftpb.Message {
	for i := 0; i < len(msg); i++ {
		if msg[i].Type == raftpb.MsgSnap {
			msg[i].Snapshot.Metadata.ConfState = rn.confState
		}
	}
	return msg
}

func (rn *raftNode) publishEntries(ents []raftpb.Entry) (<-chan struct{}, bool) {
	if len(ents) == 0 {
		return nil, true
	}

	data := make([][]byte, 0, len(ents))
	for i := range ents {
		switch ents[i].Type {
		case raftpb.EntryNormal:
			if len(ents[i].Data) == 0 {
				// ignore empty messages
				break
			}
			data = append(data, ents[i].Data)
		case raftpb.EntryConfChange:
			var cc raftpb.ConfChange
			cc.Unmarshal(ents[i].Data)
			rn.confState = *rn.node.ApplyConfChange(cc)
			switch cc.Type {
			case raftpb.ConfChangeAddNode:
				if len(cc.Context) > 0 {
					rn.transport.AddPeer(types.ID(cc.NodeID), []string{string(cc.Context)})
				}
			case raftpb.ConfChangeRemoveNode:
				if cc.NodeID == uint64(rn.id) {
					log.Println("I've been removed from the cluster! Shutting down.")
					return nil, false
				}
				rn.transport.RemovePeer(types.ID(cc.NodeID))
			}
			if cc.ID != 0 {
				select {
				case rn.commitConfCh <- cc.ID:
				case <-rn.ctx.Done():
				}
			}
		}
	}

	var applyDoneCh chan struct{}

	if len(data) > 0 {
		applyDoneCh = make(chan struct{}, 1)
		select {
		case rn.commitCh <- &commit{data, applyDoneCh}:
		case <-rn.ctx.Done():
			return nil, false
		}
	}

	// after commit, update appliedIndex
	rn.appliedIndex = ents[len(ents)-1].Index

	return applyDoneCh, true
}

func (rn *raftNode) entriesToApply(ents []raftpb.Entry) (nents []raftpb.Entry) {
	if len(ents) == 0 {
		return ents
	}
	firstIdx := ents[0].Index
	if firstIdx > rn.appliedIndex+1 {
		log.Fatalf("first index of committed entry[%d] should <= progress.appliedIndex[%d]+1", firstIdx, rn.appliedIndex)
	}
	if rn.appliedIndex-firstIdx+1 < uint64(len(ents)) {
		nents = ents[rn.appliedIndex-firstIdx+1:]
	}
	return
}

func (rn *raftNode) stop() {
	rn.cancel()
	rn.wg.Wait()

	rn.transport.Stop()
	close(rn.commitCh)
	close(rn.commitConfCh)
	rn.node.Stop()
}

func (rn *raftNode) Process(ctx context.Context, msg raftpb.Message) error {
	return rn.node.Step(ctx, msg)
}

func (rn *raftNode) IsIDRemoved(id uint64) bool {
	return false
}

func (rn *raftNode) ReportUnreachable(id uint64) {
	rn.node.ReportUnreachable(id)
}

func (rn *raftNode) ReportSnapshot(id uint64, status raft.SnapshotStatus) {
	rn.node.ReportSnapshot(id, status)
}
