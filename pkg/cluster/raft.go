package cluster

import (
	"context"
	"go.etcd.io/etcd/client/pkg/v3/types"
	"go.etcd.io/etcd/raft/v3"
	"go.etcd.io/etcd/raft/v3/raftpb"
	"go.etcd.io/etcd/server/v3/etcdserver/api/rafthttp"
	"go.etcd.io/etcd/server/v3/etcdserver/api/snap"
	stats "go.etcd.io/etcd/server/v3/etcdserver/api/v2stats"
	"go.etcd.io/etcd/server/v3/wal"
	"go.etcd.io/etcd/server/v3/wal/walpb"
	"go.uber.org/zap"
	"log"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"
)

type commit struct {
	data       [][]byte
	applyDoneC chan<- struct{}
}

type raftNode struct {
	id          int
	peerAddr    string
	peers       *sync.Map
	join        bool
	walDir      string
	snapDir     string
	getSnapshot func() ([]byte, error)

	proposeCh    <-chan []byte
	confChangeCh <-chan raftpb.ConfChange
	commitCh     chan<- *commit

	confState     raftpb.ConfState
	appliedIndex  uint64
	snapshotIndex uint64

	node        raft.Node
	raftStorage *raft.MemoryStorage
	wal         *wal.WAL
	transport   *rafthttp.Transport

	snapCount     uint64
	snapshotter   *snap.Snapshotter
	snapshotReady chan *snap.Snapshotter

	ctx    context.Context
	cancel context.CancelFunc
	wg     *sync.WaitGroup
}

var defaultSnapshotCount uint64 = 5
var defaultSnapshotCatchUpEntries uint64 = 1

//var defaultSnapshotCount uint64 = 10_000
//var defaultSnapshotCatchUpEntries uint64 = 10_000

func newRaftNode(
	ctx context.Context,
	id int,
	peerURL string,
	initialCluster []string,
	discovery []string,
	join bool,
	walDir string,
	snapDir string,
	getSnapshot func() ([]byte, error),
	proposeCh <-chan []byte,
	confChangeCh <-chan raftpb.ConfChange,
) (<-chan *commit, <-chan *snap.Snapshotter, func()) {
	commitCh := make(chan *commit)
	snapshotReady := make(chan *snap.Snapshotter)

	URL, err := url.Parse(peerURL)
	if err != nil {
		log.Fatal(err)
	}

	rn := &raftNode{
		id:          id,
		peerAddr:    URL.Host,
		peers:       &sync.Map{},
		join:        join,
		walDir:      walDir,
		snapDir:     snapDir,
		getSnapshot: getSnapshot,

		proposeCh:    proposeCh,
		confChangeCh: confChangeCh,
		commitCh:     commitCh,

		snapCount:     defaultSnapshotCount,
		snapshotReady: snapshotReady,

		wg: &sync.WaitGroup{},
	}
	rn.ctx, rn.cancel = context.WithCancel(ctx)

	go rn.start(initialCluster, discovery)
	return commitCh, snapshotReady, rn.stop
}

func (rn *raftNode) start(initialCluster, discovery []string) {
	if _, err := os.Stat(rn.snapDir); os.IsNotExist(err) {
		if err := os.MkdirAll(rn.snapDir, 0750); err != nil {
			log.Fatalf("cannot create dir for snapshot (%v)", err)
		}
	}
	rn.snapshotter = snap.New(zap.NewExample(), rn.snapDir)

	walExists := wal.Exist(rn.walDir)
	rn.wal = rn.openWAL()
	rn.raftStorage = raft.NewMemoryStorage()
	if walExists {
		rn.replayWAL()
	}

	rn.snapshotReady <- rn.snapshotter

	cfg := &raft.Config{
		ID:                        uint64(rn.id),
		ElectionTick:              10,
		HeartbeatTick:             1,
		Storage:                   rn.raftStorage,
		MaxSizePerMsg:             1024 * 1024,
		MaxInflightMsgs:           256,
		MaxUncommittedEntriesSize: 1 << 30,
	}

	if walExists || rn.join {
		// join
		rn.discoverCluster(discovery)

		rn.node = raft.RestartNode(cfg)
	} else {
		// init cluster
		for _, peer := range initialCluster {
			idAndURL := strings.Split(peer, "@")
			pid, err := strconv.Atoi(idAndURL[0])
			if err != nil {
				log.Fatalf("invalid peer format %v", peer)
			}
			rn.peers.Store(pid, idAndURL[1])
		}

		rpeers := make([]raft.Peer, 0)
		rn.peers.Range(func(pid, _ any) bool {
			rpeers = append(rpeers, raft.Peer{ID: uint64(pid.(int))})
			return true
		})
		log.Printf("start with peers %v", rpeers)

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
	rn.peers.Range(func(pid, purl any) bool {
		if pid.(int) != rn.id {
			rn.transport.AddPeer(types.ID(pid.(int)), []string{purl.(string)})
		}
		return true
	})

	rn.wg.Add(2)
	go rn.serveRaftHttp()
	go rn.serveChannels()
}

func (rn *raftNode) openWAL() *wal.WAL {
	if !wal.Exist(rn.walDir) {
		if err := os.MkdirAll(rn.walDir, 0750); err != nil {
			log.Fatalf("cannot create dir for wal: %v", err)
		}
		w, err := wal.Create(zap.NewExample(), rn.walDir, nil)
		if err != nil {
			log.Fatalf("cannot create wal: %v", err)
		}
		return w
	}

	w, err := wal.Open(zap.NewExample(), rn.walDir, walpb.Snapshot{})
	if err != nil {
		log.Fatalf("failed to open wal: %v", err)
	}
	return w
}

func (rn *raftNode) replayWAL() {
	_, state, ents, err := rn.wal.ReadAll()
	if err != nil {
		log.Fatalf("failed to read wal: %v", err)
	}

	rn.raftStorage.SetHardState(state)
	rn.raftStorage.Append(ents)
}

func (rn *raftNode) serveRaftHttp() {
	defer rn.wg.Done()

	ln, err := newKeepAliveListener(rn.ctx, rn.peerAddr)
	if err != nil {
		log.Fatalf("raftexample: Failed to listen rafthttp (%v)", err)
	}

	mux := rn.transport.Handler().(*http.ServeMux)
	mux.Handle(DiscoveryPrefix, rn.newDiscoveryHandler())
	err = (&http.Server{Handler: mux}).Serve(ln)
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
			if !raft.IsEmptySnap(rd.Snapshot) {
				rn.saveSnap(rd.Snapshot)
			}
			rn.wal.Save(rd.HardState, rd.Entries)

			if !raft.IsEmptySnap(rd.Snapshot) {
				rn.raftStorage.ApplySnapshot(rd.Snapshot)
				rn.publishSnapshot(rd.Snapshot)
			}
			rn.raftStorage.Append(rd.Entries)
			rn.transport.Send(rn.processMessage(rd.Messages))

			applyDoneCh, ok := rn.publishEntries(rn.entriesToApply(rd.CommittedEntries))
			if !ok {
				return
			}
			rn.maybeTriggerSnapshot(applyDoneCh)
			rn.node.Advance()

		case <-rn.transport.ErrorC:
			return
		case <-rn.ctx.Done():
			return
		}
	}
}

func (rn *raftNode) publishSnapshot(snapshot raftpb.Snapshot) {
	if raft.IsEmptySnap(snapshot) {
		return
	}

	log.Printf("publishing snapshot at index %d", rn.snapshotIndex)
	defer log.Printf("finished publishing snapshot at index %d", rn.snapshotIndex)

	if snapshot.Metadata.Index <= rn.appliedIndex {
		log.Fatalf("snapshot index [%d] should > progress.appliedIndex [%d]", snapshot.Metadata.Index, rn.appliedIndex)
	}
	rn.commitCh <- nil // trigger kvstore to load snapshot

	rn.confState = snapshot.Metadata.ConfState
	rn.snapshotIndex = snapshot.Metadata.Index
	rn.appliedIndex = snapshot.Metadata.Index
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
					log.Printf("transport add peer %d@%s", cc.NodeID, string(cc.Context))
					rn.transport.AddPeer(types.ID(cc.NodeID), []string{string(cc.Context)})
					rn.peers.Store(int(cc.NodeID), string(cc.Context))
				}
			case raftpb.ConfChangeRemoveNode:
				if cc.NodeID == uint64(rn.id) {
					log.Println("I've been removed from the cluster! Shutting down.")
					return nil, false
				}
				log.Printf("transport remove peer %d@%s", cc.NodeID, string(cc.Context))
				rn.transport.RemovePeer(types.ID(cc.NodeID))
				rn.peers.Delete(int(cc.NodeID))
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

func (rn *raftNode) maybeTriggerSnapshot(applyDoneCh <-chan struct{}) {
	if rn.appliedIndex-rn.snapshotIndex <= rn.snapCount {
		return
	}

	if applyDoneCh != nil {
		select {
		case <-applyDoneCh:
		case <-rn.ctx.Done():
			return
		}
	}

	log.Printf("start snapshot [applied index: %d | last snapshot index: %d]", rn.appliedIndex, rn.snapshotIndex)
	data, err := rn.getSnapshot()
	if err != nil {
		log.Panic(err)
	}
	snapshot, err := rn.raftStorage.CreateSnapshot(rn.appliedIndex, &rn.confState, data)
	if err != nil {
		log.Panic(err)
	}
	if err := rn.saveSnap(snapshot); err != nil {
		log.Panic(err)
	}

	// slow follower 를 위해 10K 정도는 남겨둠
	compactIndex := uint64(1)
	if rn.appliedIndex > defaultSnapshotCatchUpEntries {
		compactIndex = rn.appliedIndex - defaultSnapshotCatchUpEntries
	}
	if err := rn.raftStorage.Compact(compactIndex); err != nil {
		log.Panic(err)
	}

	log.Printf("compacted log at index %d", compactIndex)
	rn.snapshotIndex = rn.appliedIndex
}

func (rn *raftNode) saveSnap(snapshot raftpb.Snapshot) error {
	walSnap := walpb.Snapshot{
		Index:     snapshot.Metadata.Index,
		Term:      snapshot.Metadata.Term,
		ConfState: &snapshot.Metadata.ConfState,
	}
	if err := rn.snapshotter.SaveSnap(snapshot); err != nil {
		return err
	}
	if err := rn.wal.SaveSnapshot(walSnap); err != nil {
		return err
	}
	return rn.wal.ReleaseLockTo(snapshot.Metadata.Index)
}

func (rn *raftNode) stop() {
	rn.cancel()
	rn.wg.Wait()

	rn.transport.Stop()
	close(rn.commitCh)
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
