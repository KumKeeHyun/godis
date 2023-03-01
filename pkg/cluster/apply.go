package cluster

import (
	"bytes"
	"context"
	"encoding/gob"
	"errors"
	"github.com/KumKeeHyun/godis/pkg/apply"
	"github.com/KumKeeHyun/godis/pkg/command"
	resp "github.com/KumKeeHyun/godis/pkg/resp/v2"
	"github.com/KumKeeHyun/godis/pkg/store"
	"go.etcd.io/etcd/pkg/v3/idutil"
	"go.etcd.io/etcd/pkg/v3/wait"
	"go.etcd.io/etcd/raft/v3/raftpb"
	"go.etcd.io/etcd/server/v3/etcdserver/api/snap"
	"log"
	"time"
)

type raftRequest struct {
	ID   uint64
	Data []byte
}

type clusterApplier struct {
	store        *store.Store
	proposeCh    chan<- []byte
	confChangeCh chan<- raftpb.ConfChange
	commitCh     <-chan *commit
	snapshotter  *snap.Snapshotter

	applier apply.Applier

	w     wait.Wait
	idGen *idutil.Generator
}

func newClusterApplier(
	store *store.Store,
	proposeCh chan<- []byte,
	confChangeCh chan<- raftpb.ConfChange,
	commitCh <-chan *commit,
	snapshotter *snap.Snapshotter,
	w wait.Wait,
	idGen *idutil.Generator) apply.Applier {

	ca := &clusterApplier{
		store:        store,
		proposeCh:    proposeCh,
		confChangeCh: confChangeCh,
		commitCh:     commitCh,
		snapshotter:  snapshotter,
		applier:      apply.NewApplier(store),
		w:            w,
		idGen:        idGen,
	}
	go ca.applyCommits()
	return ca
}

func (a *clusterApplier) Apply(ctx context.Context, cmd command.Command) resp.Reply {
	switch c := cmd.(type) {
	case command.WriteCommand:
		res, err := a.processWriteCommand(ctx, c)
		if err != nil {
			return resp.NewErrorReply(err)
		}
		return res
	case command.ConfChangeCommand:
		res, err := a.processConfChangeCommand(ctx, c)
		if err != nil {
			return resp.NewErrorReply(err)
		}
		return res
	default:
		return a.applier.Apply(ctx, c)
	}
}

func (a *clusterApplier) processWriteCommand(ctx context.Context, cmd command.WriteCommand) (resp.Reply, error) {
	id := a.idGen.Next()
	d, _ := command.Marshal(cmd)

	req := raftRequest{
		ID:   id,
		Data: d,
	}
	var buf bytes.Buffer
	gob.NewEncoder(&buf).Encode(req)
	a.proposeCh <- buf.Bytes()

	ch := a.w.Register(id)
	cctx, cancel := context.WithTimeout(ctx, time.Second)
	defer cancel()

	select {
	case r := <-ch:
		if r == nil {
			return nil, errors.New("cancel propose")
		}
		return r.(resp.Reply), nil
	case <-cctx.Done():
		a.w.Trigger(id, nil)
		return nil, errors.New("propose canceled by context")
	}
}

func (a *clusterApplier) processConfChangeCommand(ctx context.Context, cmd command.ConfChangeCommand) (resp.Reply, error) {
	cc := cmd.ConfChange()
	a.confChangeCh <- cc
	return resp.OKReply, nil
}

func (a *clusterApplier) applyCommits() {
	for commit := range a.commitCh {
		if commit == nil {
			snapshot, err := a.loadSnapshot()
			if err != nil {
				log.Panic(err)
			}
			if snapshot != nil {
				log.Printf("loading snapshot at term %d and index %d and size %d", snapshot.Metadata.Term, snapshot.Metadata.Index, len(snapshot.Data))
				if err := a.store.RecoverFromSnapshot(snapshot.Data); err != nil {
					log.Panic(err)
				}
			}
			continue
		}
		for _, d := range commit.data {
			a.applyCommit(d)
		}
		close(commit.applyDoneC)
	}
}

func (a *clusterApplier) loadSnapshot() (*raftpb.Snapshot, error) {
	snapshot, err := a.snapshotter.Load()
	if err == snap.ErrNoSnapshot {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return snapshot, nil
}

func (a *clusterApplier) applyCommit(d []byte) {
	var req raftRequest
	err := gob.NewDecoder(bytes.NewBuffer(d)).Decode(&req)
	if err != nil {
		log.Printf("failed to decode raftRequest: %v\n", err)
		return
	}

	cmd, err := command.Unmarshal(req.Data)
	if err != nil {
		log.Printf("failed to decode command: %v\n", err)
		return
	}

	reply := a.applier.Apply(context.TODO(), cmd)
	if a.w.IsRegistered(req.ID) {
		a.w.Trigger(req.ID, reply)
	}
}
