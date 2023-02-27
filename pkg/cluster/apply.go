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
	"log"
	"time"
)

type raftRequest struct {
	ID   uint64
	Data []byte
}

type clusterApplier struct {
	proposeCh    chan<- []byte
	confChangeCh chan<- raftpb.ConfChange
	commitCh     <-chan *commit

	applier apply.Applier

	w     wait.Wait
	idGen *idutil.Generator
}

func newClusterApplier(
	store *store.Store,
	proposeCh chan<- []byte,
	confChangeCh chan<- raftpb.ConfChange,
	commitCh <-chan *commit,
	w wait.Wait,
	idGen *idutil.Generator) apply.Applier {

	ca := &clusterApplier{
		proposeCh:    proposeCh,
		confChangeCh: confChangeCh,
		commitCh:     commitCh,
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
			// TODO: snapshot
			continue
		}
		for _, d := range commit.data {
			a.applyCommit(d)
		}
		close(commit.applyDoneC)
	}
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
