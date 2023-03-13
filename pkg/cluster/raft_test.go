package cluster

import (
	"context"
	"fmt"
	"go.etcd.io/etcd/pkg/v3/wait"
	"go.etcd.io/etcd/raft/v3/raftpb"
	"os"
	"testing"
)

func getSnapshotFn() (func() ([]byte, error), <-chan struct{}) {
	snapshotTriggeredC := make(chan struct{})
	return func() ([]byte, error) {
		snapshotTriggeredC <- struct{}{}
		return nil, nil
	}, snapshotTriggeredC
}

type cluster struct {
	ctx    context.Context
	cancel context.CancelFunc

	w wait.Wait

	peers           map[int]string
	commitCh        map[int]<-chan *commit
	errorCh         map[int]<-chan error
	proposeCh       map[int]chan []byte
	confChangeCh    map[int]chan raftpb.ConfChange
	snapTriggeredCh map[int]<-chan struct{}
}

func newCluster(num int) *cluster {
	c := &cluster{
		w: wait.New(),

		peers:           make(map[int]string),
		commitCh:        make(map[int]<-chan *commit),
		errorCh:         make(map[int]<-chan error),
		proposeCh:       make(map[int]chan []byte),
		confChangeCh:    make(map[int]chan raftpb.ConfChange),
		snapTriggeredCh: make(map[int]<-chan struct{}),
	}
	c.ctx, c.cancel = context.WithCancel(context.Background())

	for i := 1; i <= num; i++ {
		c.peers[i] = fmt.Sprintf("http://127.0.0.1:%d", 10000+i)
	}
	var initialCluster []string
	for i, peer := range c.peers {
		initialCluster = append(initialCluster, fmt.Sprintf("%d@%s", i, peer))
	}

	for i, peer := range c.peers {
		os.RemoveAll(fmt.Sprintf("raft-test-%d", i))
		os.RemoveAll(fmt.Sprintf("raft-test-%d-snap", i))

		c.proposeCh[i] = make(chan []byte, 1)
		c.confChangeCh[i] = make(chan raftpb.ConfChange, 1)
		getSnapshot, snapTriggeredCh := getSnapshotFn()
		c.snapTriggeredCh[i] = snapTriggeredCh

		c.commitCh[i], _, c.errorCh[i] = newRaftNode(
			c.ctx,
			i,
			peer,
			initialCluster,
			nil,
			false,
			fmt.Sprintf("raft-test-%d", i),
			fmt.Sprintf("raft-test-%d-snap", i),
			getSnapshot,
			c.w,
			c.proposeCh[i],
			c.confChangeCh[i],
		)
	}

	return c
}

func (c *cluster) Close() (err error) {
	c.cancel()
	for i := range c.peers {
		go func(i int) {
			for range c.commitCh[i] {
			}
		}(i)

		close(c.proposeCh[i])
		if rerr := <-c.errorCh[i]; rerr != nil {
			err = fmt.Errorf("%v, %d@%v", err, i, rerr)
		}

		os.RemoveAll(fmt.Sprintf("raft-test-%d", i))
		os.RemoveAll(fmt.Sprintf("raft-test-%d-snap", i))
	}
	return
}

func (c *cluster) closeNoErrors(t *testing.T) {
	if err := c.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestCloseProposerBeforeReplay(t *testing.T) {
	c := newCluster(1)
	// close before replay so raft never starts
	defer c.closeNoErrors(t)
}

func TestSnapshot(t *testing.T) {
	prevDefaultSnapshotCount := defaultSnapshotCount
	prevSnapshotCatchUpEntries := defaultSnapshotCatchUpEntries
	defaultSnapshotCount = 4 // config 3 + propose 1
	defaultSnapshotCatchUpEntries = 4
	defer func() {
		defaultSnapshotCount = prevDefaultSnapshotCount
		defaultSnapshotCatchUpEntries = prevSnapshotCatchUpEntries
	}()

	c := newCluster(3)
	defer c.closeNoErrors(t)

	go func() {
		c.proposeCh[1] <- []byte("foo")
	}()

	ch := <-c.commitCh[1]

	select {
	case <-c.snapTriggeredCh[1]:
		t.Fatalf("snapshot triggered before applying done")
	default:
	}
	close(ch.applyDoneC)
	<-c.snapTriggeredCh[1]
}
