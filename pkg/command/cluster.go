package command

import (
	"errors"
	resp "github.com/KumKeeHyun/godis/pkg/resp/v2"
	"go.etcd.io/etcd/raft/v3/raftpb"
	"net/url"
)

var clusterParseFns = map[string]cmdParseFn{
	"forget": parseClusterForget,
	"meet":   parseClusterMeet,
}

var parseCluster cmdParseFn = func(replies []resp.Reply) Command {
	cmdName := mustString(replies[1])
	parse, exists := clusterParseFns[cmdName]
	if !exists {
		return &invalidCommand{errors.New("ERR unknown command")}
	}
	return parse(replies)
}

var parseClusterForget cmdParseFn = func(replies []resp.Reply) Command {
	return &ClusterForget{
		ID: mustInt(replies[2]),
	}
}

type ClusterForget struct {
	ID int
}

func (cmd *ClusterForget) Command() string {
	return "cluster forget"
}

func (cmd *ClusterForget) ConfChange() raftpb.ConfChange {
	return raftpb.ConfChange{
		Type:   raftpb.ConfChangeRemoveNode,
		NodeID: uint64(cmd.ID),
	}
}

var parseClusterMeet cmdParseFn = func(replies []resp.Reply) Command {
	host := mustString(replies[3])
	_, err := url.Parse(host)
	if err != nil {
		return &invalidCommand{errors.New("ERR invalid host argument")}
	}
	return &ClusterMeet{
		ID:   mustInt(replies[2]),
		Host: host,
	}
}

type ClusterMeet struct {
	ID   int
	Host string
}

func (cmd *ClusterMeet) Command() string {
	return "cluster meet"
}

func (cmd *ClusterMeet) ConfChange() raftpb.ConfChange {
	return raftpb.ConfChange{
		Type:    raftpb.ConfChangeAddNode,
		NodeID:  uint64(cmd.ID),
		Context: []byte(cmd.Host),
	}
}
