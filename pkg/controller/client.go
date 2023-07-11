package controller

import (
	"fmt"
	"github.com/KumKeeHyun/godis/pkg/client"
	godisapis "github.com/KumKeeHyun/godis/pkg/controller/apis/godis/v1"
	resp "github.com/KumKeeHyun/godis/pkg/resp/v2"
	"net"
	"strconv"
)

type GodisClusterClient interface {
	Meet(godises []*godisapis.Godis, newID int) error
	Forget(godises []*godisapis.Godis, deletedID int) error
}

var _ GodisClusterClient = &godisClusterClient{}

func NewClient() GodisClusterClient {
	return &godisClusterClient{}
}

type godisClusterClient struct{}

func (c *godisClusterClient) Meet(godises []*godisapis.Godis, newID int) error {
	if len(godises) == 0 {
		return fmt.Errorf("")
	}
	namespace := godises[0].Namespace
	name, _, _ := splitGodisNameID(godises[0].Name)
	meet := meetReply(namespace, name, newID)
	return sendRequest(godises, meet)
}

func (c *godisClusterClient) Forget(godises []*godisapis.Godis, deletedID int) error {
	forget := forgetReply(deletedID)
	return sendRequest(godises, forget)
}

func meetReply(namespace, name string, newID int) resp.Reply {
	reply := &resp.ArrayReply{
		Len:   4,
		Value: make([]resp.Reply, 4),
	}
	reply.Value[0] = &resp.SimpleStringReply{Value: "cluster"}
	reply.Value[1] = &resp.SimpleStringReply{Value: "meet"}
	reply.Value[2] = &resp.SimpleStringReply{Value: strconv.Itoa(newID)}
	reply.Value[3] = &resp.SimpleStringReply{Value: godisPeerURL(namespace, godisName(name, newID))}
	return reply
}

func forgetReply(deletedID int) resp.Reply {
	reply := &resp.ArrayReply{
		Len:   3,
		Value: make([]resp.Reply, 3),
	}
	reply.Value[0] = &resp.SimpleStringReply{Value: "cluster"}
	reply.Value[1] = &resp.SimpleStringReply{Value: "forget"}
	reply.Value[2] = &resp.SimpleStringReply{Value: strconv.Itoa(deletedID)}
	return reply
}

func sendRequest(godises []*godisapis.Godis, reply resp.Reply) error {
	sendRequestTo := func(godis *godisapis.Godis) error {
		conn, err := net.Dial("tcp", fmt.Sprintf("%s:6379", serviceFQDN(godis.Namespace, godis.Name)))
		if err != nil {
			return err
		}
		defer conn.Close()

		return client.SendRequest(conn, reply).Err()
	}

	for _, godis := range godises {
		if err := sendRequestTo(godis); err == nil {
			return nil
		}
	}

	return fmt.Errorf("failed to send meet command")
}
