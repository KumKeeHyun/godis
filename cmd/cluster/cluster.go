package cluster

import (
	"context"
	"github.com/KumKeeHyun/godis/pkg/cluster"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"log"
)

const (
	keyID             = "id"
	keyListenClient   = "listen-client"
	keyListenPeer     = "listen-peer"
	keyInitialCluster = "initial-cluster"
	keyDiscovery      = "discovery"
	keyJoin           = "join"
	keyWalDir         = "waldir"
	keySnapDir        = "snapdir"
)

func New(vp *viper.Viper) *cobra.Command {
	cmd := &cobra.Command{
		Use: "cluster",
		RunE: func(_ *cobra.Command, _ []string) error {
			return runServer(vp)
		},
	}

	flags := cmd.Flags()
	flags.Int(keyID, 1, "node id")
	flags.String(keyListenClient, "http://0.0.0.0:6379", "url for listening client request")
	flags.String(keyListenPeer, "http://0.0.0.0:6300", "url for listening peer request")
	flags.StringSlice(keyInitialCluster, []string{"1@http://127.0.0.1:6300"}, "(id,url) pairs seperated by '@' that initialized cluster")
	flags.StringSlice(keyDiscovery, []string{}, "subset of cluster for discover peers")
	flags.Bool(keyJoin, false, "join")
	flags.String(keyWalDir, "", "location of wal")
	flags.String(keySnapDir, "", "location of snapshot")

	vp.BindPFlags(flags)

	return cmd
}

func runServer(vp *viper.Viper) error {
	log.Printf(
		"start server in id(%d) listenClient(%s) listenPeer(%s) initialCluster(%v) discovery(%v) join(%v)\n",
		vp.GetInt(keyID),
		vp.GetString(keyListenClient),
		vp.GetString(keyListenPeer),
		vp.GetStringSlice(keyInitialCluster),
		vp.GetStringSlice(keyDiscovery),
		vp.GetBool(keyJoin),
	)

	s := cluster.New(vp.GetInt(keyID), vp.GetString(keyListenClient))
	s.Start(
		context.Background(),
		vp.GetString(keyListenPeer),
		vp.GetStringSlice(keyInitialCluster),
		vp.GetStringSlice(keyDiscovery),
		vp.GetBool(keyJoin),
		vp.GetString(keyWalDir),
		vp.GetString(keySnapDir),
	)

	return nil
}
