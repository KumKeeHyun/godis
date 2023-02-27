package cluster

import (
	"context"
	"github.com/KumKeeHyun/godis/pkg/cluster"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"log"
)

const (
	keyID      = "id"
	keyHost    = "host"
	keyPort    = "port"
	keyCluster = "cluster"
	keyJoin    = "join"
	keyWalDir  = "waldir"
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
	flags.String(keyHost, "0.0.0.0", "addr to listen request")
	flags.String(keyPort, "6379", "port to listen request")
	flags.StringSlice(keyCluster, []string{"http://127.0.0.1:6300"}, "peers")
	flags.Bool(keyJoin, false, "join")
	flags.String(keyWalDir, "", "location of wal")
	vp.BindPFlags(flags)

	return cmd
}

func runServer(vp *viper.Viper) error {
	log.Printf("start server in id(%d) host(%s:%s) cluster(%v) join(%v)\n", vp.GetInt(keyID), vp.GetString(keyHost), vp.GetString(keyPort), vp.GetStringSlice(keyCluster), vp.GetBool(keyJoin))

	s := cluster.New(vp.GetInt(keyID), vp.GetString(keyHost), vp.GetString(keyPort))
	s.Start(context.Background(), vp.GetStringSlice(keyCluster), vp.GetBool(keyJoin), vp.GetString(keyWalDir))

	return nil
}
