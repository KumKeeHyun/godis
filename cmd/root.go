package cmd

import (
	"github.com/KumKeeHyun/godis/cmd/client"
	"github.com/KumKeeHyun/godis/cmd/cluster"
	"github.com/KumKeeHyun/godis/cmd/server"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

func New() *cobra.Command {
	rootCmd := &cobra.Command{
		Use:   "godis",
		Short: "godis is implement 'Build Your Own Redis with C/C++' in Go",
	}

	rootCmd.AddCommand(
		cluster.New(newClusterViper()),
		server.New(newServerViper()),
		client.New(newClientViper()),
	)
	return rootCmd
}

func newClusterViper() *viper.Viper {
	vp := viper.New()
	vp.SetEnvPrefix("godis_cluster")
	vp.AutomaticEnv()
	return vp
}

func newServerViper() *viper.Viper {
	vp := viper.New()
	vp.SetEnvPrefix("godis_server")
	vp.AutomaticEnv()
	return vp
}

func newClientViper() *viper.Viper {
	vp := viper.New()
	vp.SetEnvPrefix("godis_client")
	vp.AutomaticEnv()
	return vp
}
