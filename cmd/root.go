package cmd

import (
	"github.com/KumKeeHyun/godis/cmd/client"
	"github.com/KumKeeHyun/godis/cmd/server"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

func New() *cobra.Command {
	rootCmd := &cobra.Command{
		Use:   "godis",
		Short: "godis is implement 'Build Your Own Redis with C/C++' in Go",
	}
	vp := newViper()

	rootCmd.AddCommand(
		server.New(vp),
		client.New(vp),
	)
	return rootCmd
}

func newViper() *viper.Viper {
	vp := viper.New()
	vp.SetEnvPrefix("godis")
	vp.AutomaticEnv()
	return vp
}
