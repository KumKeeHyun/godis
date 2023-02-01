package server

import (
	"github.com/KumKeeHyun/godis/pkg/server"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"log"
)

const (
	keyHost = "host"
	keyPort = "port"
)

func New(vp *viper.Viper) *cobra.Command {
	cmd := &cobra.Command{
		Use: "server",
		RunE: func(_ *cobra.Command, _ []string) error {
			return runServer(vp)
		},
	}

	flags := cmd.Flags()
	flags.String(keyHost, "0.0.0.0", "addr to listen request")
	flags.String(keyPort, "6379", "port to listen request")
	vp.BindPFlags(flags)

	return cmd
}

func runServer(vp *viper.Viper) error {
	log.Printf("start server in %s:%s\n", vp.GetString(keyHost), vp.GetString(keyPort))

	return server.New(vp.GetString(keyHost), vp.GetString(keyPort)).Run()
}
