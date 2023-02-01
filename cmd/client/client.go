package client

import (
	"github.com/KumKeeHyun/godis/pkg/client"
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
		Use: "client",
		RunE: func(_ *cobra.Command, _ []string) error {
			return runClient(vp)
		},
	}

	flags := cmd.Flags()
	flags.String(keyHost, "127.0.0.1", "host to connect server")
	flags.String(keyPort, "6379", "port to connect server")
	vp.BindPFlags(flags)

	return cmd
}

func runClient(vp *viper.Viper) error {
	log.Println("start client")
	log.Printf("send request to %s:%s\n", vp.GetString(keyHost), vp.GetString(keyPort))

	return client.New(vp.GetString(keyHost), vp.GetString(keyPort)).Run()
}
