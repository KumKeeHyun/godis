package client

import (
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"log"
	"net"
)

const (
	keyServer = "server"
)

func New(vp *viper.Viper) *cobra.Command {
	cmd := &cobra.Command{
		Use: "client",
		RunE: func(_ *cobra.Command, _ []string) error {
			return runClient(vp)
		},
	}

	flags := cmd.Flags()
	flags.String(keyServer, "localhost:6379", "server addr to send request")
	vp.BindPFlags(flags)

	return cmd
}

func runClient(vp *viper.Viper) error {
	log.Println("start client")
	log.Printf("send request to %s\n", vp.GetString(keyServer))

	conn, err := net.Dial("tcp", vp.GetString(keyServer))
	if err != nil {
		return err
	}
	defer conn.Close()

	if _, err := conn.Write([]byte("hello server!")); err != nil {
		return err
	}

	buf := make([]byte, 4096)
	if _, err := conn.Read(buf); err != nil {
		return err
	}
	log.Printf("recv: %s\n", string(buf))
	return nil
}
