package server

import (
	"fmt"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"log"
	"net"
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

	l, err := net.Listen("tcp", fmt.Sprintf("%s:%s", vp.GetString(keyHost), vp.GetString(keyPort)))
	if err != nil {
		return err
	}
	defer l.Close()

	for {
		conn, err := l.Accept()
		if err != nil {
			return err
		}

		doSomething(conn)
		conn.Close()
	}
}

func doSomething(conn net.Conn) {
	buf := make([]byte, 4096)

	_, err := conn.Read(buf)
	if err != nil {
		return
	}
	log.Printf("recv: %s\n", string(buf))

	conn.Write([]byte("hello client!"))
}
