package cluster

import (
	"context"
	"errors"
	"net"
	"time"
)

type keepAliveListener struct {
	ctx context.Context
	*net.TCPListener
}

func newKeepAliveListener(ctx context.Context, addr string) (*keepAliveListener, error) {
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return nil, err
	}
	return &keepAliveListener{ctx, ln.(*net.TCPListener)}, nil
}

func (ln keepAliveListener) Accept() (c net.Conn, err error) {
	connCh := make(chan *net.TCPConn, 1)
	errCh := make(chan error, 1)
	go func() {
		tc, err := ln.AcceptTCP()
		if err != nil {
			errCh <- err
			return
		}
		connCh <- tc
	}()
	select {
	case <-ln.ctx.Done():
		return nil, errors.New("server stopped")
	case err := <-errCh:
		return nil, err
	case conn := <-connCh:
		conn.SetKeepAlive(true)
		conn.SetKeepAlivePeriod(3 * time.Minute)
		return conn, nil
	}
}
