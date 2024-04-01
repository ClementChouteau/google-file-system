package utils

import (
	"fmt"
	"github.com/rs/zerolog/log"
	"net"
	"net/rpc"
	"sync"
)

type Server struct {
	endpoint Endpoint
	listener net.Listener
	wg       sync.WaitGroup
	done     chan struct{}
}

func NewServer(endpoint Endpoint) *Server {
	return &Server{
		endpoint: endpoint,
		done:     make(chan struct{}, 1),
	}
}

func (_ *Server) Register(receiver any) error {
	err := rpc.Register(receiver)
	if err != nil {
		return fmt.Errorf("registering RPC receiver: %w", err)
	}
	return nil
}

func (server *Server) Start() (err error) {
	server.listener, err = net.Listen("tcp", server.endpoint.Address())
	if err != nil {
		return fmt.Errorf("creating RPC server listener: %w", err)
	}

	log.Info().Msgf("RPC server ready listening on :%d", server.endpoint.Port)
	go func() {
		server.wg.Add(1)
		defer server.wg.Done()

		for {
			conn, err := server.listener.Accept()
			if err != nil {
				select {
				case <-server.done:
					return
				default:
					log.Error().Err(err).Msg("accepting connection")
				}
			}

			server.wg.Add(1)
			go func() {
				defer server.wg.Done()
				go rpc.ServeConn(conn)
			}()
		}
	}()

	return
}

func (server *Server) Stop() {
	defer close(server.done)
	server.done <- struct{}{}
	err := server.listener.Close()
	if err != nil {
		log.Error().Err(err).Msg("closing RPC server listener")
	}
	server.wg.Wait()
	log.Info().Msg("RPC server stopped")
	return
}
