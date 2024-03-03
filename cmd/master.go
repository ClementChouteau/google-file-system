package main

import (
	"Google_File_System/lib/master"
	"Google_File_System/lib/utils"
	"flag"
	"github.com/rs/zerolog/log"
	"net"
	"net/rpc"
	"strconv"
)

var (
	port     = flag.Int("port", 52684, "Server port")
	host     = flag.String("host", "localhost", "Server host")
	folder   = flag.String("folder", "data", "Path to the folder")
	logLevel = flag.String("log", "INFO", "Log level among \"PANIC\", \"FATAL\", \"ERROR\", \"WARN\", \"INFO\", \"DEBUG\", \"TRACE\"")
)

func init() {
	flag.Parse()
	log.Logger = utils.InitializeLogger(*logLevel)
}

func main() {
	masterService := new(master.MasterService)
	masterService.Settings = master.Settings{
		Endpoint: utils.Endpoint{
			Host: *host,
			Port: *port,
		},
		Folder:                 *folder,
		DefaultReplicationGoal: 3,
	}
	masterService.ChunkLocationData.ChunkReplication.Replication = make(map[utils.ChunkId][]utils.ChunkServerId)
	masterService.Namespace = master.NewNamespace()

	err := rpc.Register(masterService)
	if err != nil {
		log.Fatal().Err(err).Msg("registering RPC masterService")
	}

	listener, err := net.Listen("tcp", ":"+strconv.Itoa(*port))
	if err != nil {
		log.Fatal().Err(err).Msg("starting RPC server")
	}

	log.Info().Msgf("master (RPC) server ready listening on :%d", *port)
	for {
		conn, err := listener.Accept()
		if err != nil {
			log.Error().Err(err).Msg("accepting connection")
		} else {
			go rpc.ServeConn(conn)
		}
	}
}
