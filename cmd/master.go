package main

import (
	"Google_File_System/lib/master"
	"Google_File_System/lib/utils"
	"flag"
	"github.com/rs/zerolog/log"
	"os"
	"os/signal"
	"syscall"
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

	// Create and start RPC server
	server := utils.NewServer(masterService.Settings.Endpoint)

	err := server.Register(masterService)
	if err != nil {
		log.Fatal().Err(err).Send()
	}

	err = server.Start()
	if err != nil {
		log.Fatal().Err(err).Send()
	}

	// Wait for termination signal
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, syscall.SIGINT, syscall.SIGTERM)
	<-signals
	server.Stop()
}
