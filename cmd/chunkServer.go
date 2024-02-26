package main

import (
	"Google_File_System/lib/chunkServer"
	"Google_File_System/lib/utils"
	"flag"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"os"
)

func main() {
	var port = flag.Int("port", 52685, "Server port")
	var host = flag.String("host", "localhost", "Server host")
	var masterPort = flag.Int("master-port", 52684, "Master server port")
	var masterHost = flag.String("master-host", "localhost", "Master server host")
	var folder = flag.String("folder", "data", "Path to the folder")
	var logLevel = flag.String("log", "INFO", "Log level among \"PANIC\", \"FATAL\", \"ERROR\", \"WARN\", \"INFO\", \"DEBUG\", \"TRACE\"")
	flag.Parse()

	// Parse and initialize log level
	level, err := zerolog.ParseLevel(*logLevel)
	if err != nil {
		log.Fatal().Err(err).Send()
	}
	zerolog.SetGlobalLevel(level)

	// Setup logger
	consoleWriter := zerolog.ConsoleWriter{Out: os.Stderr}
	logger := zerolog.New(consoleWriter).
		With().
		Timestamp()
	if level == zerolog.TraceLevel {
		logger = logger.Caller()
	}
	log.Logger = logger.Logger()

	// Start server
	settings := chunkServer.Settings{
		Endpoint: utils.Endpoint{
			Host: *host,
			Port: *port,
		},
		Master: utils.Endpoint{
			Host: *masterHost,
			Port: *masterPort,
		},
		Folder: *folder,
	}

	chunkService, err := chunkServer.NewChunkService(settings)
	if err != nil {
		log.Fatal().Err(err).Send()
	}
	chunkService.Start()
}
