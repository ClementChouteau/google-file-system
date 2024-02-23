package main

import (
	"Google_File_System/chunkServer"
	"Google_File_System/utils/common"
	"Google_File_System/utils/rpcdefs"
	"encoding/binary"
	"errors"
	"flag"
	"fmt"
	"github.com/google/uuid"
	lru "github.com/hashicorp/golang-lru/v2"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"net"
	"net/rpc"
	"os"
	"strconv"
	"sync"
	"time"
)

type ChunkService struct {
	settings      chunkServer.Settings
	id            common.ChunkServerId
	chunks        sync.Map // common.ChunkId => *chunkServer.Chunk
	servers       sync.Map // common.ChunkServerId => common.ChunkServer
	temporaryData *lru.Cache[uuid.UUID, []byte]
}

func (chunkService *ChunkService) ensureFolders() (err error) {
	err = os.Mkdir(chunkService.settings.Folder, 0755)
	if err != nil && !os.IsExist(err) {
		return err
	}

	err = os.Mkdir(chunkService.settings.GetChunksFolder(), 0755)
	if err != nil && !os.IsExist(err) {
		return err
	}

	return
}

func (chunkService *ChunkService) readChunkMetadataFromDisk() (count int, err error) {
	chunksPath := chunkService.settings.GetChunksFolder()
	entries, err := os.ReadDir(chunksPath)
	if err != nil {
		return 0, errors.New("could not open \"chunks\" directory")
	}

	for _, file := range entries {
		id, err := strconv.ParseUint(file.Name(), 10, 32)
		if err != nil {
			return 0, err
		}

		chunkMetadata := &chunkServer.Chunk{
			Id: common.ChunkId(id),
		}
		chunkService.chunks.Store(common.ChunkId(id), chunkMetadata)
		count++
	}

	return
}

func (chunkService *ChunkService) readServerIdFromDisk() (common.ChunkServerId, error) {
	var id common.ChunkServerId

	file, err := os.Open(chunkService.settings.GetServerIdPath())
	if err != nil {
		return id, err
	}

	err = binary.Read(file, binary.BigEndian, &id)
	return id, err
}

func (chunkService *ChunkService) writeServerIdToDisk(id common.ChunkServerId) (err error) {
	var file *os.File
	file, err = os.Create(chunkService.settings.GetServerIdPath())
	if err != nil {
		return
	}

	if err = binary.Write(file, binary.BigEndian, id); err != nil {
		return
	}

	err = file.Sync()

	return
}

func (chunkService *ChunkService) askServerId() common.ChunkServerId {
	log.Debug().Msg("requesting chunk server id from master")

	client, err := rpc.Dial("tcp", chunkService.settings.Master.Address())
	if err != nil {
		log.Fatal().Err(err).Msg("connecting to RPC server")
	}
	defer client.Close()

	var reply common.ChunkServerId
	err = client.Call("MasterService.RegisterRPC", chunkService.settings.Endpoint, &reply)
	if err != nil {
		log.Fatal().Err(err).Msg("calling RegisterRPC")
	}
	return reply
}

func (chunkService *ChunkService) ensureServerId() common.ChunkServerId {
	id, err := chunkService.readServerIdFromDisk()

	if err != nil {
		if !os.IsNotExist(err) {
			log.Fatal().Err(err).Msg("could not read chunk server id")
		}

		id = chunkService.askServerId()
		log.Debug().Msgf("received new server id=%d from master", id)
		err := chunkService.writeServerIdToDisk(id)
		if err != nil {
			log.Fatal().Err(err).Msg("could not read chunk server id")
		}
	} else {
		log.Debug().Msgf("read server id=%d from disk", id)
	}

	chunkService.id = id
	return id
}

func (chunkService *ChunkService) sendHeartbeat() {
	chunks := make([]common.ChunkId, 0)
	chunkService.chunks.Range(func(key any, value any) bool {
		chunks = append(chunks, value.(*chunkServer.Chunk).Id)
		return true
	})

	log.Debug().Msgf("sending heartbeat with %d chunks", len(chunks))

	client, err := rpc.Dial("tcp", chunkService.settings.Master.Address())
	if err != nil {
		log.Error().Err(err).Msgf("connecting to RPC server: %v", err)
		return
	}
	defer client.Close()

	request := rpcdefs.HeartBeatArgs{
		Id:     chunkService.id,
		Chunks: chunks,
	}
	var reply struct{}
	err = client.Call("MasterService.HeartbeatRPC", request, &reply)
	if err != nil {
		log.Error().Err(err).Msgf("calling HeartbeatRPC: %v", err)
	}
}

func (chunkService *ChunkService) heartbeatTicker(wg *sync.WaitGroup) {
	defer wg.Done()

	log.Info().Msg("starting to send heartbeats")
	heartbeatTick := time.Tick(5 * time.Second)
	chunkService.sendHeartbeat()
	for range heartbeatTick {
		chunkService.sendHeartbeat()
	}
}

func (chunkService *ChunkService) startServer(wg *sync.WaitGroup) error {
	defer wg.Done()

	err := rpc.Register(chunkService)
	if err != nil {
		return fmt.Errorf("registering RPC chunkService: %w", err)
	}

	listener, err := net.Listen("tcp", chunkService.settings.Address())
	if err != nil {
		return fmt.Errorf("starting RPC server: %w", err)
	}

	log.Info().Msgf("chunk (RPC) server ready listening on :%d", chunkService.settings.Port)
	for {
		conn, err := listener.Accept()
		if err != nil {
			return fmt.Errorf("accepting connection: %w", err)
		}
		go rpc.ServeConn(conn)
	}
}

func (chunkService *ChunkService) start() {
	if err := chunkService.ensureFolders(); err != nil {
		log.Fatal().Err(err).Msg("calling ensureFolders")
	}

	chunksCount, err := chunkService.readChunkMetadataFromDisk()
	if err != nil {
		log.Fatal().Err(err).Send()
	}
	log.Info().Msgf("found %d chunk(s) on disk", chunksCount)

	serverId := chunkService.ensureServerId()
	log.Logger = log.Logger.With().Uint32("chunkServer", serverId).Logger()
	log.Info().Msg("chunk server ready")

	var wg sync.WaitGroup
	wg.Add(2)
	go chunkService.heartbeatTicker(&wg)
	go chunkService.startServer(&wg)

	wg.Wait()
}

func (chunkService *ChunkService) writeTemporary(id uuid.UUID, data []byte) {
	chunkService.temporaryData.Add(id, data)
}

func (chunkService *ChunkService) readTemporary(id uuid.UUID) (data []byte, exists bool) {
	value, exists := chunkService.temporaryData.Get(id)
	chunkService.temporaryData.Remove(id)
	return value, exists
}

// Ensure that the chunk exists, writes it
func (chunkService *ChunkService) ensureChunk(id common.ChunkId, create bool) (chunk *chunkServer.Chunk, err error) {
	newChunk := &chunkServer.Chunk{
		Id: id,
	}
	value, exists := chunkService.chunks.LoadOrStore(id, newChunk)

	if !create && !exists {
		err = errors.New("no corresponding chunk")
		return
	}

	// Ensure that the file exists
	chunk = value.(*chunkServer.Chunk)
	if !exists {
		err = chunk.Ensure(chunkService.settings.GetChunkPath(id))
		if err != nil {
			log.Error().Err(err).Msgf("could not create chunk %d", id)
			return
		}
		log.Debug().Msgf("created chunk %d", id)
	}
	return
}

func (chunkService *ChunkService) ReadRPC(request rpcdefs.ReadArgs, reply *rpcdefs.ReadReply) error {
	value, exists := chunkService.chunks.Load(request.Id)
	if !exists {
		// TODO return slice of 0 ?
		// TODO this chunks could be sparse
		return errors.New("no corresponding chunk")
	}

	data, err := value.(*chunkServer.Chunk).Read(chunkService.settings.GetChunkPath(request.Id), request.Offset, request.Length)
	if err != nil {
		log.Error().Err(err).Msgf("error when reading to chunk with id %d", request.Id)
		return err
	}
	reply.Data = data

	return nil
}

func (chunkService *ChunkService) GrantLeaseRPC(request rpcdefs.GrantLeaseArgs, _ *rpcdefs.GrantLeaseReply) error {
	log.Debug().Msgf("received lease for chunk %d", request.ChunkId)

	chunk, err := chunkService.ensureChunk(request.ChunkId, true)
	if err != nil {
		return err
	}

	chunk.LeaseMutex.Lock()
	chunk.GiveLeaseNow(request.Replication)
	chunk.LeaseMutex.Unlock()

	return nil
}

func (chunkService *ChunkService) RevokeLeaseRPC(request rpcdefs.RevokeLeaseArgs, _ *rpcdefs.RevokeLeaseReply) error {
	value, exists := chunkService.chunks.Load(request.ChunkId)
	if !exists {
		return errors.New("no corresponding chunk")
	}
	chunk := value.(*chunkServer.Chunk)

	chunk.LeaseMutex.Lock()
	chunk.RevokeLease()
	chunk.LeaseMutex.Unlock()

	return nil
}

func (chunkService *ChunkService) PushDataRPC(request rpcdefs.PushDataArgs, _ *rpcdefs.PushDataReply) (err error) {
	if len(request.Data) == 0 {
		return errors.New("empty Data")
	}

	// Write data in memory cache
	chunkService.writeTemporary(request.Id, request.Data)

	// Push to next server if we are not the last one
	var forwardReply rpcdefs.PushDataReply
	for i, server := range request.Servers {
		if server.Id != chunkService.id {
			continue
		}

		if i != len(request.Servers)-1 {
			err = request.Servers[i+1].Endpoint.Call("ChunkService.PushDataRPC", request, &forwardReply)
			if err != nil {
				return
			}
		}
		break
	}

	log.Debug().Msgf("wrote temporary file of length %d with id %s", len(request.Data), request.Id.String())

	return
}

func (chunkService *ChunkService) ApplyWriteRPC(request rpcdefs.ApplyWriteArgs, _ *rpcdefs.ApplyWriteReply) error {
	chunk, err := chunkService.ensureChunk(request.Id, true)
	if err != nil {
		log.Error().Err(err).Msg("calling ensureChunk")
		return err
	}

	data, exists := chunkService.readTemporary(request.DataId)
	if !exists {
		return errors.New("temporary data not found")
	}
	err = chunk.Write(chunkService.settings.GetChunkPath(request.Id), request.Offset, data)
	if err != nil {
		log.Error().Err(err).Msgf("error when writing to chunk with id %d", request.Id)
		return err
	}

	log.Debug().Msgf("wrote %d bytes to chunk %d at offset %d", len(data), request.Id, request.Offset)

	return nil
}

func (chunkService *ChunkService) WriteRPC(request rpcdefs.WriteArgs, reply *rpcdefs.WriteReply) error {
	chunk, err := chunkService.ensureChunk(request.Id, false)
	if err != nil {
		log.Error().Err(err).Msg("calling ensureChunk")
		return err
	}

	// Check lease
	chunk.LeaseMutex.RLock()
	defer chunk.LeaseMutex.RUnlock()
	if !chunk.HasLease() {
		log.Debug().Msgf("no lease for writing to chunk %d", request.Id)
		return &rpcdefs.NoLeaseError{Message: fmt.Sprintf("no lease for chunk with id %d", request.Id)}
	}

	// TODO push in queue to propagate to replicas

	// Write to all replicas
	for _, server := range chunk.Replication {
		if server.Id == chunkService.id {
			continue
		}

		writeReply := &rpcdefs.WriteReply{}
		err = server.Endpoint.Call("ChunkService.ApplyWriteRPC", request, writeReply)
		if err != nil {
			return err
		}
	}

	return chunkService.ApplyWriteRPC(request, reply)
}

func (chunkService *ChunkService) RecordAppendRPC(request rpcdefs.RecordAppendArgs, reply *rpcdefs.RecordAppendReply) error {
	chunk, err := chunkService.ensureChunk(request.Id, false) // Chunk is created when receiving lease
	if err != nil {
		log.Error().Err(err).Msg("calling ensureChunk")
		return err
	}

	chunk.LeaseMutex.RLock()
	defer chunk.LeaseMutex.RUnlock()
	if !chunk.HasLease() {
		log.Debug().Msgf("no lease for writing to chunk %d", request.Id)
		return &rpcdefs.NoLeaseError{Message: fmt.Sprintf("no lease for chunk with id %d", request.Id)}
	}

	data, exists := chunkService.readTemporary(request.DataId)
	if !exists {
		return errors.New("temporary data not found")
	}

	padding, offset, err := chunk.Append(chunkService.settings.GetChunkPath(request.Id), data)
	if err != nil {
		log.Error().Err(err).Msgf("error when appending to chunk with id %d", request.Id)
		return err
	}

	for _, server := range chunk.Replication {
		if server.Id == chunkService.id {
			continue
		}

		// Write to the replica
		writeRequest := rpcdefs.WriteArgs{
			Id:     request.Id,
			DataId: request.DataId,
			Offset: offset,
		}
		writeReply := &rpcdefs.WriteReply{}
		err := server.Endpoint.Call("ChunkService.ApplyWriteRPC", writeRequest, writeReply)
		if err != nil {
			return err
		}
	}

	log.Debug().Msgf("record append of %d bytes done for chunk %d at offset %d", len(data), request.Id, offset)

	*reply = rpcdefs.RecordAppendReply{
		Done: !padding,
		Pos:  offset,
	}

	return nil
}

func NewChunkService(settings chunkServer.Settings) (chunkService *ChunkService, err error) {
	chunkService = new(ChunkService)
	chunkService.settings = settings
	chunkService.temporaryData, err = lru.New[uuid.UUID, []byte](4096)

	return
}

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
		Endpoint: common.Endpoint{
			Host: *host,
			Port: *port,
		},
		Master: common.Endpoint{
			Host: *masterHost,
			Port: *masterPort,
		},
		Folder: *folder,
	}

	chunkService, err := NewChunkService(settings)
	if err != nil {
		log.Fatal().Err(err).Send()
	}
	chunkService.start()
}
