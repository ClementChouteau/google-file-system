package chunkServer

import (
	"Google_File_System/lib/utils"
	"encoding/binary"
	"errors"
	"fmt"
	"github.com/google/uuid"
	lru "github.com/hashicorp/golang-lru/v2"
	"github.com/rs/zerolog/log"
	"net"
	"net/rpc"
	"os"
	"strconv"
	"sync"
	"time"
)

type Operation struct {
	chunk      *Chunk
	request    utils.WriteArgs
	completion chan error
}

type ChunkService struct {
	settings        Settings
	id              utils.ChunkServerId
	chunkOperations chan Operation
	chunks          sync.Map // common.ChunkId => *Chunk
	servers         sync.Map // common.ChunkServerId => common.ChunkServer
	temporaryData   *lru.Cache[uuid.UUID, []byte]
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

		chunk := &Chunk{
			Id: utils.ChunkId(id),
		}
		chunkService.chunks.Store(utils.ChunkId(id), chunk)
		count++
	}

	return
}

func (chunkService *ChunkService) readServerIdFromDisk() (utils.ChunkServerId, error) {
	var id utils.ChunkServerId

	file, err := os.Open(chunkService.settings.GetServerIdPath())
	if err != nil {
		return id, err
	}

	err = binary.Read(file, binary.BigEndian, &id)
	return id, err
}

func (chunkService *ChunkService) writeServerIdToDisk(id utils.ChunkServerId) (err error) {
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

func (chunkService *ChunkService) askServerId() utils.ChunkServerId {
	log.Debug().Msg("requesting chunk server id from master")

	client, err := rpc.Dial("tcp", chunkService.settings.Master.Address())
	if err != nil {
		log.Fatal().Err(err).Msg("connecting to RPC server")
	}
	defer client.Close()

	var reply utils.ChunkServerId
	err = client.Call("MasterService.RegisterRPC", chunkService.settings.Endpoint, &reply)
	if err != nil {
		log.Fatal().Err(err).Msg("calling RegisterRPC")
	}
	return reply
}

func (chunkService *ChunkService) ensureServerId() utils.ChunkServerId {
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
	chunks := make([]utils.ChunkId, 0)
	chunkService.chunks.Range(func(key any, value any) bool {
		chunks = append(chunks, value.(*Chunk).Id)
		return true
	})

	log.Debug().Msgf("sending heartbeat with %d chunks", len(chunks))

	client, err := rpc.Dial("tcp", chunkService.settings.Master.Address())
	if err != nil {
		log.Error().Err(err).Msgf("connecting to RPC server: %v", err)
		return
	}
	defer client.Close()

	request := utils.HeartBeatArgs{
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

// Thread to send writes (in order) to replicas, assuming the operation is already applied on primary
func (chunkService *ChunkService) startOperations(wg *sync.WaitGroup) {
	defer wg.Done()

nextOperation:
	for {
		operation := <-chunkService.chunkOperations

		// Apply operation on all replicas
		for _, server := range operation.chunk.Replication {
			if server.Id == chunkService.id {
				continue
			}

			writeReply := &utils.WriteReply{}
			err := server.Endpoint.Call("ChunkService.ApplyWriteRPC", operation.request, writeReply)
			if err != nil {
				operation.completion <- err
				continue nextOperation
			}
		}

		operation.completion <- nil
	}
}

func (chunkService *ChunkService) Start() {
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
	wg.Add(3)
	go chunkService.heartbeatTicker(&wg)
	go chunkService.startServer(&wg)
	go chunkService.startOperations(&wg)

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

// ensureChunk ensures that the chunk exists, writes it
func (chunkService *ChunkService) ensureChunk(id utils.ChunkId, create bool) (chunk *Chunk, err error) {
	newChunk := &Chunk{
		Id: id,
	}
	value, exists := chunkService.chunks.LoadOrStore(id, newChunk)

	if !create && !exists {
		err = errors.New("no corresponding chunk")
		log.Error().Err(err).Uint32("chunk", id)
		return
	}

	// Ensure that the file exists
	chunk = value.(*Chunk)
	if !exists {
		chunk.Lock.Lock()
		err = chunk.Ensure(chunkService.settings.GetChunkPath(id))
		chunk.Lock.Unlock()
		if err != nil {
			log.Error().Err(err).Msgf("could not create chunk %d", id)
			return
		}
		log.Debug().Msgf("created chunk %d", id)
	}
	return
}

// getChunk accesses the chunk, returns an error if it does not exist
func (chunkService *ChunkService) getChunk(id utils.ChunkId) (chunk *Chunk, err error) {
	return chunkService.ensureChunk(id, false)
}

func (chunkService *ChunkService) ReadRPC(request utils.ReadArgs, reply *utils.ReadReply) error {
	value, exists := chunkService.chunks.Load(request.Id)
	if !exists {
		return errors.New("no corresponding chunk")
	}

	chunk := value.(*Chunk)
	chunk.Lock.RLock()
	data, err := chunk.Read(chunkService.settings.GetChunkPath(request.Id), request.Offset, request.Length)
	chunk.Lock.RUnlock()
	if err != nil {
		log.Error().Err(err).Msgf("error when reading to chunk with id %d", request.Id)
		return err
	}
	reply.Data = data

	return nil
}

func (chunkService *ChunkService) EnsureChunkRPC(request utils.EnsureChunkArgs, _ *utils.EnsureChunkReply) error {
	_, err := chunkService.ensureChunk(request.ChunkId, true)
	return err
}

func (chunkService *ChunkService) GrantLeaseRPC(request utils.GrantLeaseArgs, _ *utils.GrantLeaseReply) error {
	log.Debug().Msgf("received lease for chunk %d", request.ChunkId)

	// Create the chunk on our replicas
	ensureChunkRequest := utils.EnsureChunkArgs{
		ChunkId: request.ChunkId,
	}
	for _, server := range request.Replication {
		if server.Id == chunkService.id {
			continue
		}

		err := server.Endpoint.Call("ChunkService.EnsureChunkRPC", &ensureChunkRequest, &utils.EnsureChunkReply{})
		if err != nil {
			return err
		}
	}

	// Create the chunk on primary
	chunk, err := chunkService.ensureChunk(request.ChunkId, true)
	if err != nil {
		return err
	}

	chunk.LeaseMutex.Lock()
	chunk.GiveLeaseNow(request.Replication)
	chunk.LeaseMutex.Unlock()

	return nil
}

func (chunkService *ChunkService) RevokeLeaseRPC(request utils.RevokeLeaseArgs, _ *utils.RevokeLeaseReply) error {
	value, exists := chunkService.chunks.Load(request.ChunkId)
	if !exists {
		return errors.New("no corresponding chunk")
	}
	chunk := value.(*Chunk)

	chunk.LeaseMutex.Lock()
	chunk.RevokeLease()
	chunk.LeaseMutex.Unlock()

	return nil
}

func (chunkService *ChunkService) PushDataRPC(request utils.PushDataArgs, _ *utils.PushDataReply) (err error) {
	if len(request.Data) == 0 {
		return errors.New("empty Data")
	}

	// Write data in memory cache
	chunkService.writeTemporary(request.Id, request.Data)

	// Push to next server if we are not the last one
	var forwardReply utils.PushDataReply
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

	log.Debug().Msgf("received temporary data of length %d with id %s", len(request.Data), request.Id.String())

	return
}

// ApplyWriteRPC (replicas only)
func (chunkService *ChunkService) ApplyWriteRPC(request utils.ApplyWriteArgs, _ *utils.ApplyWriteReply) error {
	chunk, err := chunkService.getChunk(request.Id)
	if err != nil {
		log.Error().Err(err).Msg("calling getChunk")
		return err
	}

	data, exists := chunkService.readTemporary(request.DataId)
	if !exists {
		return errors.New("temporary data not found")
	}
	chunk.Lock.Lock()
	err = chunk.Write(chunkService.settings.GetChunkPath(request.Id), request.Offset, data)
	chunk.Lock.Unlock()
	if err != nil {
		log.Error().Err(err).Msgf("error when writing to chunk with id %d", request.Id)
		return err
	}

	log.Debug().Msgf("wrote %d bytes to chunk %d at offset %d", len(data), request.Id, request.Offset)

	return nil
}

// WriteRPC (primary only)
func (chunkService *ChunkService) WriteRPC(request utils.WriteArgs, _ *utils.WriteReply) error {
	chunk, err := chunkService.getChunk(request.Id)
	if err != nil {
		log.Error().Err(err).Msg("calling getChunk")
		return err
	}

	// Check lease
	chunk.LeaseMutex.RLock()
	defer chunk.LeaseMutex.RUnlock()
	if !chunk.HasLease() {
		log.Debug().Msgf("no lease for writing to chunk %d", request.Id)
		return &utils.NoLeaseError{Message: fmt.Sprintf("no lease for chunk with id %d", request.Id)}
	}

	err = chunkService.ApplyWriteRPC(request, &utils.WriteReply{})
	if err != nil {
		return err
	}

	completion := make(chan error)
	chunkService.chunkOperations <- Operation{
		chunk:      chunk,
		request:    request,
		completion: completion,
	}

	err = <-completion

	return err
}

// RecordAppendRPC (primary only)
func (chunkService *ChunkService) RecordAppendRPC(request utils.RecordAppendArgs, reply *utils.RecordAppendReply) error {
	chunk, err := chunkService.getChunk(request.Id) // Chunk is created when receiving lease
	if err != nil {
		log.Error().Err(err).Msg("calling getChunk")
		return err
	}

	chunk.LeaseMutex.RLock()
	defer chunk.LeaseMutex.RUnlock()
	if !chunk.HasLease() {
		log.Debug().Msgf("no lease for writing to chunk %d", request.Id)
		return &utils.NoLeaseError{Message: fmt.Sprintf("no lease for chunk with id %d", request.Id)}
	}

	data, exists := chunkService.readTemporary(request.DataId)
	if !exists {
		return errors.New("temporary data not found")
	}

	chunk.Lock.Lock()
	padding, offset, err := chunk.Append(chunkService.settings.GetChunkPath(request.Id), data)
	if err != nil {
		chunk.Lock.Unlock()
		log.Error().Err(err).Msgf("error when appending to chunk with id %d", request.Id)
		return err
	}

	writeRequest := utils.WriteArgs{
		Id:     request.Id,
		DataId: request.DataId,
		Offset: offset,
	}
	completion := make(chan error)
	chunkService.chunkOperations <- Operation{
		chunk:      chunk,
		request:    writeRequest,
		completion: completion,
	}
	chunk.Lock.Unlock()

	log.Debug().Str("Appended on primary", request.DataId.String()).Send()

	err = <-completion
	if err != nil {
		return err
	}

	log.Debug().Msgf("record append of %d bytes done for chunk %d at offset %d", len(data), request.Id, offset)

	*reply = utils.RecordAppendReply{
		Done: !padding,
		Pos:  offset,
	}

	return nil
}

func NewChunkService(settings Settings) (chunkService *ChunkService, err error) {
	chunkService = new(ChunkService)
	chunkService.settings = settings
	chunkService.chunkOperations = make(chan Operation)
	chunkService.temporaryData, err = lru.New[uuid.UUID, []byte](4096)

	return
}
