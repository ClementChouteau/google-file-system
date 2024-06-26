package chunkServer

import (
	"Google_File_System/lib/utils"
	"encoding/binary"
	"errors"
	"fmt"
	"github.com/google/uuid"
	lru "github.com/hashicorp/golang-lru/v2"
	"github.com/rs/zerolog/log"
	"golang.org/x/sync/errgroup"
	"net/rpc"
	"os"
	"path"
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
	chunks          sync.Map // utils.ChunkId => *Chunk
	chunkVersions   ChunkVersions
	servers         sync.Map // utils.ChunkServerId => utils.ChunkServer
	temporaryData   *lru.Cache[uuid.UUID, []byte]
}

func (chunkService *ChunkService) ensureFolders() (err error) {
	err = os.MkdirAll(chunkService.settings.GetChunksFolder(), 0755)
	if err != nil && !os.IsExist(err) {
		return err
	}

	return
}

func (chunkService *ChunkService) readChunkMetadataFromDisk() (count int, err error) {
	file, versions, err := readChunkVersionsFromDisk(chunkService.settings.GetChunkVersionsPath())
	if err != nil {
		return 0, err
	}
	chunkService.chunkVersions.file = file

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

		chunkId := utils.ChunkId(id)

		version, exists := versions[chunkId] // No need to lock as this happens before start
		if !exists {
			log.Error().Msgf("no version for chunk %d", chunkId)
			err := os.Remove(path.Join(chunksPath, file.Name()))
			if err != nil {
				log.Error().Err(err).Uint32("chunk", chunkId).Msg("removing chunk with no version")
			}
			continue
		}

		chunk := &Chunk{
			Id:            chunkId,
			Version:       version.version,
			VersionOffset: version.offset,
		}
		chunkService.chunks.Store(chunkId, chunk)
		count++
	}

	// TODO unset versions that have no corresponding chunk

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
	// TODO send version alongside chunks

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

func (chunkService *ChunkService) heartbeatTicker() {
	log.Info().Msg("starting to send heartbeats")
	heartbeatTick := time.Tick(5 * time.Second)
	chunkService.sendHeartbeat()
	for range heartbeatTick {
		chunkService.sendHeartbeat()
	}
}

func (chunkService *ChunkService) startServer() (err error) {
	server := utils.NewServer(chunkService.settings.Endpoint)

	err = server.Register(chunkService)
	if err != nil {
		return
	}

	err = server.Start()
	if err != nil {
		return
	}

	return
}

// Thread to send writes (in order) to replicas, assuming the operation is already applied on primary
func (chunkService *ChunkService) startOperations() {
	for {
		operation := <-chunkService.chunkOperations

		// Apply operation on all replicas
		errGroup := new(errgroup.Group)
		for _, server := range operation.chunk.Replication {
			if server.Id == chunkService.id {
				continue
			}

			endpoint := server.Endpoint
			errGroup.Go(func() error {
				writeReply := &utils.WriteReply{}
				return endpoint.Call("ChunkService.ApplyWriteRPC", operation.request, writeReply)
			})

		}
		err := errGroup.Wait()

		operation.completion <- err
	}
}

func (chunkService *ChunkService) Start() error {
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

	errorGroup := errgroup.Group{}
	errorGroup.Go(func() error {
		chunkService.heartbeatTicker()
		return nil
	})
	errorGroup.Go(func() error {
		return chunkService.startServer()
	})
	errorGroup.Go(func() error {
		go chunkService.startOperations()
		return nil
	})

	return errorGroup.Wait()
}

func (chunkService *ChunkService) writeTemporary(id uuid.UUID, data []byte) {
	chunkService.temporaryData.Add(id, data)
}

func (chunkService *ChunkService) readTemporary(id uuid.UUID) (data []byte, exists bool) {
	value, exists := chunkService.temporaryData.Get(id)
	chunkService.temporaryData.Remove(id)
	return value, exists
}

// createChunk ensures that the chunk exists, writes it
func (chunkService *ChunkService) createChunk(id utils.ChunkId, nextVersion utils.ChunkVersion) (chunk *Chunk, err error) {
	// Ensure that the file exists
	newChunk := &Chunk{
		Id:      id,
		Version: nextVersion,
	}
	newChunk.Lock.Lock() // Prevent accesses to chunk before it is properly initialized
	defer newChunk.Lock.Unlock()

	value, exists := chunkService.chunks.LoadOrStore(id, newChunk)
	chunk = value.(*Chunk)

	if !exists {
		// Create chunk file on disk
		err = chunk.Ensure(chunkService.settings.GetChunkPath(id))
		if err != nil {
			log.Error().Err(err).Msgf("could not create chunk %d", id)
			return
		}

		// Write chunk version
		chunkService.chunkVersions.lock.Lock()
		defer chunkService.chunkVersions.lock.Unlock()
		var offset int64
		offset, err = chunkService.chunkVersions.NextAvailableOffset()
		if err != nil {
			return
		}

		chunk.VersionOffset = offset
		err = chunkService.chunkVersions.Set(id, nextVersion, offset)
		if err != nil {
			return
		}

		log.Debug().Msgf("created chunk %d in version %d", id, nextVersion)
	} else {
		chunk.Lock.Lock()
		defer chunk.Lock.Unlock()

		if chunk.Version == nextVersion {
			return
		}

		// Increase chunk version
		chunk.Version = nextVersion
		chunkService.chunkVersions.lock.Lock()
		err = chunkService.chunkVersions.Set(id, nextVersion, chunk.VersionOffset)
		chunkService.chunkVersions.lock.Unlock()

		log.Debug().Msgf("increased chunk %d version to %d", id, nextVersion)
	}

	return
}

// getChunk accesses the chunk, returns an error if it does not exist
func (chunkService *ChunkService) getChunk(id utils.ChunkId) (chunk *Chunk, err error) {
	value, exists := chunkService.chunks.Load(id)
	if !exists {
		err = errors.New("no corresponding chunk")
		log.Error().Err(err).Uint32("chunk", id)
		return
	}
	chunk = value.(*Chunk)
	return
}

func (chunkService *ChunkService) ReadRPC(request utils.ReadArgs, reply *utils.ReadReply) error {
	value, exists := chunkService.chunks.Load(request.Id)
	if !exists {
		return errors.New("no corresponding chunk")
	}

	chunk := value.(*Chunk)
	if chunk.Version < request.MinVersion {
		err := errors.New("trying to read an outdated chunk")
		log.Error().Err(err).Uint32("chunk", request.Id).Send()
		return err
	}
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

func (chunkService *ChunkService) EnsureChunkRPC(request utils.EnsureChunkArgs, _ *utils.EnsureChunkReply) (err error) {
	_, err = chunkService.createChunk(request.ChunkId, request.Version)
	return
}

func (chunkService *ChunkService) GrantLeaseRPC(request utils.GrantLeaseArgs, _ *utils.GrantLeaseReply) error {
	log.Debug().Msgf("received lease for chunk %d in version %d", request.ChunkId, request.Version)

	// Create the chunk on our replicas
	ensureChunkRequest := utils.EnsureChunkArgs{
		ChunkId: request.ChunkId,
		Version: request.Version,
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
	chunk, err := chunkService.createChunk(request.ChunkId, request.Version)
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

	log.Debug().Str("dataId", request.Id.String()).Msgf("received temporary data of length %d", len(request.Data))

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

	if chunk.Version < request.MinVersion {
		err := errors.New("trying to read an outdated chunk")
		log.Error().Err(err).Uint32("chunk", request.Id).Send()
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

	if chunk.Version < request.MinVersion {
		err := errors.New("trying to read an outdated chunk")
		log.Error().Err(err).Uint32("chunk", request.Id).Send()
		return err
	}

	// Check lease
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

	log.Debug().Str("dataId", request.DataId.String()).Msg("Appended on primary")

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
