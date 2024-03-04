package master

import (
	"Google_File_System/lib/utils"
	"errors"
	"github.com/rs/zerolog/log"
	"math"
	"strconv"
	"sync"
	"sync/atomic"
	"time"
)

func (masterService *MasterService) chooseLeastLeased(servers []utils.ChunkServerId) (leastLeasedServer utils.ChunkServerId) {
	leastLeasedCount := uint64(math.MaxUint64)

	for _, serverId := range servers {
		value, exists := masterService.ChunkLocationData.chunkServers.Load(serverId)

		if exists {
			serverMetadata := value.(*ChunkServerMetadata)

			leaseCount := serverMetadata.leaseCount.Load()
			if leaseCount < leastLeasedCount {
				leastLeasedCount = leaseCount
				leastLeasedServer = serverId
			}
		}
	}

	return
}

type ChunkReplication struct {
	Replication map[utils.ChunkId][]utils.ChunkServerId // Servers storing the chunk, including primary if any
	mutex       sync.Mutex
}

type ChunkServerMetadata struct {
	utils.ChunkServer
	LastHeartbeat time.Time
	Chunks        []utils.ChunkId
	Heartbeat     *utils.ResettableTimer
	leaseCount    atomic.Uint64
}

type ChunkLocationData struct {
	ChunkReplication ChunkReplication
	chunkServers     sync.Map // common.ChunkServerId => *ChunkServerMetadata
	// TODO lease map: ChunkId => ChunkServerId + expiration
}

type MasterService struct {
	Settings                   Settings
	nextAvailableChunkId       atomic.Uint32
	nextAvailableChunkServerId atomic.Uint32
	Namespace                  *Namespace
	ChunkLocationData          ChunkLocationData
}

func (masterService *MasterService) getChunkServersForNewChunk(n uint32) (servers []utils.ChunkServerId) {
	type OccupiedServer = struct {
		id        utils.ChunkServerId
		occupancy int
	}

	leastOccupiedServers := make([]OccupiedServer, 0, n) // No need to use a heap as n is small

	masterService.ChunkLocationData.chunkServers.Range(func(key any, value any) bool {
		server := value.(*ChunkServerMetadata)
		occupancy := len(server.Chunks) // TODO lock access

		if len(leastOccupiedServers) < int(n) {
			leastOccupiedServers = append(leastOccupiedServers, OccupiedServer{id: server.Id, occupancy: occupancy})
		} else {
			for i, occupiedServer := range leastOccupiedServers {
				if occupancy < occupiedServer.occupancy {
					leastOccupiedServers[i] = OccupiedServer{id: server.Id, occupancy: occupancy}
				}
			}
		}
		return true
	})

	servers = make([]utils.ChunkServerId, len(leastOccupiedServers))
	for i, server := range leastOccupiedServers {
		servers[i] = server.id
	}

	return
}

func (masterService *MasterService) expireChunks(chunkServerId utils.ChunkServerId, expiredChunks []utils.ChunkId) {
	chunkReplication := &masterService.ChunkLocationData.ChunkReplication
	chunkReplication.mutex.Lock()
	for _, chunkId := range expiredChunks {
		replication := utils.Remove(chunkReplication.Replication[chunkId], chunkServerId)
		chunkReplication.Replication[chunkId] = replication
		// TODO if below per chunk/file replication goal
		if len(replication) == 0 {
			log.Error().Msgf("chunk with id %d has no replica, it is lost", chunkId)
		}
	}
	chunkReplication.mutex.Unlock()
}

func (masterService *MasterService) ensureChunkServer(endpoint utils.Endpoint, chunkServerId utils.ChunkServerId) bool {
	chunkServers := &masterService.ChunkLocationData.chunkServers

	newChunkServer := &ChunkServerMetadata{
		ChunkServer: utils.ChunkServer{
			Id:       chunkServerId,
			Endpoint: endpoint,
		},
		LastHeartbeat: time.Now(),
		Chunks:        make([]utils.ChunkId, 0),
		Heartbeat:     utils.NewResettableTimer(10 * time.Second),
	}

	// TODO mutex heartbeats ?
	chunkServer, exists := chunkServers.LoadOrStore(chunkServerId, newChunkServer)
	var heartbeat *utils.ResettableTimer
	if exists {
		heartbeat = chunkServer.(*ChunkServerMetadata).Heartbeat
	} else {
		// TODO no need to expire whatsoever when there is no chunks
		heartbeat = newChunkServer.Heartbeat
	}

	go func() {
		select {
		case <-heartbeat.C:
			chunkServers := &masterService.ChunkLocationData.chunkServers
			chunkServer, exists := chunkServers.Load(chunkServerId)
			if exists {
				expiredChunks := chunkServer.(*ChunkServerMetadata).Chunks
				log.Error().Msgf("heartbeat timer expired for chunk server with id=%d containing %d chunks", chunkServerId, len(expiredChunks))
				masterService.expireChunks(chunkServerId, expiredChunks)
			}
		}
	}()

	return exists
}

func (masterService *MasterService) RegisterRPC(request utils.RegisterArgs, reply *utils.RegisterReply) error {
	id := masterService.nextAvailableChunkServerId.Add(1) - 1
	// TODO persist it + Sync, before replying
	*reply = id

	exists := masterService.ensureChunkServer(request, id)
	if exists {
		log.Error().Msgf("trying to register an already existing chunk server with id=%d", id)
	} else {
		log.Debug().Msgf("registering a new chunk server with id=%d", id)
	}

	return nil
}

func (masterService *MasterService) HeartbeatRPC(request utils.HeartBeatArgs, _ *utils.HeartBeatReply) error {
	chunkServers := &masterService.ChunkLocationData.chunkServers
	chunkServerId := request.Id
	value, exists := chunkServers.Load(chunkServerId)
	if !exists {
		return errors.New("unregistered chunk server with id=" + strconv.Itoa(int(chunkServerId)))
	}
	chunkServer := value.(*ChunkServerMetadata)

	// TODO reset the timer
	chunkServer.Heartbeat.Reset(10 * time.Second)

	currentTime := time.Now()
	var previousTime time.Time
	if exists {
		previousTime = chunkServer.LastHeartbeat
	} else {
		previousTime = currentTime
	}
	chunkServer.LastHeartbeat = currentTime
	added, removed := utils.Diff(chunkServer.Chunks, request.Chunks)
	chunkServer.Chunks = request.Chunks
	chunkServers.Store(chunkServerId, chunkServer)

	log.Debug().Msgf("heartbeat from chunk server with id=%d and %d chunks after %v", chunkServerId, len(request.Chunks), currentTime.Sub(previousTime))

	chunkReplication := &masterService.ChunkLocationData.ChunkReplication
	for _, chunkId := range added {
		chunkReplication.mutex.Lock()
		chunkReplication.Replication[chunkId] = utils.Insert(chunkReplication.Replication[chunkId], chunkServerId)
		chunkReplication.mutex.Unlock()
	}
	for _, chunkId := range removed {
		chunkReplication.mutex.Lock()
		chunkReplication.Replication[chunkId] = utils.Remove(chunkReplication.Replication[chunkId], chunkServerId)
		chunkReplication.mutex.Unlock()
	}
	// TODO we can't immediately act

	// TODO   for additions => Impossible because master won't return a new chunk id before it being durable
	// TODO   for missing => check replication

	// TODO before setting it check if some Chunks disappeared
	// TODO if so, check their replication status
	// TODO if replication status is insufficient
	// TODO queue for re-replication, with priorities

	return nil
}

func (masterService *MasterService) MkdirRPC(request utils.MkdirArgs, _ *utils.MkdirReply) (err error) {
	var path string
	path, err = normalize(request.Path)
	if err != nil {
		return err
	}
	if path == "/" {
		return errors.New("directory already exists")
	}

	err = masterService.Namespace.Mkdir(path)

	return
}

func (masterService *MasterService) RmdirRPC(request utils.RmdirArgs, _ *utils.RmdirReply) (err error) {
	var path string
	path, err = normalize(request.Path)
	if err != nil {
		return err
	}
	if path == "/" {
		return errors.New("cannot remove root directory")
	}

	err = masterService.Namespace.Rmdir(path)

	return
}

func (masterService *MasterService) LsRPC(request utils.LsArgs, reply *utils.LsReply) (err error) {
	var path string
	path, err = normalize(request.Path)
	if err != nil {
		return err
	}

	var paths []string
	paths, err = masterService.Namespace.Ls(path)
	*reply = utils.LsReply{
		Paths: paths,
	}

	return
}

func (masterService *MasterService) CreateRPC(request utils.CreateArgs, _ *utils.CreateReply) (err error) {
	var path string
	path, err = normalize(request.Path)
	if err != nil {
		return err
	}
	if path == "/" {
		return errors.New("cannot create file with name which is root")
	}

	err = masterService.Namespace.Create(path)

	return
}

func (masterService *MasterService) DeleteRPC(request utils.DeleteArgs, _ *utils.DeleteReply) (err error) {
	var path string
	path, err = normalize(request.Path)
	if err != nil {
		return err
	}
	if path == "/" {
		return errors.New("cannot remove root directory")
	}

	// Remove file from namespace
	var file *File
	file, err = masterService.Namespace.Delete(path)

	if file == nil {
		return errors.New("no such file")
	}

	if err != nil {
		return
	}

	// Remove chunks of the file
	chunkReplication := &masterService.ChunkLocationData.ChunkReplication
	chunkReplication.mutex.Lock()
	for _, chunk := range file.chunks {
		delete(chunkReplication.Replication, chunk.Id)
	}
	chunkReplication.mutex.Unlock()

	return
}

func (masterService *MasterService) RecordAppendChunksRPC(request utils.RecordAppendChunksArgs, reply *utils.RecordAppendChunksReply) (err error) {
	var path string
	path, err = normalize(request.Path)
	if err != nil {
		return err
	}

	err = masterService.Namespace.LockFileAncestors(path, func(file *File) {
		file.mutex.Lock()
		defer file.mutex.Unlock()

		var chunkId utils.ChunkId
		var primaryServerId utils.ChunkServerId
		servers := make([]utils.ChunkServer, 0)

		nr := max(request.Nr, len(file.chunks)-1)
		file.iterate(masterService, nr, nr, func(chunk *Chunk) bool {
			chunkId = (*chunk).Id

			selectedServers := (*chunk).ensureInitialized(masterService)

			// Add corresponding servers to the reply
			for _, chunkServerId := range selectedServers {
				chunkServerMetadata, exists := masterService.ChunkLocationData.chunkServers.Load(chunkServerId)
				if exists {
					servers = utils.Insert(servers, chunkServerMetadata.(*ChunkServerMetadata).ChunkServer)
				}
			}

			primaryServerId, err = (*chunk).ensureLease(masterService)
			if err != nil {
				return false
			}

			return false
		})

		if err != nil {
			return
		}

		// Reply with info about servers
		replication := make([]utils.ChunkReplication, 0)
		masterService.ChunkLocationData.ChunkReplication.mutex.Lock()
		chunkReplication := utils.ChunkReplication{
			Id:      chunkId,
			Servers: masterService.ChunkLocationData.ChunkReplication.Replication[chunkId],
		}
		replication = append(replication, chunkReplication)
		masterService.ChunkLocationData.ChunkReplication.mutex.Unlock()

		*reply = utils.RecordAppendChunksReply{
			Nr:        nr,
			Id:        chunkId,
			PrimaryId: primaryServerId,
			ServersLocation: utils.ServersLocation{
				Servers:     servers,
				Replication: replication,
			},
		}
	})

	return
}

const (
	READ  = iota
	WRITE = iota
)

func (masterService *MasterService) readWriteChunks(mode int, request utils.ReadWriteChunks, reply *utils.ChunksAndServersLocation) (err error) {
	var path string
	path, err = normalize(request.Path)
	if err != nil {
		return err
	}

	err = masterService.Namespace.LockFileAncestors(path, func(file *File) {
		if mode == READ {
			file.mutex.RLock()
			defer file.mutex.RUnlock()
		} else {
			file.mutex.Lock()
			defer file.mutex.Unlock()
		}

		startChunkNr := int(request.Offset / utils.ChunkSize)
		endChunkNr := int((request.Offset + request.Length - 1) / utils.ChunkSize)

		chunksCount := endChunkNr - startChunkNr + 1

		chunks := make([]utils.ChunkId, 0, chunksCount)
		servers := make([]utils.ChunkServer, 0, chunksCount)
		var primaryServers map[utils.ChunkId]utils.ChunkServerId
		if mode == WRITE {
			primaryServers = make(map[utils.ChunkId]utils.ChunkServerId, chunksCount)
		}

		// TODO if one does not exist respond with error
		// TODO do not initialize anything if absent
		file.iterate(masterService, startChunkNr, endChunkNr, func(chunk *Chunk) bool {
			chunks = append(chunks, (*chunk).Id)

			if mode == READ && !chunk.Initialized {
				err = errors.New("reading uninitialized chunk")
				return false
			}
			selectedServers := (*chunk).ensureInitialized(masterService)

			// Add corresponding servers to the reply
			for _, chunkServerId := range selectedServers {
				chunkServerMetadata, exists := masterService.ChunkLocationData.chunkServers.Load(chunkServerId)
				if exists {
					servers = utils.Insert(servers, chunkServerMetadata.(*ChunkServerMetadata).ChunkServer)
				}
			}

			if mode == WRITE {
				var primaryServerId utils.ChunkServerId
				primaryServerId, err = (*chunk).ensureLease(masterService)
				if err != nil {
					return false
				}
				primaryServers[chunk.Id] = primaryServerId
			}
			return true
		})

		if err != nil {
			return
		}

		// Reply with info about servers
		replication := make([]utils.ChunkReplication, 0, chunksCount)
		masterService.ChunkLocationData.ChunkReplication.mutex.Lock()
		for _, chunkId := range chunks {
			chunkReplication := utils.ChunkReplication{
				Id:      chunkId,
				Servers: masterService.ChunkLocationData.ChunkReplication.Replication[chunkId],
			}
			replication = append(replication, chunkReplication)
		}
		masterService.ChunkLocationData.ChunkReplication.mutex.Unlock()

		*reply = utils.WriteChunksReply{
			PrimaryServers: primaryServers,
			ServersLocation: utils.ServersLocation{
				Servers:     servers,
				Replication: replication,
			},
			Chunks: chunks,
		}
	})

	return nil
}

func (masterService *MasterService) WriteChunksRPC(request utils.WriteChunksArgs, reply *utils.WriteChunksReply) error {
	return masterService.readWriteChunks(WRITE, request, reply)
}

func (masterService *MasterService) ReadChunksRPC(request utils.ReadChunksArgs, reply *utils.ReadChunksReply) error {
	return masterService.readWriteChunks(READ, request, reply)
}
