package master

import (
	"Google_File_System/lib/utils"
	"errors"
	"github.com/rs/zerolog/log"
	"math"
	"strconv"
	"strings"
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

type Directory struct {
	// TODO add methods ?
	mutex sync.RWMutex
	Files []string
}

type File struct {
	// TODO add methods ?
	mutex  sync.RWMutex
	chunks []*ChunkMetadataMaster // consecutive
}

func (file *File) appendUninitializedChunk(chunkId utils.ChunkId) {
	uninitializedChunk := &ChunkMetadataMaster{
		Id:              chunkId,
		Initialized:     false,
		ReplicationGoal: 0,
	}

	file.chunks = append(file.chunks, uninitializedChunk)
}

func (file *File) iterate(masterService *MasterService, startChunkNr int, endChunkNr int, f func(chunk *ChunkMetadataMaster) bool) {
	// Ensure that we have uninitialized chunks
	file.mutex.Lock()
	for len(file.chunks) < endChunkNr+1 {
		chunkId := masterService.nextAvailableChunkId.Add(1) - 1
		file.appendUninitializedChunk(chunkId)
	}
	file.mutex.Unlock()
	// TODO can we access array when other may modify it ?
	// TODO read lock ?

	// Initialize chunks that will be written to
	for i := startChunkNr; i <= endChunkNr; i++ {
		// TODO get chunk or initialize it
		chunk := file.chunks[i]
		if !f(chunk) {
			break
		}
	}
}

type ChunkReplication struct {
	Replication map[utils.ChunkId][]utils.ChunkServerId // Replication is ~3, so a simple array should be good enough
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
	Namespace                  sync.Map // string => *Directory | *File
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
		chunks := utils.Remove(chunkReplication.Replication[chunkId], chunkServerId)
		chunkReplication.Replication[chunkId] = chunks
		// TODO if below replication goal
		if len(chunks) == 0 {
			log.Error().Msgf("chunk with id %d has no replica, it is lost", chunkId)
		}
		// TODO if below chunk replication goal
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

// lockAncestors check ancestors and read lock all items in the path excluding the last one
// Then either returns an error or calls f(value) which can be either *Directory | *File
func (masterService *MasterService) lockAncestors(path string, f func(value any)) error {
	// TODO simplify parts logic
	var parts []string
	if path == "/" {
		parts = append(parts, "")
	} else {
		parts = strings.Split(path, "/")
	}
	ancestor := ""
	n := len(parts)
	for i := 0; i < n; i++ {
		if ancestor != "/" {
			ancestor += "/"
		}
		ancestor += parts[i]

		value, exists := masterService.Namespace.Load(ancestor)
		if !exists {
			break
		}

		if i == n-1 {
			f(value)
			return nil
		}

		switch fileSystemEntry := value.(type) {
		case *File:
			return errors.New("an ancestor in the path is a file " + ancestor)
		case *Directory:
			fileSystemEntry.mutex.RLock()
			defer fileSystemEntry.mutex.RUnlock()
		}
	}

	return errors.New("a parent directory does not exist " + ancestor)
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

	// Lock everything strictly above parent directory
	directory := parentPath(path)
	err = masterService.lockAncestors(directory, func(value any) {
		switch fileSystemEntry := value.(type) {
		case *File:
			err = errors.New("parent directory is a file")
			return
		case *Directory:
			// Write lock parent directory
			fileSystemEntry.mutex.Lock()
			defer fileSystemEntry.mutex.Unlock()

			// Create new directory if it does not already exist
			newDirectory := &Directory{
				Files: make([]string, 0),
			}
			if _, exists := masterService.Namespace.LoadOrStore(path, newDirectory); exists {
				err = errors.New("directory already exists")
				return
			}

			// Insert new directory in parent directory
			fileSystemEntry.Files = append(fileSystemEntry.Files, path)
		}
	})

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

	// Lock everything strictly above parent directory
	directory := parentPath(path)
	err = masterService.lockAncestors(directory, func(value any) {
		switch fileSystemEntry := value.(type) {
		case *File:
			err = errors.New("parent directory is a file")
			return
		case *Directory:
			// Write lock parent directory
			fileSystemEntry.mutex.Lock()
			defer fileSystemEntry.mutex.Unlock()

			// Check that directory to remove exists and is empty
			value, exists := masterService.Namespace.Load(path)
			if !exists {
				err = errors.New("no such directory")
				return
			}

			switch fileSystemEntry := value.(type) {
			case *File:
				err = errors.New("can't remove file")
				return
			case *Directory:
				fileSystemEntry.mutex.RLock()
				defer fileSystemEntry.mutex.RUnlock()

				if len(fileSystemEntry.Files) != 0 {
					err = errors.New("directory is not empty")
				}

				masterService.Namespace.Delete(path)
			}

			// Remove directory from parent directory
			fileSystemEntry.Files = utils.Remove(fileSystemEntry.Files, path)
		}
	})

	return
}

func (masterService *MasterService) LsRPC(request utils.LsArgs, reply *utils.LsReply) (err error) {
	var path string
	path, err = normalize(request.Path)
	if err != nil {
		return err
	}

	// Lock everything strictly above directory
	err = masterService.lockAncestors(path, func(value any) {
		switch fileSystemEntry := value.(type) {
		case *File:
			paths := make([]string, 1)
			paths[0] = path
			*reply = utils.LsReply{
				Paths: paths,
			}
			return
		case *Directory:
			// Read lock directory
			fileSystemEntry.mutex.RLock()
			defer fileSystemEntry.mutex.RUnlock()

			*reply = utils.LsReply{
				Paths: fileSystemEntry.Files,
			}
		}
	})

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

	// Lock everything strictly above parent directory
	directory := parentPath(path)
	err = masterService.lockAncestors(directory, func(value any) {
		switch fileSystemEntry := value.(type) {
		case *File:
			err = errors.New("parent directory is a file")
			return
		case *Directory:
			// Write lock parent directory
			fileSystemEntry.mutex.Lock()
			defer fileSystemEntry.mutex.Unlock()

			// Creating the file
			file := &File{
				chunks: make([]*ChunkMetadataMaster, 0),
			}
			_, exists := masterService.Namespace.LoadOrStore(path, file)
			if exists {
				err = errors.New("file already exists")
				return
			}

			// Inserting the file in its parent directory
			fileSystemEntry.Files = append(fileSystemEntry.Files, path)
		}
	})

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

	// Lock everything strictly above parent directory
	directory := parentPath(path)
	err = masterService.lockAncestors(directory, func(value any) {
		switch fileSystemEntry := value.(type) {
		case *File:
			err = errors.New("parent directory is a file")
			return
		case *Directory:
			// Write lock parent directory
			fileSystemEntry.mutex.Lock()
			defer fileSystemEntry.mutex.Unlock()

			// Check if file to remove is indeed a file and exists
			value, exists := masterService.Namespace.Load(path)
			if !exists {
				err = errors.New("no such file")
				return
			}
			switch fileSystemEntry := value.(type) {
			case *File:
				// Remove chunks of the file
				chunkReplication := &masterService.ChunkLocationData.ChunkReplication
				chunkReplication.mutex.Lock()
				for _, chunk := range fileSystemEntry.chunks {
					delete(chunkReplication.Replication, chunk.Id)
				}
				chunkReplication.mutex.Unlock()

				// Remove file
				masterService.Namespace.Delete(path)
			case *Directory:
				err = errors.New("file to delete is a directory")
				return
			}

			// Remove file from its parent directory
			fileSystemEntry.Files = utils.Remove(fileSystemEntry.Files, path)
		}
	})

	return
}

func (masterService *MasterService) RecordAppendChunksRPC(request utils.RecordAppendChunksArgs, reply *utils.RecordAppendChunksReply) (err error) {
	var path string
	path, err = normalize(request.Path)
	if err != nil {
		return err
	}

	err = masterService.lockAncestors(request.Path, func(value any) {
		value, exists := masterService.Namespace.Load(path)
		// Check if the file exists
		if !exists {
			err = errors.New("no such file or directory")
			return
		}

		switch fileSystemEntry := value.(type) {
		case *Directory:
			err = errors.New("trying to read a directory")
			return
		case *File:
			var chunkId utils.ChunkId
			servers := make([]utils.ChunkServer, 0)

			fileSystemEntry.mutex.RLock()
			nr := max(request.Nr, len(fileSystemEntry.chunks)-1)
			fileSystemEntry.mutex.RUnlock()
			var primaryId utils.ChunkServerId
			fileSystemEntry.iterate(masterService, nr, nr, func(chunk *ChunkMetadataMaster) bool {
				chunkId = (*chunk).Id

				selectedServers := (*chunk).ensureInitialized(masterService)
				primaryId = selectedServers[0]

				// Add corresponding servers to the reply
				for _, chunkServerId := range selectedServers {
					chunkServerMetadata, exists := masterService.ChunkLocationData.chunkServers.Load(chunkServerId)
					if exists {
						servers = utils.Insert(servers, chunkServerMetadata.(*ChunkServerMetadata).ChunkServer)
					}
				}

				err = (*chunk).ensureLease(masterService)
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
				PrimaryId: primaryId,
				ServersLocation: utils.ServersLocation{
					Servers:     servers,
					Replication: replication,
				},
			}
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

	err = masterService.lockAncestors(request.Path, func(value any) {
		value, exists := masterService.Namespace.Load(path)
		// Check if the file exists
		if !exists {
			err = errors.New("no such file or directory")
			return
		}

		switch fileSystemEntry := value.(type) {
		case *Directory:
			err = errors.New("trying to read a directory")
			return
		case *File:
			startChunkNr := int(request.Offset / utils.ChunkSize)
			endChunkNr := int((request.Offset + request.Length - 1) / utils.ChunkSize)

			chunks := make([]utils.ChunkId, 0, endChunkNr-startChunkNr+1)
			servers := make([]utils.ChunkServer, 0)

			// TODO if one does not exist respond with error
			// TODO do not initialize anything if absent
			var primaryId utils.ChunkServerId
			fileSystemEntry.iterate(masterService, startChunkNr, endChunkNr, func(chunk *ChunkMetadataMaster) bool {
				chunks = append(chunks, (*chunk).Id)

				if mode == READ && !chunk.Initialized { // TODO lock
					err = errors.New("reading past the end of file")
					return false
				}
				selectedServers := (*chunk).ensureInitialized(masterService)
				primaryId = selectedServers[0]

				// Add corresponding servers to the reply
				for _, chunkServerId := range selectedServers {
					chunkServerMetadata, exists := masterService.ChunkLocationData.chunkServers.Load(chunkServerId)
					if exists {
						servers = utils.Insert(servers, chunkServerMetadata.(*ChunkServerMetadata).ChunkServer)
					}
				}

				if mode == WRITE {
					err = (*chunk).ensureLease(masterService)
					if err != nil {
						return false
					}
				}
				return true
			})

			if err != nil {
				return
			}

			// Reply with info about servers
			replication := make([]utils.ChunkReplication, 0)
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
				PrimaryId: primaryId,
				ServersLocation: utils.ServersLocation{
					Servers:     servers,
					Replication: replication,
				},
				Chunks: chunks,
			}
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
