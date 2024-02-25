package main

import (
	"Google_File_System/utils/arraySet"
	"Google_File_System/utils/common"
	"Google_File_System/utils/rpcdefs"
	"errors"
	"flag"
	"github.com/rs/zerolog/log"
	"math"
	"net"
	"net/rpc"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

type ChunkMetadataMaster struct {
	Id                  common.ChunkId
	Initialized         bool // Uninitialized chunks can be parts of sparse files, they have no replication
	ReplicationGoal     uint32
	LeaseMutex          sync.Mutex
	Lease               time.Time // Time point when sending the lease
	LeasedChunkServerId common.ChunkServerId
}

func (masterService *MasterService) chooseLeastLeased(servers []common.ChunkServerId) (leastLeasedServer common.ChunkServerId) {
	leastLeasedCount := uint64(math.MaxUint64)

	for _, serverId := range servers {
		value, exists := masterService.chunkLocationData.chunkServers.Load(serverId)

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

func (chunk *ChunkMetadataMaster) ensureLease(masterService *MasterService) (err error) {
	chunk.LeaseMutex.Lock()
	defer chunk.LeaseMutex.Unlock()
	hasLease := !chunk.Lease.IsZero() && time.Now().Before(chunk.Lease.Add(common.LeaseDuration))
	if !hasLease {
		// Choose one of the chunk servers as the primary
		replication := masterService.chunkLocationData.chunkReplication.replication[chunk.Id]
		if len(replication) == 0 {
			// TODO error case
			return
		}
		primaryServerId := masterService.chooseLeastLeased(replication)
		primaryChunkServerMetadata, exists := masterService.chunkLocationData.chunkServers.Load(primaryServerId)
		primaryChunkServer := primaryChunkServerMetadata.(*ChunkServerMetadata).ChunkServer

		// Add corresponding servers to the reply
		replicas := arraySet.Remove(replication, primaryServerId)
		servers := make([]common.ChunkServer, 0, len(replicas))
		for _, chunkServerId := range replicas {
			chunkServerMetadata, exists := masterService.chunkLocationData.chunkServers.Load(chunkServerId)
			if exists {
				servers = arraySet.Insert(servers, chunkServerMetadata.(*ChunkServerMetadata).ChunkServer)
			}
		}

		if exists {
			// Grant lease to this new primary
			request := rpcdefs.GrantLeaseArgs{
				ChunkId:     chunk.Id,
				Replication: servers,
			}
			err = primaryChunkServer.Endpoint.Call("ChunkService.GrantLeaseRPC", request, &rpcdefs.GrantLeaseReply{})
			if err != nil {
				return
			}
			chunk.LeasedChunkServerId = primaryChunkServer.Id
			chunk.Lease = time.Now()
			primaryChunkServerMetadata.(*ChunkServerMetadata).leaseCount.Add(1)
		} else {
			return errors.New("server not found")
		}
	}
	return
}

func (chunk *ChunkMetadataMaster) ensureInitialized(masterService *MasterService) (servers []common.ChunkServerId) {
	if !chunk.Initialized {
		// TODO lock initialization
		chunk.Initialized = true
		replicationGoal := masterService.defaultReplicationGoal.Load()
		chunk.ReplicationGoal = replicationGoal
		servers = masterService.getChunkServersForNewChunk(replicationGoal)

		// Indicate that they replicate the selected chunks
		masterService.chunkLocationData.chunkReplication.mutex.Lock()
		masterService.chunkLocationData.chunkReplication.replication[chunk.Id] = servers
		masterService.chunkLocationData.chunkReplication.mutex.Unlock()
	} else {
		masterService.chunkLocationData.chunkReplication.mutex.Lock()
		servers = masterService.chunkLocationData.chunkReplication.replication[chunk.Id]
		masterService.chunkLocationData.chunkReplication.mutex.Unlock()
	}
	return
}

type Directory struct {
	// TODO add methods ?
	mutex sync.RWMutex
	files []string
}

type File struct {
	// TODO add methods ?
	mutex  sync.RWMutex
	chunks []*ChunkMetadataMaster // consecutive
}

func (file *File) appendUninitializedChunk(chunkId common.ChunkId) {
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
	replication map[common.ChunkId][]common.ChunkServerId // Replication is ~3, so a simple array should be good enough
	mutex       sync.Mutex
}

type ChunkServerMetadata struct {
	common.ChunkServer
	LastHeartbeat time.Time
	Chunks        []common.ChunkId
	Heartbeat     *common.ResettableTimer
	leaseCount    atomic.Uint64
}

type ChunkLocationData struct {
	chunkReplication ChunkReplication
	chunkServers     sync.Map // common.ChunkServerId => *ChunkServerMetadata
	// TODO lease map: ChunkId => ChunkServerId + expiration
}

type MasterServerSettings struct {
	common.Endpoint
	folder string
}

type MasterService struct {
	settings                   MasterServerSettings
	nextAvailableChunkId       atomic.Uint32
	nextAvailableChunkServerId atomic.Uint32
	defaultReplicationGoal     atomic.Uint32
	namespace                  sync.Map // string => *Directory | *File
	chunkLocationData          ChunkLocationData
}

func (masterService *MasterService) getChunkServersForNewChunk(n uint32) (servers []common.ChunkServerId) {
	type OccupiedServer = struct {
		id        common.ChunkServerId
		occupancy int
	}

	leastOccupiedServers := make([]OccupiedServer, 0, n) // No need to use a heap as n is small

	masterService.chunkLocationData.chunkServers.Range(func(key any, value any) bool {
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

	servers = make([]common.ChunkServerId, len(leastOccupiedServers))
	for i, server := range leastOccupiedServers {
		servers[i] = server.id
	}

	return
}

func (masterService *MasterService) expireChunks(chunkServerId common.ChunkServerId, expiredChunks []common.ChunkId) {
	chunkReplication := &masterService.chunkLocationData.chunkReplication
	chunkReplication.mutex.Lock()
	for _, chunkId := range expiredChunks {
		chunks := arraySet.Remove(chunkReplication.replication[chunkId], chunkServerId)
		chunkReplication.replication[chunkId] = chunks
		// TODO if below replication goal
		if len(chunks) == 0 {
			log.Error().Msgf("chunk with id %d has no replica, it is lost", chunkId)
		}
		// TODO if below chunk replication goal
	}
	chunkReplication.mutex.Unlock()
}

func (masterService *MasterService) ensureChunkServer(endpoint common.Endpoint, chunkServerId common.ChunkServerId) bool {
	chunkServers := &masterService.chunkLocationData.chunkServers

	newChunkServer := &ChunkServerMetadata{
		ChunkServer: common.ChunkServer{
			Id:       chunkServerId,
			Endpoint: endpoint,
		},
		LastHeartbeat: time.Now(),
		Chunks:        make([]common.ChunkId, 0),
		Heartbeat:     common.NewResettableTimer(10 * time.Second),
	}

	// TODO mutex heartbeats ?
	chunkServer, exists := chunkServers.LoadOrStore(chunkServerId, newChunkServer)
	var heartbeat *common.ResettableTimer
	if exists {
		heartbeat = chunkServer.(*ChunkServerMetadata).Heartbeat
	} else {
		// TODO no need to expire whatsoever when there is no chunks
		heartbeat = newChunkServer.Heartbeat
	}

	go func() {
		select {
		case <-heartbeat.C:
			chunkServers := &masterService.chunkLocationData.chunkServers
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

func (masterService *MasterService) RegisterRPC(request rpcdefs.RegisterArgs, reply *rpcdefs.RegisterReply) error {
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

func (masterService *MasterService) HeartbeatRPC(request rpcdefs.HeartBeatArgs, _ *rpcdefs.HeartBeatReply) error {
	chunkServers := &masterService.chunkLocationData.chunkServers
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
	added, removed := arraySet.Diff(chunkServer.Chunks, request.Chunks)
	chunkServer.Chunks = request.Chunks
	chunkServers.Store(chunkServerId, chunkServer)

	log.Debug().Msgf("heartbeat from chunk server with id=%d and %d chunks after %v", chunkServerId, len(request.Chunks), currentTime.Sub(previousTime))

	chunkReplication := &masterService.chunkLocationData.chunkReplication
	for _, chunkId := range added {
		chunkReplication.mutex.Lock()
		chunkReplication.replication[chunkId] = arraySet.Insert(chunkReplication.replication[chunkId], chunkServerId)
		chunkReplication.mutex.Unlock()
	}
	for _, chunkId := range removed {
		chunkReplication.mutex.Lock()
		chunkReplication.replication[chunkId] = arraySet.Remove(chunkReplication.replication[chunkId], chunkServerId)
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

func normalize(path string) (string, error) {
	if len(path) == 0 {
		return "", errors.New("empty path")
	}
	if !strings.HasPrefix(path, "/") {
		return "", errors.New("not an absolute path")
	}

	var builder strings.Builder
	builder.Grow(len(path))
	// Remove duplicated consecutive '/'
	prev := rune(0)
	for _, l := range path {
		if l != '/' || prev != '/' {
			builder.WriteRune(l)
		}
		prev = l
	}

	// Remove trailing '/'
	normalized, _ := strings.CutSuffix(builder.String(), "/")

	if len(normalized) == 0 {
		normalized += "/"
	}

	return normalized, nil
}

// Return the parent directory, example: "/a/b" => "/a'
func parentPath(path string) string {
	i := strings.LastIndex(path, "/")
	if i <= 0 {
		return "/"
	}
	return path[:i]
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

		value, exists := masterService.namespace.Load(ancestor)
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

func (masterService *MasterService) MkdirRPC(request rpcdefs.MkdirArgs, _ *rpcdefs.MkdirReply) (err error) {
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
				files: make([]string, 0),
			}
			if _, exists := masterService.namespace.LoadOrStore(path, newDirectory); exists {
				err = errors.New("directory already exists")
				return
			}

			// Insert new directory in parent directory
			fileSystemEntry.files = append(fileSystemEntry.files, path)
		}
	})

	return
}

func (masterService *MasterService) RmdirRPC(request rpcdefs.RmdirArgs, _ *rpcdefs.RmdirReply) (err error) {
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
			value, exists := masterService.namespace.Load(path)
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

				if len(fileSystemEntry.files) != 0 {
					err = errors.New("directory is not empty")
				}

				masterService.namespace.Delete(path)
			}

			// Remove directory from parent directory
			fileSystemEntry.files = arraySet.Remove(fileSystemEntry.files, path)
		}
	})

	return
}

func (masterService *MasterService) LsRPC(request rpcdefs.LsArgs, reply *rpcdefs.LsReply) (err error) {
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
			*reply = rpcdefs.LsReply{
				Paths: paths,
			}
			return
		case *Directory:
			// Read lock directory
			fileSystemEntry.mutex.RLock()
			defer fileSystemEntry.mutex.RUnlock()

			*reply = rpcdefs.LsReply{
				Paths: fileSystemEntry.files,
			}
		}
	})

	return
}

func (masterService *MasterService) CreateRPC(request rpcdefs.CreateArgs, _ *rpcdefs.CreateReply) (err error) {
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
			_, exists := masterService.namespace.LoadOrStore(path, file)
			if exists {
				err = errors.New("file already exists")
				return
			}

			// Inserting the file in its parent directory
			fileSystemEntry.files = append(fileSystemEntry.files, path)
		}
	})

	return
}

func (masterService *MasterService) DeleteRPC(request rpcdefs.DeleteArgs, _ *rpcdefs.DeleteReply) (err error) {
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
			value, exists := masterService.namespace.Load(path)
			if !exists {
				err = errors.New("no such file")
				return
			}
			switch fileSystemEntry := value.(type) {
			case *File:
				// Remove chunks of the file
				chunkReplication := &masterService.chunkLocationData.chunkReplication
				chunkReplication.mutex.Lock()
				for _, chunk := range fileSystemEntry.chunks {
					delete(chunkReplication.replication, chunk.Id)
				}
				chunkReplication.mutex.Unlock()

				// Remove file
				masterService.namespace.Delete(path)
			case *Directory:
				err = errors.New("file to delete is a directory")
				return
			}

			// Remove file from its parent directory
			fileSystemEntry.files = arraySet.Remove(fileSystemEntry.files, path)
		}
	})

	return
}

func (masterService *MasterService) RecordAppendChunksRPC(request rpcdefs.RecordAppendChunksArgs, reply *rpcdefs.RecordAppendChunksReply) (err error) {
	var path string
	path, err = normalize(request.Path)
	if err != nil {
		return err
	}

	err = masterService.lockAncestors(request.Path, func(value any) {
		value, exists := masterService.namespace.Load(path)
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
			var chunkId common.ChunkId
			servers := make([]common.ChunkServer, 0)

			fileSystemEntry.mutex.RLock()
			nr := max(request.Nr, len(fileSystemEntry.chunks)-1)
			fileSystemEntry.mutex.RUnlock()
			var primaryId common.ChunkServerId
			fileSystemEntry.iterate(masterService, nr, nr, func(chunk *ChunkMetadataMaster) bool {
				chunkId = (*chunk).Id

				selectedServers := (*chunk).ensureInitialized(masterService)
				primaryId = selectedServers[0]

				// Add corresponding servers to the reply
				for _, chunkServerId := range selectedServers {
					chunkServerMetadata, exists := masterService.chunkLocationData.chunkServers.Load(chunkServerId)
					if exists {
						servers = arraySet.Insert(servers, chunkServerMetadata.(*ChunkServerMetadata).ChunkServer)
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
			replication := make([]rpcdefs.ChunkReplication, 0)
			masterService.chunkLocationData.chunkReplication.mutex.Lock()
			chunkReplication := rpcdefs.ChunkReplication{
				Id:      chunkId,
				Servers: masterService.chunkLocationData.chunkReplication.replication[chunkId],
			}
			replication = append(replication, chunkReplication)
			masterService.chunkLocationData.chunkReplication.mutex.Unlock()

			*reply = rpcdefs.RecordAppendChunksReply{
				Nr:        nr,
				Id:        chunkId,
				PrimaryId: primaryId,
				ServersLocation: rpcdefs.ServersLocation{
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

func (masterService *MasterService) readWriteChunks(mode int, request rpcdefs.ReadWriteChunks, reply *rpcdefs.ChunksAndServersLocation) (err error) {
	var path string
	path, err = normalize(request.Path)
	if err != nil {
		return err
	}

	err = masterService.lockAncestors(request.Path, func(value any) {
		value, exists := masterService.namespace.Load(path)
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
			startChunkNr := int(request.Offset / common.ChunkSize)
			endChunkNr := int((request.Offset + request.Length - 1) / common.ChunkSize)

			chunks := make([]common.ChunkId, 0, endChunkNr-startChunkNr+1)
			servers := make([]common.ChunkServer, 0)

			// TODO if one does not exist respond with error
			// TODO do not initialize anything if absent
			var primaryId common.ChunkServerId
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
					chunkServerMetadata, exists := masterService.chunkLocationData.chunkServers.Load(chunkServerId)
					if exists {
						servers = arraySet.Insert(servers, chunkServerMetadata.(*ChunkServerMetadata).ChunkServer)
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
			replication := make([]rpcdefs.ChunkReplication, 0)
			masterService.chunkLocationData.chunkReplication.mutex.Lock()
			for _, chunkId := range chunks {
				chunkReplication := rpcdefs.ChunkReplication{
					Id:      chunkId,
					Servers: masterService.chunkLocationData.chunkReplication.replication[chunkId],
				}
				replication = append(replication, chunkReplication)
			}
			masterService.chunkLocationData.chunkReplication.mutex.Unlock()

			*reply = rpcdefs.WriteChunksReply{
				PrimaryId: primaryId,
				ServersLocation: rpcdefs.ServersLocation{
					Servers:     servers,
					Replication: replication,
				},
				Chunks: chunks,
			}
		}
	})

	return nil
}

func (masterService *MasterService) WriteChunksRPC(request rpcdefs.WriteChunksArgs, reply *rpcdefs.WriteChunksReply) error {
	return masterService.readWriteChunks(WRITE, request, reply)
}

func (masterService *MasterService) ReadChunksRPC(request rpcdefs.ReadChunksArgs, reply *rpcdefs.ReadChunksReply) error {
	return masterService.readWriteChunks(READ, request, reply)
}

var (
	port     = flag.Int("port", 52684, "Server port")
	host     = flag.String("host", "localhost", "Server host")
	folder   = flag.String("folder", "data", "Path to the folder")
	logLevel = flag.String("log", "INFO", "Log level among \"PANIC\", \"FATAL\", \"ERROR\", \"WARN\", \"INFO\", \"DEBUG\", \"TRACE\"")
)

func init() {
	flag.Parse()
	log.Logger = common.InitializeLogger(*logLevel)
}

func main() {
	masterService := new(MasterService)
	masterService.settings = MasterServerSettings{
		Endpoint: common.Endpoint{
			Host: *host,
			Port: *port,
		},
		folder: *folder,
	}
	masterService.defaultReplicationGoal.Store(3)
	masterService.chunkLocationData.chunkReplication.replication = make(map[common.ChunkId][]common.ChunkServerId)
	root := &Directory{
		files: make([]string, 0),
	}
	masterService.namespace.Store("/", root)

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
