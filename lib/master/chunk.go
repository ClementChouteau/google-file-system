package master

import (
	"Google_File_System/lib/utils"
	"errors"
	"sync"
	"time"
)

type Lease struct {
	Mutex     sync.Mutex
	GrantTime time.Time // Time point when sending the lease
	Primary   utils.ChunkServerId
}

func (lease *Lease) grant(primary utils.ChunkServerId) {
	lease.Primary = primary
	lease.GrantTime = time.Now()
}

func (lease *Lease) has() bool {
	return lease.GrantTime.IsZero() && time.Now().Before(lease.GrantTime.Add(utils.LeaseDuration))
}

type Chunk struct {
	Id              utils.ChunkId
	Initialized     bool // Uninitialized chunks can be parts of sparse files, they have no replication
	ReplicationGoal uint32
	Lease           Lease
}

func (chunk *Chunk) ensureLease(masterService *MasterService) (utils.ChunkServerId, error) {
	chunk.Lease.Mutex.Lock()
	defer chunk.Lease.Mutex.Unlock()
	hasLease := chunk.Lease.has()
	if !hasLease {
		// Choose one of the chunk servers as the primary
		replication := masterService.ChunkLocationData.ChunkReplication.Replication[chunk.Id]
		if len(replication) == 0 {
			return 0, errors.New("no servers replicating the chunk")
		}
		primaryServerId := masterService.ChunkLocationData.chooseLeastLeased(replication)
		value, exists := masterService.ChunkLocationData.chunkServers.Load(primaryServerId)
		primaryChunkServer := value.(*ChunkServerMetadata)

		// Add corresponding servers to the reply
		replicas := utils.Remove(replication, primaryServerId)
		servers := make([]utils.ChunkServer, 0, len(replicas))
		for _, chunkServerId := range replicas {
			chunkServerMetadata, exists := masterService.ChunkLocationData.chunkServers.Load(chunkServerId)
			if exists {
				servers = utils.Insert(servers, chunkServerMetadata.(*ChunkServerMetadata).ChunkServer)
			}
		}

		if exists {
			// Grant lease to this new primary
			request := utils.GrantLeaseArgs{
				ChunkId:     chunk.Id,
				Replication: servers,
			}
			err := primaryChunkServer.Endpoint.Call("ChunkService.GrantLeaseRPC", request, &utils.GrantLeaseReply{})
			if err != nil {
				return 0, err
			}
			chunk.Lease.grant(primaryServerId)
			primaryChunkServer.leaseCount.Add(1)
		} else {
			return 0, errors.New("server not found")
		}
	}
	return chunk.Lease.Primary, nil
}

func (chunk *Chunk) ensureInitialized(masterService *MasterService) (servers []utils.ChunkServerId) {
	if !chunk.Initialized {
		// TODO lock initialization
		chunk.Initialized = true
		replicationGoal := masterService.Settings.DefaultReplicationGoal
		chunk.ReplicationGoal = replicationGoal
		servers = masterService.ChunkLocationData.getChunkServersForNewChunk(replicationGoal)

		// Indicate that they replicate the selected chunks
		masterService.ChunkLocationData.ChunkReplication.mutex.Lock()
		masterService.ChunkLocationData.ChunkReplication.Replication[chunk.Id] = servers
		masterService.ChunkLocationData.ChunkReplication.mutex.Unlock()
	} else {
		masterService.ChunkLocationData.ChunkReplication.mutex.Lock()
		servers = masterService.ChunkLocationData.ChunkReplication.Replication[chunk.Id]
		masterService.ChunkLocationData.ChunkReplication.mutex.Unlock()
	}
	return
}
