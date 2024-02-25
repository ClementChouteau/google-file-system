package master

import (
	"Google_File_System/utils/arraySet"
	"Google_File_System/utils/common"
	"Google_File_System/utils/rpcdefs"
	"errors"
	"sync"
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

func (chunk *ChunkMetadataMaster) ensureLease(masterService *MasterService) (err error) {
	chunk.LeaseMutex.Lock()
	defer chunk.LeaseMutex.Unlock()
	hasLease := !chunk.Lease.IsZero() && time.Now().Before(chunk.Lease.Add(common.LeaseDuration))
	if !hasLease {
		// Choose one of the chunk servers as the primary
		replication := masterService.ChunkLocationData.ChunkReplication.Replication[chunk.Id]
		if len(replication) == 0 {
			// TODO error case
			return
		}
		primaryServerId := masterService.chooseLeastLeased(replication)
		primaryChunkServerMetadata, exists := masterService.ChunkLocationData.chunkServers.Load(primaryServerId)
		primaryChunkServer := primaryChunkServerMetadata.(*ChunkServerMetadata).ChunkServer

		// Add corresponding servers to the reply
		replicas := arraySet.Remove(replication, primaryServerId)
		servers := make([]common.ChunkServer, 0, len(replicas))
		for _, chunkServerId := range replicas {
			chunkServerMetadata, exists := masterService.ChunkLocationData.chunkServers.Load(chunkServerId)
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
		replicationGoal := masterService.DefaultReplicationGoal.Load()
		chunk.ReplicationGoal = replicationGoal
		servers = masterService.getChunkServersForNewChunk(replicationGoal)

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
