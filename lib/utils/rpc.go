package utils

import (
	"errors"
	"github.com/google/uuid"
)

type RegisterArgs = Endpoint
type RegisterReply = ChunkServerId

type HeartBeatArgs struct {
	Id     ChunkServerId
	Chunks []ChunkId
}
type HeartBeatReply struct{}

type MkdirArgs struct {
	Path string
}
type MkdirReply struct{}

type RmdirArgs struct {
	Path string
}
type RmdirReply struct{}

type LsArgs struct {
	Path string
}
type LsReply struct {
	Paths []string
}

type CreateArgs struct {
	Path string
}
type CreateReply struct{}

type DeleteArgs struct {
	Path string
}
type DeleteReply struct{}

type ReadArgs struct {
	Id     ChunkId
	Offset uint32
	Length uint32
}
type ReadReply struct {
	Data []byte
}

type GrantLeaseArgs struct {
	ChunkId     ChunkId
	Replication []ChunkServer // Non-primary servers replicating this chunk
}
type GrantLeaseReply struct{}

type RevokeLeaseArgs struct {
	ChunkId ChunkId
}
type RevokeLeaseReply struct{}

type ServersLocation struct {
	Servers     []ChunkServer
	Replication []ChunkReplication
}

type PushDataArgs struct {
	Data    []byte
	Id      uuid.UUID     // Identification to be able to refer to this data later
	Servers []ChunkServer // Chain of servers (in the order of push)
}
type PushDataReply struct{}

type ApplyWriteArgs = WriteArgs
type ApplyWriteReply = WriteReply

type WriteArgs struct {
	Id     ChunkId
	DataId uuid.UUID
	Offset uint32
}
type WriteReply struct{}

type RecordAppendArgs struct {
	Id     ChunkId
	DataId uuid.UUID
}
type RecordAppendReply struct {
	Done bool   // Not enough space in the current chunk
	Pos  uint32 // Position of the beginning of the appended data
}

type ChunkReplication struct {
	Id      ChunkId
	Servers []ChunkServerId
}

type ReadWriteChunks struct {
	Path   string
	Offset uint64
	Length uint64
}

func (serversLocation *ServersLocation) FindServer(id ChunkServerId) *ChunkServer {
	for _, server := range serversLocation.Servers {
		if server.Id == id {
			return &server
		}
	}
	return nil
}

func (serversLocation *ServersLocation) FindReplication(id ChunkId) *[]ChunkServerId {
	for _, replication := range serversLocation.Replication {
		if replication.Id == id {
			return &replication.Servers
		}
	}
	return nil
}

type ChunksAndServersLocation struct {
	Chunks    []ChunkId // Chunks of the file in order
	PrimaryId ChunkServerId
	ServersLocation
}

type WriteChunksArgs = ReadWriteChunks
type WriteChunksReply = ChunksAndServersLocation

type ReadChunksArgs = ReadWriteChunks
type ReadChunksReply = ChunksAndServersLocation

type RecordAppendChunksArgs struct {
	Path string
	Nr   int // Give at least the Nr chunk of the file or the last chunk of the file
}
type RecordAppendChunksReply struct {
	Nr        int
	Id        ChunkId
	PrimaryId ChunkServerId
	ServersLocation
}

type NoLeaseError struct {
	Message string
}

func (err *NoLeaseError) Error() string {
	if err != nil {
		return err.Message
	}
	return "no lease"
}

func IsNoLeaseError(err error) bool {
	var noLeaseError *NoLeaseError
	ok := errors.As(err, &noLeaseError)
	return ok
}
