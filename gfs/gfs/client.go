package gfs

import (
	"Google_File_System/utils/common"
	"Google_File_System/utils/rpcdefs"
	"errors"
	"fmt"
	"github.com/google/uuid"
	"log"
	"net/rpc"
	"strconv"
)

type GFSClient struct {
	client *rpc.Client
	// TODO cache of master info
	// TODO pool of rpc.Client ?
}

func New(host string, port int) (*GFSClient, error) {
	endpoint := common.Endpoint{Host: host, Port: port}
	client, err := rpc.Dial("tcp", endpoint.Address())
	if err != nil {
		return nil, err
	}

	return &GFSClient{client: client}, err
}

func (gfsClient *GFSClient) Close() error {
	if gfsClient != nil {
		return gfsClient.client.Close()
	}
	return nil
}

func (gfsClient *GFSClient) Mkdir(path string) error {
	request := rpcdefs.MkdirArgs{
		Path: path,
	}
	var reply struct{}
	return gfsClient.client.Call("MasterService.MkdirRPC", request, &reply)
}

func (gfsClient *GFSClient) Rmdir(path string) error {
	request := rpcdefs.RmdirArgs{
		Path: path,
	}
	var reply struct{}
	return gfsClient.client.Call("MasterService.RmdirRPC", request, &reply)
}

func (gfsClient *GFSClient) Ls(path string) ([]string, error) {
	request := rpcdefs.LsArgs{
		Path: path,
	}
	var reply rpcdefs.LsReply
	err := gfsClient.client.Call("MasterService.LsRPC", request, &reply)
	return reply.Paths, err
}

func (gfsClient *GFSClient) Create(path string) error {
	request := rpcdefs.CreateArgs{
		Path: path,
	}
	var reply struct{}
	return gfsClient.client.Call("MasterService.CreateRPC", request, &reply)
}

func (gfsClient *GFSClient) Delete(path string) error {
	request := rpcdefs.DeleteArgs{
		Path: path,
	}
	var reply struct{}
	return gfsClient.client.Call("MasterService.DeleteRPC", request, &reply)
}

type ChunkRange struct {
	id common.ChunkId
	common.Range[uint32]
}

func generateChunkRanges(offset uint64, length uint64, chunks []common.ChunkId) (chunkRanges []ChunkRange) {
	chunkRanges = make([]ChunkRange, 0, len(chunks))

	n := len(chunks)
	for i, chunkId := range chunks {
		// Offset
		chunkOffset := uint32(0)
		if i == 0 {
			chunkOffset = uint32(offset % common.ChunkSize)
		} else if i == n-1 {
			chunkOffset = uint32((offset + length) % common.ChunkSize)
		}
		// Length
		chunkLength := uint32(common.ChunkSize)
		if i == 0 {
			chunkLength = uint32(min(uint64(common.ChunkSize-chunkOffset), length))
		} else if i == n-1 {
			chunkLength = uint32((offset + length) % common.ChunkSize)
		}

		chunkRanges = append(chunkRanges, ChunkRange{
			id: chunkId,
			Range: common.Range[uint32]{
				Offset: chunkOffset,
				Length: chunkLength,
			},
		})
	}

	return
}

func (gfsClient *GFSClient) Read(path string, offset uint64, length uint64) (data []byte, err error) {
	// Retrieve chunks and servers location
	masterRequest := rpcdefs.ReadChunksArgs{
		Path:   path,
		Offset: offset,
		Length: length,
	}
	masterReply := rpcdefs.ReadChunksReply{}
	err = gfsClient.client.Call("MasterService.ReadChunksRPC", masterRequest, &masterReply)
	if err != nil {
		return
	}

	// Retrieve chunks data in chunk servers
	for _, chunkRange := range generateChunkRanges(offset, length, masterReply.Chunks) {
		serverIds := masterReply.FindReplication(chunkRange.id)
		if serverIds == nil {
			return nil, errors.New("invalid response from master, missing replication info")
		}
		common.Shuffle(*serverIds)
		for _, chunkServerId := range *serverIds {
			chunkRequest := rpcdefs.ReadArgs{
				Id:     chunkRange.id,
				Offset: chunkRange.Offset,
				Length: chunkRange.Length,
			}
			chunkReply := &rpcdefs.ReadReply{}
			server := masterReply.FindServer(chunkServerId)
			if server == nil {
				return nil, errors.New("invalid response from master, missing server info")
			}
			err = server.Endpoint.Call("ChunkService.ReadRPC", chunkRequest, chunkReply)

			if err != nil {
				log.Println("Error: trying to read on a replica", err)
				continue
			}
			data = append(data, chunkReply.Data...)
			break
		}
		if err != nil {
			return nil, errors.New("read failed on all replicas for chunk with id=" + strconv.Itoa(int(chunkRange.id)))
		}
	}

	return data, nil
}

func (gfsClient *GFSClient) Write(path string, offset uint64, data []byte) (err error) {
masterLoop:
	for {
		// Retrieve chunks and servers location
		masterRequest := rpcdefs.WriteChunksArgs{
			Path:   path,
			Offset: offset,
			Length: uint64(len(data)),
		}
		masterReply := rpcdefs.WriteChunksReply{}
		err = gfsClient.client.Call("MasterService.WriteChunksRPC", masterRequest, &masterReply)
		if err != nil {
			return
		}

		// Write data in chunk servers
		for i, chunkRange := range generateChunkRanges(offset, uint64(len(data)), masterReply.Chunks) {
			serverIds := masterReply.FindReplication(chunkRange.id)
			if serverIds == nil {
				return errors.New("invalid response from master, missing replication info")
			}

			var writeStart uint32
			if i != 0 {
				writeStart = uint32(i*common.ChunkSize) - chunkRange.Offset%common.ChunkSize
			}
			writeEnd := min(uint32((i+1)*common.ChunkSize)-chunkRange.Offset%common.ChunkSize, uint32(len(data)))

			// Push data to all servers
			dataRequest := rpcdefs.PushDataArgs{
				Data:    data[writeStart:writeEnd],
				Id:      uuid.New(),
				Servers: masterReply.Servers,
			}
			dataReply := &rpcdefs.PushDataReply{}
			primaryServer := masterReply.FindServer(masterReply.PrimaryId) // TODO We should push to the closest server
			if primaryServer == nil {
				return errors.New("invalid response from master, missing server info")
			}
			err = primaryServer.Endpoint.Call("ChunkService.PushDataRPC", dataRequest, dataReply)

			// Commit on primary
			chunkRequest := rpcdefs.WriteArgs{
				Id:     chunkRange.id,
				DataId: dataRequest.Id,
				Offset: chunkRange.Offset,
			}
			chunkReply := &rpcdefs.WriteReply{}

			err = primaryServer.Endpoint.Call("ChunkService.WriteRPC", chunkRequest, chunkReply)
			if err != nil {
				log.Println(err)
				if rpcdefs.IsNoLeaseError(err) {
					continue masterLoop
				} else {
					return fmt.Errorf("write failed for chunk with id=%d", int(chunkRange.id))
				}
			}
		}
		break
	}

	return nil
}

func (gfsClient *GFSClient) RecordAppend(path string, data []byte) (err error) {
	var nr int
	for {
		// Retrieve primary that holds last chunk location and ensure it has lease
		masterRequest := rpcdefs.RecordAppendChunksArgs{
			Path: path,
			Nr:   nr,
		}
		masterReply := rpcdefs.RecordAppendChunksReply{}
		err = gfsClient.client.Call("MasterService.RecordAppendChunksRPC", masterRequest, &masterReply)
		if err != nil {
			return
		}

		serverIds := masterReply.FindReplication(masterReply.Id)
		if serverIds == nil {
			return errors.New("invalid response from master, missing replication info")
		}

		// Push data to all servers
		dataRequest := rpcdefs.PushDataArgs{
			Data:    data,
			Id:      uuid.New(),
			Servers: masterReply.Servers,
		}
		dataReply := rpcdefs.PushDataReply{}
		primaryServer := masterReply.FindServer(masterReply.PrimaryId) // TODO We should push to the closest server
		if primaryServer == nil {
			return errors.New("invalid response from master, missing server info")
		}
		err = primaryServer.Endpoint.Call("ChunkService.PushDataRPC", dataRequest, &dataReply)

		// Try to record append in this chunk
		chunkRequest := rpcdefs.RecordAppendArgs{
			Id:     masterReply.Id,
			DataId: dataRequest.Id,
		}
		chunkReply := rpcdefs.RecordAppendReply{}
		err = primaryServer.Endpoint.Call("ChunkService.RecordAppendRPC", chunkRequest, &chunkReply)
		if err != nil {
			if rpcdefs.IsNoLeaseError(err) {
				err = nil
				continue
			}
			return
		}

		if chunkReply.Done {
			break
		}

		nr = masterRequest.Nr + 1
	}

	return nil
}
