package chunkServer

import (
	"Google_File_System/lib/utils"
	"errors"
	"os"
	"sync"
	"time"
)

type Chunk struct {
	Id            utils.ChunkId
	Lock          sync.RWMutex
	Version       utils.ChunkVersion
	VersionOffset int64
	LeaseMutex    sync.RWMutex
	lease         time.Time // Time point when we received the lease
	Replication   []utils.ChunkServer
}

func (chunk *Chunk) Ensure(path string) (err error) {
	file, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE, 0644)
	if err != nil {
		return err
	}
	defer file.Close()
	return
}

func (chunk *Chunk) GiveLeaseNow(replication []utils.ChunkServer) {
	chunk.lease = time.Now()
	chunk.Replication = replication
}

func (chunk *Chunk) HasLease() bool {
	return !chunk.lease.IsZero() && time.Now().Before(chunk.lease.Add(utils.LeaseDuration))
}

func (chunk *Chunk) RevokeLease() {
	chunk.lease = time.Time{}
}

func (chunk *Chunk) Read(path string, offset uint32, length uint32) (data []byte, err error) {
	file, err := os.OpenFile(path, os.O_RDONLY, 0644)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	chunkSize, err := ChunkSize(file)
	if err != nil {
		return nil, err
	}
	if offset+length > chunkSize {
		return nil, errors.New("reading past the end of chunk")
	}

	start := offset / BlockSize
	end := (offset + length - 1) / BlockSize

	data, err = ReadChunkBlocks(file, start, end)
	if err != nil {
		return nil, err
	}

	return data[offset%BlockSize : min(offset%BlockSize+length, uint32(len(data)))], nil
}

func (chunk *Chunk) Write(path string, offset uint32, data []byte) (err error) {
	file, err := os.OpenFile(path, os.O_RDWR, 0644)
	if err != nil {
		return err
	}
	defer file.Close()

	err = WriteChunkBlocks(file, offset, data)
	return
}

func (chunk *Chunk) Append(path string, data []byte) (padding bool, offset uint32, err error) {
	var file *os.File
	file, err = os.OpenFile(path, os.O_RDWR, 0644)
	if err != nil {
		return
	}
	defer file.Close()

	var size uint32
	size, err = ChunkSize(file)
	if err != nil {
		return
	}

	var length uint32
	if size+uint32(len(data)) < utils.ChunkSize {
		padding = false
		offset = size
		size += uint32(len(data))
		length = size
	} else {
		padding = true
		length = utils.ChunkSize - size
		size = utils.ChunkSize // Padding
	}

	if padding {
		data = make([]byte, length)
	}

	err = WriteChunkBlocks(file, offset, data)
	return
}
