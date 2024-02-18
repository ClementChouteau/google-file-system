package chunkServer

import (
	"Google_File_System/utils/common"
	"sync"
	"time"
)

type Blocks struct {
	deletionLock sync.RWMutex
	blocks       sync.Map // common.ChunkId => *block.ChunkBlock
	LengthLock   sync.Mutex
	Length       uint32 // TODO double check length management
}

type Chunk struct {
	Id          common.ChunkId
	FileMutex   sync.RWMutex // Protect accesses to the underlying file
	Blocks      Blocks
	LeaseMutex  sync.RWMutex
	lease       time.Time // Time point when we received the lease
	Replication []common.ChunkServer
}

func (chunk *Chunk) GiveLeaseNow(replication []common.ChunkServer) {
	chunk.lease = time.Now()
	chunk.Replication = replication
}

func (chunk *Chunk) HasLease() bool {
	return !chunk.lease.IsZero() && time.Now().Before(chunk.lease.Add(common.LeaseDuration))
}

func (chunk *Chunk) RevokeLease() {
	chunk.lease = time.Time{}
}

func (chunk *Chunk) ReadInMemoryChunk(blocksCache *BlocksCache, offset uint32, length uint32) (data []byte, err error) {
	chunk.Blocks.deletionLock.RLock()
	defer chunk.Blocks.deletionLock.RUnlock()

	data = make([]byte, 0, length)
	startBlockId := offset / BlockSize
	endBlockId := (offset + length - 1) / BlockSize
	for blockId := startBlockId; blockId <= endBlockId; blockId++ {
		var block *ChunkBlock
		block, err = blocksCache.Get(chunk, blockId, ReadBlock)
		if err != nil {
			return
		}
		defer blocksCache.Put(block)

		var readStart int
		if blockId == startBlockId {
			readStart = int(offset % BlockSize)
		}

		readEnd := BlockSize
		if blockId == endBlockId {
			readEnd = int((offset + length) % BlockSize)
		}

		// Only padding
		if readStart >= int(block.Length) {
			data = append(data, make([]byte, readEnd-readStart)...)
		} else {
			validReadEnd := min(readEnd, readStart+int(block.Length))
			data = append(data, block.Data[readStart:validReadEnd]...)

			// Padding at the end
			if readEnd != validReadEnd {
				data = append(data, make([]byte, readEnd-validReadEnd)...)
			}
		}
	}

	return
}

func (chunk *Chunk) WriteInMemoryChunk(blocksCache *BlocksCache, offset uint32, data []byte) (checksums []uint32, err error) {
	chunk.Blocks.deletionLock.RLock()
	defer chunk.Blocks.deletionLock.RUnlock()

	startBlockId := offset / BlockSize
	endBlockId := (offset + uint32(len(data)) - 1) / BlockSize

	checksums = make([]uint32, 0, endBlockId-startBlockId+1)

	for blockId := startBlockId; blockId <= endBlockId; blockId++ {
		var block *ChunkBlock
		// TODO operationId
		block, err = blocksCache.Get(chunk, blockId, WriteBlock)
		if err != nil {
			return
		}
		defer blocksCache.Put(block)

		var writeOffset uint32
		if blockId == startBlockId {
			writeOffset = offset % BlockSize
		}

		var writeStart uint32
		if blockId != startBlockId {
			writeStart = (blockId-startBlockId)*BlockSize - offset%BlockSize
		}

		writeEnd := min((blockId-startBlockId+1)*BlockSize-offset%BlockSize, uint32(len(data)))

		err = block.Write(writeOffset, data[writeStart:writeEnd])
		if err != nil {
			return
		}
		checksums = append(checksums, block.Checksum)
	}

	return
}
