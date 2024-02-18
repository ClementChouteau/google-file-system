package chunkServer

import (
	"Google_File_System/utils/common"
	"os"
	"sync"
)

// TODO add BufferPool for block []byte of BlockSize

const (
	ReadBlock = iota
	WriteBlock
)

// TODO how can read be consistent ?
// TODO client gets chunk version
// TODO => need MVCC
// TODO => each replica has [version]=>block
// TODO this is mandatory as replicas can't increase version all at once

// TODO stale replica detection: the master maintains the chunk version number to distinguish between up-to-date and stale replicas.
// TODO The chunk version is updated when new lease is granted.
//
// TODO Master failure or chunkserver failure leave the chunk versions locally to be stale and handled accordingly. The client also verifies that it has the latest chunk version no from a replica.

type ChunkBlock struct {
	initialization sync.Once
	mutex          sync.RWMutex
	operation      int // ReadBlock or WriteBlock
	version        uint32
	dirty          bool   // Changes are maybe present in oplog but not in disk block
	Length         uint32 // Useful for sparse blocks
	Checksum       uint32
	Data           []byte // Has a maximum size of blockUtils.BlockSize
}

func (chunkBlock *ChunkBlock) Write(offset uint32, data []byte) (err error) {
	chunkBlock.Length = max(chunkBlock.Length, offset+uint32(len(data)))
	chunkBlock.Data = common.ResizeSlice(chunkBlock.Data, int(chunkBlock.Length))
	chunkBlock.dirty = true
	copy(chunkBlock.Data[offset:], data)

	chunkBlock.Checksum, err = common.Checksum(chunkBlock.Data)
	return
}

type BlocksCache struct {
	settings *Settings
	blocks   sync.Map // uint64 => *ChunkBlock
	// TODO need to know usage count
}

func NewBlocksCache(settings *Settings) *BlocksCache {
	return &BlocksCache{
		settings: settings,
	}
}

func (blockCache *BlocksCache) get(chunk *Chunk, blockId uint32) (chunkBlock *ChunkBlock, err error) {
	var file *os.File

	value, _ := blockCache.blocks.LoadOrStore(blockId, &ChunkBlock{})
	chunkBlock = value.(*ChunkBlock)
	chunkBlock.initialization.Do(func() {
		if file == nil {
			file, err = os.Open(blockCache.settings.GetChunkPath(chunk.Id))
			if err != nil {
				return
			}
			defer file.Close()
		}

		// TODO hold lock time necessary to read all blocks
		chunk.FileMutex.RLock()
		defer chunk.FileMutex.RUnlock() // Keep the locks on the region
		chunkBlock.Data, err = ReadChunkBlock(file, blockId)
	})

	return
}

func (blockCache *BlocksCache) Get(chunk *Chunk, blockId uint32, operation int) (chunkBlock *ChunkBlock, err error) {
	chunkBlock, err = blockCache.get(chunk, blockId)
	if operation == ReadBlock {
		chunkBlock.mutex.RLock()
	}
	if operation == WriteBlock {
		chunkBlock.mutex.Lock()
		chunkBlock.dirty = true
	}
	chunkBlock.operation = operation
	return
}

func (blockCache *BlocksCache) Put(chunkBlock *ChunkBlock) {
	if chunkBlock.operation == ReadBlock {
		chunkBlock.mutex.RUnlock()
	}
	if chunkBlock.operation == WriteBlock {
		chunkBlock.mutex.Unlock()
	}
}
