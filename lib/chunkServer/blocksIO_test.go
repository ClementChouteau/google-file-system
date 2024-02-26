package chunkServer

import (
	"github.com/stretchr/testify/assert"
	"os"
	"path"
	"testing"
)

func generateTestData(size int) []byte {
	testData := make([]byte, size)
	for i := 0; i < len(testData); i++ {
		testData[i] = byte(i % 256)
	}
	return testData
}

func TestEnsureChunk(t *testing.T) {
	dir, err := os.MkdirTemp("", "tmp_chunks_")
	defer os.Remove(dir)
	assert.NoError(t, err)

	chunkPath := path.Join(dir, "chunk")
	_, err = os.Create(chunkPath)
	defer os.Remove(chunkPath)
	assert.NoError(t, err)
}

func TestReadWriteChunkBlock(t *testing.T) {
	// Create chunk file
	mainFile, err := os.CreateTemp("", "chunk_block_")
	assert.NoError(t, err)
	defer os.Remove(mainFile.Name())

	// Check empty chunk size
	size, err := ChunkSize(mainFile)
	assert.NoError(t, err)
	assert.EqualValues(t, size, 0, "Result of ChunkSize result is not 0")

	// Read empty chunk block
	readData0, err := ReadChunkBlocks(mainFile, 0, 0)
	assert.NoError(t, err)
	assert.EqualValues(t, size, 0, "Empty chunk block should have a chunk of size 0")

	// Write chunk block
	data := generateTestData(2 * BlockSize)
	err = WriteChunkBlocks(mainFile, 0, data)
	assert.NoError(t, err)

	// Check chunk size
	size, err = ChunkSize(mainFile)
	assert.NoError(t, err)
	assert.EqualValues(t, size, 2*BlockSize, "Result of ChunkSize result is not consistent with written data size")

	// Read chunk block
	readData0, err = ReadChunkBlocks(mainFile, 0, 0)
	assert.NoError(t, err)
	readData1, err := ReadChunkBlocks(mainFile, 1, 1)
	assert.NoError(t, err)

	// Compare read data with original data
	assert.EqualValues(t, append(readData0, readData1...), data, "Read data does not match original data")

	// Read last empty chunk block
	readData0, err = ReadChunkBlocks(mainFile, 2, 2)
	assert.NoError(t, err)
	assert.EqualValues(t, len(readData0), 0, "Empty chunk block should have a chunk of size 0")
}
