package chunkServer

import (
	"Google_File_System/utils/common"
	"github.com/stretchr/testify/assert"
	"io"
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
	err = EnsureChunk(chunkPath)
	defer os.Remove(chunkPath)
	assert.NoError(t, err)
}

func TestReadWriteChunkBlock(t *testing.T) {
	// Create temporary file with data to write to chunk
	tmpFile, err := os.CreateTemp("", "tmp_chunk_write_data_")
	assert.NoError(t, err)
	defer os.Remove(tmpFile.Name())
	data := generateTestData(2 * BlockSize)
	_, err = tmpFile.Write(data)
	assert.NoError(t, err)
	tmpFile.Seek(0, io.SeekStart) // Reset file position for reading

	// Create chunk file
	mainFile, err := os.CreateTemp("", "chunk_block_")
	assert.NoError(t, err)
	defer os.Remove(mainFile.Name())

	// Check empty chunk size
	size, err := chunkSize(mainFile)
	assert.NoError(t, err)
	assert.EqualValues(t, size, 0, "Result of chunkSize result is not 0")

	// Read empty chunk block
	readData0, err := ReadChunkBlock(mainFile, 0)
	assert.NoError(t, err)
	assert.EqualValues(t, size, 0, "Empty chunk block should have a chunk of size 0")

	// Write chunk block
	checksums := make([]uint32, 2)
	checksums[0], _ = common.Checksum(data[:BlockSize])
	checksums[1], _ = common.Checksum(data[BlockSize : 2*BlockSize])
	err = WriteChunkBlock(tmpFile, mainFile, 0, checksums)
	assert.NoError(t, err)

	// Check chunk size
	size, err = chunkSize(mainFile)
	assert.NoError(t, err)
	assert.EqualValues(t, size, 2*BlockSize, "Result of chunkSize result is not consistent with written data size")

	// Read chunk block
	readData0, err = ReadChunkBlock(mainFile, 0)
	assert.NoError(t, err)
	readData1, err := ReadChunkBlock(mainFile, 1)
	assert.NoError(t, err)

	// Compare read data with original data
	assert.EqualValues(t, append(readData0, readData1...), data, "Read data does not match original data")

	// Read last empty chunk block
	readData0, err = ReadChunkBlock(mainFile, 2)
	assert.NoError(t, err)
	assert.EqualValues(t, len(readData0), 0, "Empty chunk block should have a chunk of size 0")
}
