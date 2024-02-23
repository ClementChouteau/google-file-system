package chunkServer

import (
	"Google_File_System/utils/common"
	"encoding/binary"
	"errors"
	"io"
	"os"
)

const BlockSize = 64 * 1024

const chunkChecksumsSize = (common.ChunkSize / BlockSize) * 4

const chunkMetadataSize = chunkChecksumsSize

// ChunkSize returns the size of the data stored in the chunk
func ChunkSize(file *os.File) (size uint32, err error) {
	var fileInfo os.FileInfo
	fileInfo, err = file.Stat()
	if err != nil {
		return
	}

	if fileInfo.Size() < chunkMetadataSize {
		size = 0
	} else {
		size = uint32(fileInfo.Size() - chunkMetadataSize)
	}

	return
}

func ReadChunkBlocks(file *os.File, start uint32, end uint32) (data []byte, err error) {
	chunkSize, err := ChunkSize(file)
	if err != nil {
		return nil, err
	}

	if chunkSize == 0 {
		data = make([]byte, 0, BlockSize*(end-start+1))
		return
	}

	// Read block checksum
	_, err = file.Seek(int64(start)*4, io.SeekStart)
	if err != nil {
		return
	}

	storedChecksums := make([]uint32, end-start+1)
	err = binary.Read(file, binary.BigEndian, &storedChecksums)
	if err != nil {
		return
	}

	// Read data
	_, err = file.Seek(chunkMetadataSize+int64(start*BlockSize), io.SeekStart)
	if err != nil {
		return
	}

	data = make([]byte, BlockSize*(end-start+1))
	n, err := io.ReadFull(file, data)
	if err != nil && !errors.Is(err, io.ErrUnexpectedEOF) && !errors.Is(err, io.EOF) {
		return nil, err
	}
	data = data[:n]

	// Checksum data
	for i, storedChecksum := range storedChecksums {
		checksum, err := common.Checksum(data[i*BlockSize : min((i+1)*BlockSize, len(data))])
		if err != nil {
			return nil, err
		}
		if checksum != storedChecksum {
			return nil, errors.New("corrupted block")
		}
	}

	return data, nil
}

func WriteChunkBlocks(file *os.File, offset uint32, data []byte) (err error) {
	start := offset / BlockSize
	end := (offset + uint32(len(data)) - 1) / BlockSize

	// Read data
	var storedData []byte
	storedData, err = ReadChunkBlocks(file, start, end) // TODO we could read only start and end blocks
	if err != nil {
		return
	}

	// Write new data in buffer
	storedData = storedData[:max(int(offset%BlockSize)+len(data), len(storedData))]
	copy(storedData[offset%BlockSize:], data)

	// Compute new checksums
	checksums := make([]uint32, end-start+1)
	for i := start; i <= end; i++ {
		checksums[i-start], err = common.Checksum(storedData[i*BlockSize : min((i+1)*BlockSize, uint32(len(storedData)))])
		if err != nil {
			return
		}
	}

	// Write new checksums
	_, err = file.Seek(int64(start)*4, io.SeekStart)
	if err != nil {
		return
	}

	err = binary.Write(file, binary.BigEndian, checksums)
	if err != nil {
		return
	}

	// Write new data
	_, err = file.Seek(int64(chunkMetadataSize+offset), io.SeekStart)
	if err != nil {
		return
	}

	_, err = file.Write(data)
	if err != nil {
		return
	}

	return
}
