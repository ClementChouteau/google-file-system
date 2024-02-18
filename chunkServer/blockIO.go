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

// Size of the data stored in the chunk
func chunkSize(file *os.File) (size uint32, err error) {
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

func EnsureChunk(path string) (err error) {
	var file *os.File
	file, err = os.OpenFile(path, os.O_RDONLY|os.O_CREATE, 0644)
	if err != nil {
		return
	}
	err = file.Sync()
	if err != nil {
		return
	}
	err = file.Close()
	if err != nil {
		return
	}
	return
}

func ReadChunkBlock(file *os.File, block uint32) (data []byte, err error) {
	// Check if we don't try to read past the end of chunk
	chunkSize, err := chunkSize(file)
	if err != nil {
		return nil, err
	}
	if block*BlockSize > chunkSize {
		return nil, errors.New("reading past the end of chunk")
	}

	if chunkSize == 0 {
		data = make([]byte, 0, BlockSize)
		return
	}

	// Read block checksum
	_, err = file.Seek(int64(block)*4, io.SeekStart)
	if err != nil {
		return
	}

	var storedChecksum uint32
	err = binary.Read(file, binary.BigEndian, &storedChecksum)
	if err != nil {
		return nil, err
	}

	// Read data
	_, err = file.Seek(chunkMetadataSize+int64(block*BlockSize), io.SeekStart)
	if err != nil {
		return
	}

	data = make([]byte, BlockSize)
	blockSize, err := io.ReadFull(file, data)
	if err != nil && !errors.Is(err, io.ErrUnexpectedEOF) && !errors.Is(err, io.EOF) {
		return nil, errors.New("corrupted block")
	}
	data = data[:blockSize]

	// Checksum data
	checksum, err := common.Checksum(data)
	if err != nil {
		return nil, err
	}
	if checksum != storedChecksum {
		return nil, errors.New("corrupted block")
	}

	return data, nil
}

func WriteChunkBlock(tmp *os.File, file *os.File, offset uint32, checksums []uint32) (err error) {
	block := offset / BlockSize

	// Write new checksums
	_, err = file.Seek(int64(block)*4, io.SeekStart)
	if err != nil {
		return
	}

	err = binary.Write(file, binary.BigEndian, checksums)
	if err != nil {
		return
	}

	// Copy data
	_, err = file.Seek(int64(chunkMetadataSize+block*BlockSize+offset%BlockSize), io.SeekStart)
	if err != nil {
		return
	}

	_, err = io.Copy(file, tmp)

	err = file.Sync()

	return
}
