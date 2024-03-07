package chunkServer

import (
	"Google_File_System/lib/utils"
	"bytes"
	"encoding/binary"
	"github.com/rs/zerolog/log"
	"io"
	"os"
	"sync"
)

type ChunkVersions struct {
	lock sync.RWMutex
	file *os.File
}

type Version struct {
	version utils.ChunkVersion
	offset  int64
}

func readChunkVersionsFromDisk(path string) (file *os.File, versions map[utils.ChunkId]Version, err error) {
	file, err = os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		return
	}

	// Truncate to multiple of 12 to make future writes valid
	var fileInfo os.FileInfo
	fileInfo, err = file.Stat()
	if err != nil {
		return
	}

	err = file.Truncate((fileInfo.Size() / 8) * 8)
	if err != nil {
		return
	}

	// Read and parse versions
	var data []byte
	data, err = os.ReadFile(path)

	count := int64(0)
	reader := bytes.NewReader(data)
	versions = make(map[utils.ChunkId]Version)
	for {
		var chunkId utils.ChunkId
		if err = binary.Read(reader, binary.BigEndian, &chunkId); err != nil {
			break
		}

		var version utils.ChunkVersion
		if err = binary.Read(reader, binary.BigEndian, &version); err != nil {
			break
		}

		offset := count * 8

		versions[chunkId] = Version{
			version: version,
			offset:  offset,
		}
		count++
	}

	log.Debug().Msgf("found %d chunk version(s) on disk", count)

	return file, versions, nil
}

func (chunkVersions *ChunkVersions) NextAvailableOffset() (offset int64, err error) {
	offset, err = chunkVersions.file.Seek(0, io.SeekEnd)
	return
}

func (chunkVersions *ChunkVersions) Set(chunkId utils.ChunkId, version utils.ChunkVersion, offset int64) error {
	_, err := chunkVersions.file.Seek(offset, io.SeekStart)
	if err != nil {
		return err
	}

	err1 := binary.Write(chunkVersions.file, binary.BigEndian, chunkId)
	if err1 != nil {
		log.Error().Err(err1).Msgf("writing version (chunk id field) for chunk id %d", chunkId)
	}
	err2 := binary.Write(chunkVersions.file, binary.BigEndian, version)
	if err2 != nil {
		log.Error().Err(err2).Msgf("writing version (version field) for chunk id %d", chunkId)
	}

	if err1 != nil {
		return err1
	}
	if err2 != nil {
		return err2
	}
	return nil
}
