package chunkServer

import (
	"Google_File_System/lib/utils"
	"path/filepath"
	"strconv"
)

type Settings struct {
	utils.Endpoint
	Master utils.Endpoint
	Folder string
}

func (settings *Settings) GetChunksFolder() string {
	return filepath.Join(settings.Folder, "chunks")
}

func (settings *Settings) GetChunkPath(id utils.ChunkId) string {
	return filepath.Join(settings.GetChunksFolder(), strconv.Itoa(int(id)))
}

func (settings *Settings) GetServerIdPath() string {
	return filepath.Join(settings.Folder, "Id")
}

func (settings *Settings) GetOplogPath() string {
	return filepath.Join(settings.Folder, "oplog")
}
