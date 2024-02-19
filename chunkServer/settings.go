package chunkServer

import (
	"Google_File_System/utils/common"
	"path/filepath"
	"strconv"
)

type Settings struct {
	common.Endpoint
	Master common.Endpoint
	Folder string
}

func (settings *Settings) GetChunksFolder() string {
	return filepath.Join(settings.Folder, "chunks")
}

func (settings *Settings) GetChunkPath(id common.ChunkId) string {
	return filepath.Join(settings.GetChunksFolder(), strconv.Itoa(int(id)))
}

func (settings *Settings) GetServerIdPath() string {
	return filepath.Join(settings.Folder, "Id")
}

func (settings *Settings) GetOplogPath() string {
	return filepath.Join(settings.Folder, "oplog")
}
