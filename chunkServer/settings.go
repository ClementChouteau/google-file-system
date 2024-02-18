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

func (settings *Settings) GetTemporaryFilesFolder() string {
	return filepath.Join(settings.Folder, "tmp")
}

func (settings *Settings) GetTemporaryFilePath(name string) string {
	return filepath.Join(settings.GetTemporaryFilesFolder(), name)
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
