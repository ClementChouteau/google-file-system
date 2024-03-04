package master

import (
	"Google_File_System/lib/utils"
	"errors"
	"strings"
	"sync"
)

type Directory struct {
	mutex sync.RWMutex
	Files []string
}

type File struct {
	mutex  sync.RWMutex
	chunks []*Chunk // consecutive
}

func (file *File) appendUninitializedChunk(chunkId utils.ChunkId) {
	uninitializedChunk := &Chunk{
		Id:              chunkId,
		Initialized:     false,
		ReplicationGoal: 0,
	}

	file.chunks = append(file.chunks, uninitializedChunk)
}

// iterate file needs to be locked before
func (file *File) iterate(masterService *MasterService, startChunkNr int, endChunkNr int, f func(chunk *Chunk) bool) {
	// Ensure that we have uninitialized chunks
	for len(file.chunks) < endChunkNr+1 {
		chunkId := masterService.nextAvailableChunkId.Add(1) - 1
		file.appendUninitializedChunk(chunkId)
	}

	// Initialize chunks that will be written to
	for i := startChunkNr; i <= endChunkNr; i++ {
		chunk := file.chunks[i]
		if !f(chunk) {
			break
		}
	}
}

func NewNamespace() (namespace *Namespace) {
	namespace = new(Namespace)
	root := &Directory{
		Files: make([]string, 0),
	}
	namespace.items.Store("/", root)
	return
}

type Namespace struct {
	items sync.Map // string => *Directory | *File
}

// lockAncestors check ancestors and read lock all items in the path excluding the last one
// Then either returns an error or calls f(value) which can be either *Directory | *File
func (namespace *Namespace) lockAncestors(path string, f func(value any) error) error {
	// TODO simplify parts logic
	var parts []string
	if path == "/" {
		parts = append(parts, "")
	} else {
		parts = strings.Split(path, "/")
	}
	ancestor := ""
	n := len(parts)
	for i := 0; i < n; i++ {
		if ancestor != "/" {
			ancestor += "/"
		}
		ancestor += parts[i]

		value, exists := namespace.items.Load(ancestor)
		if !exists {
			break
		}

		if i == n-1 {
			return f(value)
		}

		switch fileSystemEntry := value.(type) {
		case *File:
			return errors.New("an ancestor in the path is a file " + ancestor)
		case *Directory:
			fileSystemEntry.mutex.RLock()
			defer fileSystemEntry.mutex.RUnlock()
		}
	}

	return errors.New("a parent directory does not exist " + ancestor)
}

// LockFileAncestors does not lock the file itself, only its ancestors
func (namespace *Namespace) LockFileAncestors(path string, f func(file *File)) error {
	return namespace.lockAncestors(path, func(value any) error {
		value, exists := namespace.items.Load(path)
		// Check if the file exists
		if !exists {
			return errors.New("no such file or directory")
		}

		switch fileSystemEntry := value.(type) {
		case *Directory:
			return errors.New("trying to read a directory")
		case *File:
			f(fileSystemEntry)
		}
		return nil
	})
}

func (namespace *Namespace) Mkdir(path string) error {
	directory := parentPath(path)
	return namespace.lockAncestors(directory, func(value any) error {
		switch fileSystemEntry := value.(type) {
		case *File:
			return errors.New("parent directory is a file")
		case *Directory:
			// Write lock parent directory
			fileSystemEntry.mutex.Lock()
			defer fileSystemEntry.mutex.Unlock()

			// Create new directory if it does not already exist
			newDirectory := &Directory{
				Files: make([]string, 0),
			}
			if _, exists := namespace.items.LoadOrStore(path, newDirectory); exists {
				return errors.New("directory already exists")
			}

			// Insert new directory in parent directory
			fileSystemEntry.Files = append(fileSystemEntry.Files, path)
		}
		return nil
	})
}

func (namespace *Namespace) Rmdir(path string) error {
	directory := parentPath(path)
	return namespace.lockAncestors(directory, func(value any) error {
		switch fileSystemEntry := value.(type) {
		case *File:
			return errors.New("parent directory is a file")
		case *Directory:
			// Write lock parent directory
			fileSystemEntry.mutex.Lock()
			defer fileSystemEntry.mutex.Unlock()

			// Check that directory to remove exists and is empty
			value, exists := namespace.items.Load(path)
			if !exists {
				return errors.New("no such directory")
			}

			switch fileSystemEntry := value.(type) {
			case *File:
				return errors.New("can't remove file")
			case *Directory:
				fileSystemEntry.mutex.RLock()
				defer fileSystemEntry.mutex.RUnlock()

				if len(fileSystemEntry.Files) != 0 {
					return errors.New("directory is not empty")
				}

				namespace.items.Delete(path)
			}

			// Remove directory from parent directory
			fileSystemEntry.Files = utils.Remove(fileSystemEntry.Files, path)
		}
		return nil
	})
}

func (namespace *Namespace) Ls(path string) (paths []string, err error) {
	return paths, namespace.lockAncestors(path, func(value any) error {
		switch fileSystemEntry := value.(type) {
		case *File:
			paths = make([]string, 1)
			paths[0] = path
			break
		case *Directory:
			// Read lock directory
			fileSystemEntry.mutex.RLock()
			defer fileSystemEntry.mutex.RUnlock()
			paths = fileSystemEntry.Files
		}
		return nil
	})
}

func (namespace *Namespace) Create(path string) error {
	directory := parentPath(path)
	return namespace.lockAncestors(directory, func(value any) error {
		switch fileSystemEntry := value.(type) {
		case *File:
			return errors.New("parent directory is a file")
		case *Directory:
			// Write lock parent directory
			fileSystemEntry.mutex.Lock()
			defer fileSystemEntry.mutex.Unlock()

			// Creating the file
			file := &File{
				chunks: make([]*Chunk, 0),
			}
			_, exists := namespace.items.LoadOrStore(path, file)
			if exists {
				return errors.New("file already exists")
			}

			// Inserting the file in its parent directory
			fileSystemEntry.Files = append(fileSystemEntry.Files, path)
		}
		return nil
	})
}

func (namespace *Namespace) Delete(path string) (file *File, err error) {
	directory := parentPath(path)
	return file, namespace.lockAncestors(directory, func(value any) error {
		switch fileSystemEntry := value.(type) {
		case *File:
			return errors.New("parent directory is a file")
		case *Directory:
			// Write lock parent directory
			fileSystemEntry.mutex.Lock()
			defer fileSystemEntry.mutex.Unlock()

			// Check if file to remove is indeed a file and exists
			value, exists := namespace.items.Load(path)
			if !exists {
				return errors.New("no such file")
			}
			switch fileSystemEntry := value.(type) {
			case *File:
				file = fileSystemEntry
				// Remove file
				namespace.items.Delete(path)
			case *Directory:
				return errors.New("file to delete is a directory")
			}

			// Remove file from its parent directory
			fileSystemEntry.Files = utils.Remove(fileSystemEntry.Files, path)
		}
		return nil
	})
}
