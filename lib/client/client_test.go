package client

import (
	"Google_File_System/lib/chunkServer"
	"crypto/rand"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestMkdir(t *testing.T) {
	client, err := New("localhost", 52684)
	assert.NoError(t, err)
	defer client.Close()

	assert.Error(t, client.Mkdir(""), "Path must start with root")
	assert.Error(t, client.Mkdir("/"), "Cannot create root directory")
	assert.NoError(t, client.Mkdir("/mkdir"))
	assert.Error(t, client.Mkdir("/mkdir/"), "Trying to create an already existing directory")
	assert.NoError(t, client.Mkdir("/mkdir2/"))
}

func TestRmdir(t *testing.T) {
	client, err := New("localhost", 52684)
	assert.NoError(t, err)
	defer client.Close()

	assert.Error(t, client.Rmdir(""), "Cannot remove empty path")
	assert.Error(t, client.Rmdir("/"), "Cannot remove root directory")
	assert.Error(t, client.Rmdir("/rmdir2"), "Cannot remove non-existing directory")

	assert.NoError(t, client.Mkdir("/rmdir"))
	assert.NoError(t, client.Rmdir("/rmdir"))

	assert.NoError(t, client.Mkdir("/rmdir_root"))
	assert.NoError(t, client.Mkdir("/rmdir_root/rmdir"))
	assert.Error(t, client.Rmdir("/rmdir_root"), "Cannot remove non empty directory")
	assert.NoError(t, client.Rmdir("/rmdir_root/rmdir"), "Removing nested directory")
}

func TestCreate(t *testing.T) {
	client, err := New("localhost", 52684)
	assert.NoError(t, err)
	defer client.Close()

	assert.NoError(t, client.Create("/a"))
	assert.Error(t, client.Create("/a"), "File already exists")
	assert.NoError(t, client.Mkdir("/create"))
	assert.NoError(t, client.Create("/create/b"))
	assert.Error(t, client.Create("/create/b"), "File already exists")
	assert.NoError(t, client.Mkdir("/create/nested"))
	assert.NoError(t, client.Create("/create/nested/c"))
}

func TestDelete(t *testing.T) {
	client, err := New("localhost", 52684)
	assert.NoError(t, err)
	defer client.Close()

	assert.NoError(t, client.Create("/x"))
	assert.NoError(t, client.Create("/y"))
	assert.NoError(t, client.Delete("/x"))
	assert.Error(t, client.Delete("/e"))

	assert.NoError(t, client.Mkdir("/delete"))
	assert.NoError(t, client.Create("/delete/y"))
	assert.NoError(t, client.Create("/delete/z"))
	assert.NoError(t, client.Delete("/delete/y"))
}

func TestLs(t *testing.T) {
	client, err := New("localhost", 52684)
	assert.NoError(t, err)
	defer client.Close()

	assert.NoError(t, client.Mkdir("/u"))
	assert.NoError(t, client.Mkdir("/v"))
	assert.NoError(t, client.Mkdir("/ls"))

	paths, err := client.Ls("/")
	assert.NoError(t, err)
	for _, path := range []string{"/u", "/v", "/ls"} {
		assert.Contains(t, paths, path)
	}

	paths, err = client.Ls("/ls")
	assert.NoError(t, err)
	assert.ElementsMatch(t, paths, []string{})

	_, err = client.Ls("/not_existing")
	assert.Error(t, err)

	assert.NoError(t, client.Mkdir("/ls/w"))
	assert.NoError(t, client.Mkdir("/ls/p"))

	paths, err = client.Ls("/ls")
	assert.NoError(t, err)
	for _, path := range []string{"/ls/w", "/ls/p"} {
		assert.Contains(t, paths, path)
	}

	_, err = client.Ls("/ls/not_existing")
	assert.Error(t, err)
}

func TestWrite(t *testing.T) {
	client, err := New("localhost", 52684)
	assert.NoError(t, err)
	defer client.Close()
	assert.NoError(t, client.Mkdir("/write"))
	assert.NoError(t, client.Mkdir("/write/test"))
	assert.NoError(t, client.Create("/write/test/file"))

	assert.NoError(t, client.Write("/write/test/file", 0, []byte("Nice")))
	data, err := client.Read("/write/test/file", 0, uint64(len("Nice")))
	assert.NoError(t, err)
	assert.EqualValues(t, "Nice", string(data))

	assert.NoError(t, client.Write("/write/test/file", uint64(len("Nice")), []byte(" Job!")))
	data, err = client.Read("/write/test/file", 0, uint64(len("Nice Job!")))
	assert.NoError(t, err)
	assert.EqualValues(t, "Nice Job!", string(data))
}

func TestMultiBlocksWrite(t *testing.T) {
	client, err := New("localhost", 52684)
	assert.NoError(t, err)
	defer client.Close()
	assert.NoError(t, client.Create("/multiBlocks"))

	length := 10 * chunkServer.BlockSize // This write will be over multiple blocks
	expectedFinalResults := make([]byte, length)

	// Writing a large region
	randomBytes := make([]byte, length)
	_, err = rand.Read(randomBytes)
	assert.NoError(t, err)

	assert.NoError(t, client.Write("/multiBlocks", 0, randomBytes))
	copy(expectedFinalResults, randomBytes)
	data, err := client.Read("/multiBlocks", 0, uint64(length))
	assert.NoError(t, err)
	assert.EqualValues(t, randomBytes, data)

	// Writing at the start
	length = 2*chunkServer.BlockSize + 1 // This write will be over 3 blocks
	randomBytes = randomBytes[:length]
	_, err = rand.Read(randomBytes)
	assert.NoError(t, err)

	assert.NoError(t, client.Write("/multiBlocks", 0, randomBytes))
	copy(expectedFinalResults, randomBytes)
	data, err = client.Read("/multiBlocks", 0, uint64(length))
	assert.NoError(t, err)
	assert.EqualValues(t, randomBytes, data)

	// Writing at the middle
	randomBytes = randomBytes[:length]
	_, err = rand.Read(randomBytes)
	assert.NoError(t, err)

	assert.NoError(t, client.Write("/multiBlocks", 3*chunkServer.BlockSize+1, randomBytes))
	copy(expectedFinalResults[3*chunkServer.BlockSize+1:], randomBytes)
	data, err = client.Read("/multiBlocks", 3*chunkServer.BlockSize+1, uint64(length))
	assert.NoError(t, err)
	assert.EqualValues(t, len(randomBytes), len(data))
	assert.EqualValues(t, randomBytes, data)

	// Checking the whole chunk
	data, err = client.Read("/multiBlocks", 0, uint64(len(expectedFinalResults)))
	assert.NoError(t, err)
	assert.EqualValues(t, expectedFinalResults, data)
}

func TestRecordAppend(t *testing.T) {
	client, err := New("localhost", 52684)
	assert.NoError(t, err)
	defer client.Close()
	assert.NoError(t, client.Mkdir("/record"))
	assert.NoError(t, client.Mkdir("/record/append"))
	assert.NoError(t, client.Create("/record/append/file"))

	assert.NoError(t, client.RecordAppend("/record/append/file", []byte("Hello ")))

	assert.NoError(t, client.RecordAppend("/record/append/file", []byte("World!")))

	data, err := client.Read("/record/append/file", 0, uint64(len("Hello World!")))
	assert.NoError(t, err)
	assert.Equal(t, "Hello World!", string(data))
}
