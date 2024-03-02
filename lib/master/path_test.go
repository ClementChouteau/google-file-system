package master

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestNormalizeEmptyPath(t *testing.T) {
	_, err := normalize("")
	assert.Error(t, err)
}

func TestNormalizeNonAbsolutePath(t *testing.T) {
	_, err := normalize("folder")
	assert.Error(t, err)
}

func TestNormalizeRootPath(t *testing.T) {
	path, err := normalize("/")
	assert.NoError(t, err)
	assert.EqualValues(t, "/", path)
}

func TestNormalizeFolder(t *testing.T) {
	path, err := normalize("/folder")
	assert.NoError(t, err)
	assert.EqualValues(t, "/folder", path)
}

func TestNormalizeFolderWithTrailingSlash(t *testing.T) {
	path, err := normalize("/folder/")
	assert.NoError(t, err)
	assert.EqualValues(t, "/folder", path)
}

func TestNormalizeDoubleSlashFolder(t *testing.T) {
	path, err := normalize("//folder//")
	assert.NoError(t, err)
	assert.EqualValues(t, "/folder", path)
}

func TestNormalizeDoubleSlashRoot(t *testing.T) {
	path, err := normalize("///")
	assert.NoError(t, err)
	assert.EqualValues(t, "/", path)
}

func TestNormalizeSubfolder(t *testing.T) {
	path, err := normalize("/folder/sub/")
	assert.NoError(t, err)
	assert.EqualValues(t, "/folder/sub", path)
}

func TestParentPathEmpty(t *testing.T) {
	path := parentPath("")
	assert.EqualValues(t, "/", path)
}

func TestParentPathRoot(t *testing.T) {
	path := parentPath("/")
	assert.EqualValues(t, "/", path)
}

func TestParentPathFolder(t *testing.T) {
	path := parentPath("/folder")
	assert.EqualValues(t, "/", path)
}

func TestParentPathSubfolder(t *testing.T) {
	path := parentPath("/folder/sub")
	assert.EqualValues(t, "/folder", path)
}
