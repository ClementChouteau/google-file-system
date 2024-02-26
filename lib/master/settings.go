package master

import (
	"Google_File_System/lib/utils"
)

type Settings struct {
	utils.Endpoint
	Folder                 string
	DefaultReplicationGoal uint32
}
