package main

import (
	"Google_File_System/chunkServer"
	"Google_File_System/utils/common"
	"Google_File_System/utils/rpcdefs"
	"bytes"
	"encoding/binary"
	"errors"
	"flag"
	"fmt"
	"github.com/google/uuid"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"io"
	"net"
	"net/rpc"
	"os"
	"strconv"
	"sync"
	"sync/atomic"
	"time"
)

const (
	Write = iota
	Done  // The corresponding operation is persisted
)

type OplogEntryHeader struct {
	OperationType uint8
	OperationId   uint32
	// TODO checksum of the entry
}

type OplogEntry struct {
	OplogEntryHeader
	Content Serializable
}

type Serializable interface {
	Serialize(io.Writer) error
}

type WithType interface {
	Type() uint8
}

type SerializableOperation interface {
	Serializable
	WithType
}

type WriteOperation struct {
	ChunkId   common.ChunkId
	Offset    uint32
	TmpFileId uuid.UUID
	Checksums []uint32
}

func (operation WriteOperation) Serialize(writer io.Writer) (err error) {
	err = binary.Write(writer, binary.BigEndian, operation.ChunkId)
	if err != nil {
		return
	}

	err = binary.Write(writer, binary.BigEndian, operation.Offset)
	if err != nil {
		return
	}

	err = binary.Write(writer, binary.BigEndian, operation.TmpFileId)
	if err != nil {
		return
	}

	// Serialize the variable-sized field (checksums) separately
	err = binary.Write(writer, binary.BigEndian, uint32(len(operation.Checksums)))
	if err != nil {
		return
	}

	for _, checksum := range operation.Checksums {
		err = binary.Write(writer, binary.BigEndian, checksum)
		if err != nil {
			return
		}
	}

	return
}

func (operation WriteOperation) Type() uint8 {
	return Write
}

type DoneOperation struct {
	TargetOperationId uint32
}

func (operation DoneOperation) Serialize(writer io.Writer) (err error) {
	err = binary.Write(writer, binary.BigEndian, operation.TargetOperationId)
	return
}

func (operation DoneOperation) Type() uint8 {
	return Done
}

func (header OplogEntryHeader) Serialize(writer io.Writer) (err error) {
	err = binary.Write(writer, binary.BigEndian, header)
	return
}

func (entry OplogEntry) Serialize(writer io.Writer) (err error) {
	err = entry.OplogEntryHeader.Serialize(writer)
	if err != nil {
		return
	}

	err = entry.Content.Serialize(writer)
	if err != nil {
		return
	}

	return
}

func Deserialize(reader io.Reader) (entry OplogEntry, err error) {
	err = binary.Read(reader, binary.BigEndian, &entry.OplogEntryHeader)
	if err != nil {
		return
	}

	switch entry.OperationType {
	case Write:
		var operation WriteOperation

		err = binary.Read(reader, binary.BigEndian, &operation.ChunkId)
		if err != nil {
			return
		}

		err = binary.Read(reader, binary.BigEndian, &operation.Offset)
		if err != nil {
			return
		}

		err = binary.Read(reader, binary.BigEndian, &operation.TmpFileId)
		if err != nil {
			return
		}

		// Serialize the variable-sized field (checksums) separately
		var length uint32
		err = binary.Read(reader, binary.BigEndian, &length)
		if err != nil {
			return
		}

		operation.Checksums = make([]uint32, 0, length)
		for i := 0; i < int(length); i++ {
			var checksum uint32
			err = binary.Read(reader, binary.BigEndian, &checksum)
			if err != nil {
				return
			}
			operation.Checksums = append(operation.Checksums, checksum)
		}

		entry.Content = &operation
		break

	case Done:
		var operation DoneOperation
		err = binary.Read(reader, binary.BigEndian, &operation)
		entry.Content = &operation
		break
	}

	return
}

type Oplog struct {
	nextAvailableOperationId atomic.Uint32
	added                    chan OplogEntry
	synced                   chan OplogEntry
}

func (oplog *Oplog) NewOperationId() uint32 {
	return oplog.nextAvailableOperationId.Add(1) - 1
}

func (oplog *Oplog) Add(operation SerializableOperation) {
	oplog.added <- OplogEntry{
		OplogEntryHeader: OplogEntryHeader{
			OperationType: operation.Type(),
			OperationId:   oplog.NewOperationId(),
			// TODO checksum
		},
		Content: operation,
	}
}

// TODO functions necessary ?
// TODO read all oplog lines (keep valid lines) + wait all operation durable?
// TODO truncate oplog to last valid line + sync
// TODO server start

type ChunkService struct {
	settings    chunkServer.Settings
	id          common.ChunkServerId
	oplog       Oplog
	blocksCache *chunkServer.BlocksCache
	chunks      sync.Map // common.ChunkId => *chunkServer.Chunk
	servers     sync.Map // common.ChunkServerId => common.ChunkServer
}

func (chunkService *ChunkService) ensureFolders() (err error) {
	err = os.Mkdir(chunkService.settings.Folder, 0755)
	if err != nil && !os.IsExist(err) {
		return err
	}

	err = os.Mkdir(chunkService.settings.GetChunksFolder(), 0755)
	if err != nil && !os.IsExist(err) {
		return err
	}

	err = os.Mkdir(chunkService.settings.GetTemporaryFilesFolder(), 0755)
	if err != nil && !os.IsExist(err) {
		return err
	}

	return
}

func (chunkService *ChunkService) readChunkMetadataFromDisk() (count int, err error) {
	chunksPath := chunkService.settings.GetChunksFolder()
	entries, err := os.ReadDir(chunksPath)
	if err != nil {
		return 0, errors.New("could not open \"chunks\" directory")
	}

	for _, file := range entries {
		id, err := strconv.ParseUint(file.Name(), 10, 32)
		if err != nil {
			return 0, err
		}

		chunkMetadata := &chunkServer.Chunk{
			Id: common.ChunkId(id),
		}
		chunkService.chunks.Store(common.ChunkId(id), chunkMetadata)
		count++
	}

	return
}

func (chunkService *ChunkService) readServerIdFromDisk() (common.ChunkServerId, error) {
	var id common.ChunkServerId

	file, err := os.Open(chunkService.settings.GetServerIdPath())
	if err != nil {
		return id, err
	}

	err = binary.Read(file, binary.BigEndian, &id)
	return id, err
}

func (chunkService *ChunkService) writeServerIdToDisk(id common.ChunkServerId) (err error) {
	var file *os.File
	file, err = os.Create(chunkService.settings.GetServerIdPath())
	if err != nil {
		return
	}

	if err = binary.Write(file, binary.BigEndian, id); err != nil {
		return
	}

	err = file.Sync()

	return
}

func (chunkService *ChunkService) askServerId() common.ChunkServerId {
	log.Debug().Msg("requesting chunk server id from master")

	client, err := rpc.Dial("tcp", chunkService.settings.Master.Address())
	if err != nil {
		log.Fatal().Err(err).Msg("connecting to RPC server")
	}
	defer client.Close()

	var reply common.ChunkServerId
	err = client.Call("MasterService.RegisterRPC", chunkService.settings.Endpoint, &reply)
	if err != nil {
		log.Fatal().Err(err).Msg("calling RegisterRPC")
	}
	return reply
}

func (chunkService *ChunkService) ensureServerId() common.ChunkServerId {
	id, err := chunkService.readServerIdFromDisk()

	if err != nil {
		if !os.IsNotExist(err) {
			log.Fatal().Err(err).Msg("could not read chunk server id")
		}

		id = chunkService.askServerId()
		log.Debug().Msgf("received new server id=%d from master", id)
		err := chunkService.writeServerIdToDisk(id)
		if err != nil {
			log.Fatal().Err(err).Msg("could not read chunk server id")
		}
	} else {
		log.Debug().Msgf("read server id=%d from disk", id)
	}

	chunkService.id = id
	return id
}

func (chunkService *ChunkService) sendHeartbeat() {
	// TODO do not send all chunks every time
	// TODO just send diff from initial chunks
	chunks := make([]common.ChunkId, 0)
	chunkService.chunks.Range(func(key any, value any) bool {
		chunks = append(chunks, value.(*chunkServer.Chunk).Id)
		return true
	})

	log.Debug().Msgf("sending heartbeat with %d chunks", len(chunks))

	client, err := rpc.Dial("tcp", chunkService.settings.Master.Address())
	if err != nil {
		log.Error().Msgf("connecting to RPC server: %v", err)
		return
	}
	defer client.Close()

	request := rpcdefs.HeartBeatArgs{
		Id:     chunkService.id,
		Chunks: chunks,
	}
	var reply struct{}
	err = client.Call("MasterService.HeartbeatRPC", request, &reply)
	if err != nil {
		log.Error().Msgf("calling HeartbeatRPC: %v", err)
	}
}

func (chunkService *ChunkService) oplogWriter(wg *sync.WaitGroup) (err error) {
	defer wg.Done()
	defer log.Info().Msg("stopping oplog writer")

	var file *os.File
	file, err = os.OpenFile(chunkService.settings.GetOplogPath(), os.O_WRONLY|os.O_APPEND|os.O_CREATE, 0644)
	if err != nil {
		return
	}
	defer file.Close()

	buffer := make([]OplogEntry, 0, len(chunkService.oplog.added))

	for {
		// Wait to have at least 1 operation to write
		operation, ok := <-chunkService.oplog.added
		if !ok {
			return
		}
		buffer = append(buffer, operation)

		// Get remaining operations immediately available
		for {
			select {
			case operation, ok := <-chunkService.oplog.added:
				if !ok {
					return
				}
				buffer = append(buffer, operation)
			default:
				// Write and sync these operations
				for _, operation := range buffer {
					buffer := new(bytes.Buffer)
					err := operation.Serialize(buffer)
					if err != nil {
						log.Error().Err(err).Msg("when serializing oplog operation")
					}
					_, err = file.Write(buffer.Bytes())
					if err != nil {
						log.Error().Err(err).Msg("when writing oplog operation to disk")
					}
				}
				err := file.Sync()
				if err != nil {
					log.Error().Err(err).Msg("when syncing oplog")
				}

				// Ask the background thread to apply these operations
				for _, operation := range buffer {
					chunkService.oplog.synced <- operation
				}
				buffer = buffer[:0]
				break
			}
		}
	}
}

func (chunkService *ChunkService) oplogReader() (err error) {
	defer log.Info().Msg("read oplog from disk")

	var file *os.File
	file, err = os.OpenFile(chunkService.settings.GetOplogPath(), os.O_RDONLY|os.O_CREATE, 0644)

	n := 0
	for {
		var entry OplogEntry
		entry, err = Deserialize(file)
		if err == io.EOF {
			err = nil
			break
		}
		if err != nil {
			return
		}

		n++
		chunkService.oplog.synced <- entry
	}

	log.Debug().Msgf("read %d entries from disk", n)

	return
}

func (chunkService *ChunkService) applyWrite(operation *WriteOperation) error {
	chunk, err := chunkService.ensureChunk(operation.ChunkId, false)
	if err != nil {
		return err
	}

	chunk.FileMutex.Lock()
	defer chunk.FileMutex.Unlock()

	tmpPath := chunkService.settings.GetTemporaryFilePath(operation.TmpFileId.String())
	tmpFile, err := os.Open(tmpPath)
	if err != nil {
		return err
	}
	defer tmpFile.Close()

	chunkFile, err := os.OpenFile(chunkService.settings.GetChunkPath(operation.ChunkId), os.O_WRONLY, 0644)
	if err != nil {
		return err
	}
	defer chunkFile.Close()

	err = chunkServer.WriteChunkBlock(tmpFile, chunkFile, operation.Offset, operation.Checksums)
	if err != nil {
		return err
	}

	err = chunkFile.Sync()
	if err != nil {
		return err
	}

	// TODO logic to remove dirty flag

	tmpFile.Close()
	return os.Remove(tmpPath)
}

func (chunkService *ChunkService) apply(entry OplogEntry) {
	switch entry.OperationType {
	case Write:
		operation := entry.Content.(*WriteOperation)
		err := chunkService.applyWrite(operation)
		if err != nil {
			log.Error().Err(err).Msg("applyWrite")
		}
		return

	case Done:
		return
	}
}

func (chunkService *ChunkService) backgroundWriter() (err error) {
	for {
		select {
		case entry, ok := <-chunkService.oplog.synced:
			if !ok {
				return
			}

			chunkService.apply(entry)
		}
	}
}

func (chunkService *ChunkService) heartbeatTicker(wg *sync.WaitGroup) {
	defer wg.Done()

	log.Info().Msg("starting to send heartbeats")
	heartbeatTick := time.Tick(5 * time.Second)
	chunkService.sendHeartbeat()
	for range heartbeatTick {
		chunkService.sendHeartbeat()
	}
}

func (chunkService *ChunkService) startServer(wg *sync.WaitGroup) error {
	defer wg.Done()

	err := rpc.Register(chunkService)
	if err != nil {
		return fmt.Errorf("registering RPC chunkService: %w", err)
	}

	listener, err := net.Listen("tcp", chunkService.settings.Address())
	if err != nil {
		return fmt.Errorf("starting RPC server: %w", err)
	}

	log.Info().Msgf("chunk (RPC) server ready listening on :%d", chunkService.settings.Port)
	for {
		conn, err := listener.Accept()
		if err != nil {
			return fmt.Errorf("accepting connection: %w", err)
		}
		go rpc.ServeConn(conn)
	}
}

func (chunkService *ChunkService) applyExistingOplog() (err error) {
	errorCh := make(chan error, 2)

	var wg sync.WaitGroup
	wg.Add(2)

	go func() {
		defer wg.Done()
		err := chunkService.oplogReader()
		if err != nil {
			log.Error().Err(err).Msg("calling oplogReader")
		}
		errorCh <- err
		close(chunkService.oplog.synced)
	}()

	go func() {
		defer wg.Done()
		err := chunkService.backgroundWriter()
		if err != nil {
			log.Error().Err(err).Msg("calling backgroundWriter")
		}
		errorCh <- err
	}()

	wg.Wait()
	close(errorCh)

	chunkService.oplog.synced = make(chan OplogEntry, 1024)

	err, _ = <-errorCh
	if err != nil {
		return errors.New("could not apply already existing oplog")
	}
	return
}

func (chunkService *ChunkService) start() {
	if err := chunkService.ensureFolders(); err != nil {
		log.Fatal().Err(err).Msg("calling ensureFolders")
	}

	chunksCount, err := chunkService.readChunkMetadataFromDisk()
	if err != nil {
		log.Fatal().Err(err).Send()
	}
	log.Info().Msgf("found %d chunk(s) on disk", chunksCount)

	serverId := chunkService.ensureServerId()
	log.Logger = log.Logger.With().Uint32("chunkServer", serverId).Logger()
	log.Info().Msg("chunk server ready")

	err = chunkService.applyExistingOplog()
	if err != nil {
		log.Fatal().Err(err).Msg("calling applyExistingOplog")
	}

	var wg sync.WaitGroup
	wg.Add(4)
	go chunkService.heartbeatTicker(&wg)
	go func() {
		defer wg.Done()
		chunkService.backgroundWriter()
	}()
	go chunkService.oplogWriter(&wg)
	go chunkService.startServer(&wg)

	wg.Wait()
}

func (chunkService *ChunkService) writeTemporary(name string, data []byte) (err error) {
	for {
		file, err := os.OpenFile(chunkService.settings.GetTemporaryFilePath(name), os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0644)
		if err != nil {
			return err
		}
		defer file.Close()

		if err == nil {
			_, err = file.Write(data)
			if err != nil {
				return err
			}

			err := file.Sync()
			if err != nil {
				return err
			}

			return nil
		}

		return err
	}
}

func (chunkService *ChunkService) readTemporary(name string) (data []byte, err error) {
	data, err = os.ReadFile(chunkService.settings.GetTemporaryFilePath(name))
	return
}

// Ensure that the chunk exists, writes it
func (chunkService *ChunkService) ensureChunk(id common.ChunkId, create bool) (chunk *chunkServer.Chunk, err error) {
	newChunk := &chunkServer.Chunk{
		Id: id,
	}
	value, exists := chunkService.chunks.LoadOrStore(id, newChunk)

	if !create && !exists {
		err = errors.New("no corresponding chunk")
		return
	}

	// Ensure that the file exists
	if !exists {
		newChunk.FileMutex.Lock()
		defer newChunk.FileMutex.Unlock()
		err = chunkServer.EnsureChunk(chunkService.settings.GetChunkPath(id))
		log.Debug().Msgf("created chunk %d", id)
	}
	chunk = value.(*chunkServer.Chunk)
	return
}

func (chunkService *ChunkService) ReadRPC(request rpcdefs.ReadArgs, reply *rpcdefs.ReadReply) error {
	value, exists := chunkService.chunks.Load(request.Id)
	if !exists {
		// TODO return slice of 0 ?
		// TODO this chunks could be sparse
		return errors.New("no corresponding chunk")
	}

	data, err := value.(*chunkServer.Chunk).ReadInMemoryChunk(chunkService.blocksCache, request.Offset, request.Length)
	if err != nil {
		return err
	}
	reply.Data = data

	return nil
}

func (chunkService *ChunkService) GrantLeaseRPC(request rpcdefs.GrantLeaseArgs, _ *rpcdefs.GrantLeaseReply) error {
	log.Debug().Msgf("received lease for chunk %d", request.ChunkId)

	chunk, err := chunkService.ensureChunk(request.ChunkId, true)
	if err != nil {
		return err
	}

	chunk.LeaseMutex.Lock()
	chunk.GiveLeaseNow(request.Replication)
	chunk.LeaseMutex.Unlock()

	return nil
}

func (chunkService *ChunkService) RevokeLeaseRPC(request rpcdefs.RevokeLeaseArgs, _ *rpcdefs.RevokeLeaseReply) error {
	value, exists := chunkService.chunks.Load(request.ChunkId)
	if !exists {
		return errors.New("no corresponding chunk")
	}
	chunk := value.(*chunkServer.Chunk)

	chunk.LeaseMutex.Lock()
	chunk.RevokeLease()
	chunk.LeaseMutex.Unlock()

	return nil
}

func (chunkService *ChunkService) PushDataRPC(request rpcdefs.PushDataArgs, _ *rpcdefs.PushDataReply) error {
	if len(request.Data) == 0 {
		return errors.New("empty Data")
	}

	var wg sync.WaitGroup
	wg.Add(2)

	// Write data on disk
	var writeErr error
	go func() {
		defer wg.Done()
		writeErr = chunkService.writeTemporary(request.Id.String(), request.Data)
	}()

	// Push to next server if we are not the last one
	var forwardErr error
	var forwardReply rpcdefs.PushDataReply
	go func() {
		defer wg.Done()
		for i, server := range request.Servers {
			if server.Id != chunkService.id {
				continue
			}

			if i != len(request.Servers)-1 {
				forwardErr = request.Servers[i+1].Endpoint.Call("ChunkService.PushDataRPC", request, &forwardReply)
			}
			break
		}
	}()

	wg.Wait()

	if writeErr != nil {
		return writeErr
	}
	if forwardErr != nil {
		return forwardErr
	}

	log.Debug().Msgf("wrote temporary file of length %d with id %s", len(request.Data), request.Id.String())

	return nil
}

func (chunkService *ChunkService) ApplyWriteRPC(request rpcdefs.ApplyWriteArgs, _ *rpcdefs.ApplyWriteReply) error {
	chunk, err := chunkService.ensureChunk(request.Id, true)
	if err != nil {
		log.Error().Err(err).Msg("calling ensureChunk")
		return err
	}

	data, err := chunkService.readTemporary(request.DataId.String())
	if err != nil {
		return err
	}
	var checksums []uint32
	checksums, err = chunk.WriteInMemoryChunk(chunkService.blocksCache, request.Offset, data)
	if err != nil {
		return err
	}

	log.Debug().Msgf("wrote %d bytes to chunk %d at offset %d", len(data), request.Id, request.Offset)
	chunkService.oplog.Add(&WriteOperation{
		ChunkId:   request.Id,
		Offset:    request.Offset,
		TmpFileId: request.DataId,
		Checksums: checksums,
	})

	return nil
}

func (chunkService *ChunkService) WriteRPC(request rpcdefs.WriteArgs, reply *rpcdefs.WriteReply) error {
	chunk, err := chunkService.ensureChunk(request.Id, false)
	if err != nil {
		log.Error().Err(err).Msg("calling ensureChunk")
		return err
	}

	// Check lease
	chunk.LeaseMutex.RLock()
	defer chunk.LeaseMutex.RUnlock()
	if !chunk.HasLease() {
		log.Debug().Msgf("no lease for writing to chunk %d", request.Id)
		return &rpcdefs.NoLeaseError{Message: fmt.Sprintf("no lease for chunk with id %d", request.Id)}
	}

	// TODO push in queue to propagate to replicas

	// Write to all replicas
	for _, server := range chunk.Replication {
		if server.Id == chunkService.id {
			continue
		}

		writeReply := &rpcdefs.WriteReply{}
		err = server.Endpoint.Call("ChunkService.ApplyWriteRPC", request, writeReply)
		if err != nil {
			return err
		}
	}

	return chunkService.ApplyWriteRPC(request, reply)
}

func (chunkService *ChunkService) RecordAppendRPC(request rpcdefs.RecordAppendArgs, reply *rpcdefs.RecordAppendReply) error {
	chunk, err := chunkService.ensureChunk(request.Id, false) // Chunk is created when receiving lease
	if err != nil {
		log.Error().Err(err).Msg("calling ensureChunk")
		return err
	}

	chunk.LeaseMutex.RLock()
	defer chunk.LeaseMutex.RUnlock()
	if !chunk.HasLease() {
		log.Debug().Msgf("no lease for writing to chunk %d", request.Id)
		return &rpcdefs.NoLeaseError{Message: fmt.Sprintf("no lease for chunk with id %d", request.Id)}
	}

	data, err := chunkService.readTemporary(request.DataId.String())
	if err != nil {
		return err
	}

	padding, offset, checksums, err := chunk.AppendInMemoryChunk(chunkService.blocksCache, data)
	if err != nil {
		return err
	}
	// TODO //////////////////////////////////////////////////////////////////

	for _, server := range chunk.Replication {
		if server.Id == chunkService.id {
			continue
		}

		// Write to the replica
		writeRequest := rpcdefs.WriteArgs{
			Id:     request.Id,
			DataId: request.DataId,
			Offset: offset,
		}
		writeReply := &rpcdefs.WriteReply{}
		err := server.Endpoint.Call("ChunkService.ApplyWriteRPC", writeRequest, writeReply)
		if err != nil {
			return err
		}
	}

	log.Debug().Msgf("record append of %d bytes done for chunk %d at offset %d", len(data), request.Id, offset)
	chunkService.oplog.Add(&WriteOperation{
		ChunkId:   request.Id,
		Offset:    offset,
		TmpFileId: request.DataId,
		Checksums: checksums,
	})

	*reply = rpcdefs.RecordAppendReply{
		Done: !padding,
	}

	return nil
}

func NewChunkService(settings chunkServer.Settings) (chunkService *ChunkService) {
	chunkService = new(ChunkService)
	chunkService.oplog = Oplog{
		added:  make(chan OplogEntry, 1024),
		synced: make(chan OplogEntry, 1024),
	}
	chunkService.settings = settings
	chunkService.blocksCache = chunkServer.NewBlocksCache(&chunkService.settings)

	return
}

func main() {
	var port = flag.Int("port", 52685, "Server port")
	var host = flag.String("host", "localhost", "Server host")
	var masterPort = flag.Int("master-port", 52684, "Master server port")
	var masterHost = flag.String("master-host", "localhost", "Master server host")
	var folder = flag.String("folder", "data", "Path to the folder")
	var logLevel = flag.String("log", "INFO", "Log level among \"PANIC\", \"FATAL\", \"ERROR\", \"WARN\", \"INFO\", \"DEBUG\", \"TRACE\"")
	flag.Parse()

	// Parse and initialize log level
	level, err := zerolog.ParseLevel(*logLevel)
	if err != nil {
		log.Fatal().Err(err).Send()
	}
	zerolog.SetGlobalLevel(level)

	// Setup logger
	consoleWriter := zerolog.ConsoleWriter{Out: os.Stderr}
	logger := zerolog.New(consoleWriter).
		With().
		Timestamp()
	if level == zerolog.TraceLevel {
		logger = logger.Caller()
	}
	log.Logger = logger.Logger()

	// Start server
	settings := chunkServer.Settings{
		Endpoint: common.Endpoint{
			Host: *host,
			Port: *port,
		},
		Master: common.Endpoint{
			Host: *masterHost,
			Port: *masterPort,
		},
		Folder: *folder,
	}

	chunkService := NewChunkService(settings)
	chunkService.start()
}
