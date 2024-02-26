package utils

import (
	"errors"
	"fmt"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"hash/crc32"
	"math/rand"
	"net/rpc"
	"os"
	"strconv"
	"time"
)

const LeaseDuration = 30 * time.Second

const ChunkSize = 64 * 1024 * 1024

type ChunkId = uint32

type ChunkServerId = uint32

type Endpoint struct {
	Host string
	Port int
}

type Range[T any] struct {
	Offset T
	Length T
}

func (endpoint Endpoint) Address() string {
	return endpoint.Host + ":" + strconv.Itoa(endpoint.Port)
}

func (endpoint Endpoint) Call(serviceMethod string, args any, reply any) error {
	client, err := rpc.Dial("tcp", endpoint.Address())
	if err != nil {
		return fmt.Errorf("connecting to RPC server: %w", err)
	}
	defer client.Close()

	err = client.Call(serviceMethod, args, reply)
	if err != nil {
		return fmt.Errorf("calling %s: %w", serviceMethod, err)
	}
	return nil
}

type ChunkServer struct {
	Id       ChunkServerId
	Endpoint Endpoint
}

type ResettableTimer struct {
	C     <-chan time.Time
	timer *time.Timer
}

func NewResettableTimer(duration time.Duration) *ResettableTimer {
	timer := time.NewTimer(duration)
	resettableTimer := &ResettableTimer{
		C:     timer.C,
		timer: timer,
	}
	return resettableTimer
}

func (rt *ResettableTimer) Reset(duration time.Duration) {
	// Stop the timer if it's active
	if !rt.timer.Stop() {
		// Drain the channel in case there is a pending signal
		<-rt.timer.C
	}
	// Reset the timer with the new duration
	rt.timer.Reset(duration)
}

func LenSum[T any](slices [][]T) int {
	size := 0
	for _, slice := range slices {
		size += len(slice)
	}
	return size
}

func ResizeSlice[T any](slice []T, newLen int) []T {
	// Resize
	if cap(slice) >= newLen {
		return slice[:newLen]
	}

	// Reallocate
	newSlice := make([]T, newLen)
	copy(newSlice, slice)
	return newSlice
}

func Shuffle[T any](arr []T) {
	for i := len(arr) - 1; i > 0; i-- {
		j := rand.Intn(i + 1)
		arr[i], arr[j] = arr[j], arr[i]
	}
}

func Checksum(slice []byte) (uint32, error) {
	// Shortcut to avoid allocation
	if len(slice) == 0 {
		return 0, nil
	}

	hash := crc32.NewIEEE()
	n, err := hash.Write(slice)
	if err != nil {
		return 0, err
	}
	if n != len(slice) {
		return 0, errors.New("error during checksum")
	}
	return hash.Sum32(), nil
}

func InitializeLogger(logLevelFlag string) zerolog.Logger {
	// Parse and initialize log level
	level, err := zerolog.ParseLevel(logLevelFlag)
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

	return logger.Logger()
}
