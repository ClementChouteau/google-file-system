#!/bin/bash

pids=()
LEVEL=TRACE

# master
go run ./cmd/master.go --log=$LEVEL &
pids+=($!)

sleep 1s

# chunk server(s)
rm -rf data1/*
go run ./cmd/chunkServer.go --port=52685 --folder=data1 --log=$LEVEL &
pids+=($!)

rm -rf data2/*
go run ./cmd/chunkServer.go --port=52686 --folder=data2 --log=$LEVEL &
pids+=($!)

rm -rf data3/*
go run ./cmd/chunkServer.go --port=52687 --folder=data3 --log=$LEVEL &
pids+=($!)

rm -rf data4/*
go run ./cmd/chunkServer.go --port=52688 --folder=data4 --log=$LEVEL &
pids+=($!)

sleep 1s

# Wait for all commands to finish
for pid in "${pids[@]}"; do
    wait $pid
done
