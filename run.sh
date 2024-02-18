#!/bin/bash

pids=()
LEVEL=TRACE

# master
go run master.go --log=$LEVEL &
pids+=($!)

sleep 1s

# chunk server(s)
rm -rf data1/*
go run chunkServer.go --port=52685 --folder=data1 --log=$LEVEL &
pids+=($!)

rm -rf data2/*
go run chunkServer.go --port=52686 --folder=data2 --log=$LEVEL &
pids+=($!)

sleep 1s

# Wait for all commands to finish
for pid in "${pids[@]}"; do
    wait $pid
done
