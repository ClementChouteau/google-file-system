# The Google File System

This project aims at implementing the Google File System.

It aims at a simple implementation but remain close to what is described in the paper.

https://research.google/pubs/the-google-file-system/

## Client library

> Is data path a different request from commit ?

Based on the wording from the paper it appears so.

## Master server

Two types of expiration are possible:
1) A chunk server stops sending heartbeats, in this case it is excluded
and its chunks are considered not replicated anymore.
2) A chunk server reports fewer chunks than previously (corrupted chunks).

## Chunk server

The chunk server stores each chunk in a separate file with checksums at the beginning,
this means that the data is in a contiguous part of the disk.
We waste around 4KB of checksums at the beginning of the file,
this should be ok as the GFS is not optimized for small files "Small files must be supported, but we need not optimize for them.".

Chunk versions are stored in a separate file to speed up restart, otherwise we would need to read
all chunk blocks before starting.

> When are chunks created ?

Chunks are created when a client wants to write to them.
The chunk file is created on primary and replicas when receiving lease.

> How data integrity is ensured ?

We assume to be in a crash-stop model where crash of all 3 (minimum replication factor)
servers holding a chunk is assumed to be impossible.
This implies that we can assume that after accepting an in memory write,
a chunk server will always survive to save user data.

> What about crash-recovery ?

When a chunk server crashes at the exact moment they accepted an operation
but before writing anything to disk (including new checksums) is tricky.
We can support this by using versions, this chunk will have an out of date version
and in the worst case the master will find out that this chunk is not up-to-date and will remove this replication.
