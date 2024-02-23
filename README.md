# The Google File System

https://research.google/pubs/the-google-file-system/

TODO implementation choices

## TODO

Version of chunk (saved on disk) to know chunk version
- refuse update and chunk when version is not up-to-date
- send versions in regular heartbeats

When chunk is empty do not touch it

## Client library

> Is data path a different request from commit ?

Based on the wording from the paper it appears so.

## Master server

## Chunk server

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
