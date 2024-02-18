# The Google File System

https://research.google/pubs/the-google-file-system/

TODO implementation choices

## TODO

Version of chunk (saved on disk) to know chunk version
- refuse update and chunk when version is not up-to-date
- send versions in regular heartbeats

When chunk is empty do not touch it
