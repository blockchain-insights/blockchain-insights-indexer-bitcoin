# Changelog

All notable changes to this project will be documented in this file.

## [2.0.1-alpha] - 2024-12-04
- Fixed partition hooping bug in block streamer base
- Memgraph based Money Flow indexing module has been replaced with standard Money Flow indexer
- Improved performance of Money Flow indexing (single db roundtrip instead of multiple)

## [2.0.0-alpha] - 2024-12-02
- Updated bitcoin rpc library to latest version
- Implemented new Money Flow indexing feature
- Implemented new Balance Tracking feature
- Added Memgraph support for real-time and archival data
- Added RedPanda support for transaction streaming
- Removed neo4j support
- Removed pickle support and vout builder
 