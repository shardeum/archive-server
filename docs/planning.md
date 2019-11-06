# Archive Server Planning

## What does it need to do?

### milestone-1

* Seed a new Shardus app network

  - Maintain a node list accessable over HTTP
  
    * HTTP server

  - Update the node list when nodes leave/join the network

    * Get fresh cycle data

### milestone-2

* Join an existing Shardus app network

  - Send/handle HTTP requests

    * HTTP client

    * HTTP server

### milestone-3

* Get fresh cycle data

  - Send/handle internal `shardus-net` requests

    * `shardus-net` package

### milestone-4

* Save cycle data to disk

  - Store/query cycle data

    * Relational DB
