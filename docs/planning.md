# Archive Server Planning

## What does it need to do?

### milestone-1

- Seed a new Shardus app network

  - Initialize archiver node

    - Config param for admin keypair

      - Creates new keypair if not provided

    - Config param for another archiver node's info

      - If provided, trys to get nodelist from it

      - Otherwise, becomes first archiver node

    - HTTP server

      - GET `/nodeinfo` endpoint that returns this archiver node's info

      - GET `/exit` endpoint for debugging

  - Maintain a consensus node list accessible over HTTP

    - `/nodelist` endpoint that returns a list of consensus nodes in the network

      - If `consensus` param provided, requester is a consensus node

### milestone-2

- Join an existing Shardus app network

  - Send archiver join request

    - HTTP client

  - Consensus nodes must handle it appropriately

### milestone-3

- Get fresh cycle data

  - Send/handle internal `shardus-net` requests

    - `shardus-net` package

* Update the consensus node list when nodes leave/join the network

  - Get fresh cycle data from consensus nodes

### milestone-4

- Save cycle data to disk

  - Store/query cycle data

    - Relational DB
