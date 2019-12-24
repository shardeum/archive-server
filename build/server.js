"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
const path_1 = require("path");
const fastify = require("fastify");
const fastifyCors = require("fastify-cors");
const Config_1 = require("./Config");
const Crypto = require("./Crypto");
const State = require("./State");
const NodeList = require("./NodeList");
const P2P = require("./P2P");
const Storage = require("./Storage");
const Data = require("./Data/Data");
// Override default config params from config file, env vars, and cli args
const file = path_1.join(process.cwd(), 'archiver-config.json');
const env = process.env;
const args = process.argv;
Config_1.overrideDefaultConfig(file, env, args);
// Set crypto hash key from config
Crypto.setCryptoHashKey(Config_1.config.ARCHIVER_HASH_KEY);
// If no keypair provided, generate one
if (Config_1.config.ARCHIVER_SECRET_KEY === '' || Config_1.config.ARCHIVER_PUBLIC_KEY === '') {
    const keypair = Crypto.core.generateKeypair();
    Config_1.config.ARCHIVER_PUBLIC_KEY = keypair.publicKey;
    Config_1.config.ARCHIVER_SECRET_KEY = keypair.secretKey;
}
// Initialize state from config
State.initFromConfig(Config_1.config);
// Initialize storage
Storage.initStorage(State.dbFile);
if (State.isFirst === false) {
    /**
     * ENTRY POINT: Existing Shardus network
     */
    // [TODO] If your not the first archiver node, get a nodelist from the others
    // [TODO] Send a join request to a consensus node from the nodelist
    // [TODO] After you've joined, select a consensus node to be your dataSender
    startServer();
}
else {
    startServer();
}
function startServer() {
    // Start REST server and register endpoints
    const server = fastify({
        logger: true,
    });
    server.register(fastifyCors);
    server.get('/nodeinfo', (_request, reply) => {
        reply.send({
            publicKey: Config_1.config.ARCHIVER_PUBLIC_KEY,
            ip: Config_1.config.ARCHIVER_IP,
            port: Config_1.config.ARCHIVER_PORT,
            time: Date.now(),
        });
    });
    /**
     * ENTRY POINT: New Shardus network
     *
     * Consensus node zero (CZ) posts IP and port to archiver node zero (AZ).
     *
     * AZ adds CZ to nodelist, sets CZ as dataSender, and responds with
     * nodelist + archiver join request
     *
     * CZ adds AZ's join reqeuest to cycle zero and sets AZ as cycleRecipient
     */
    server.post('/nodelist', (request, reply) => {
        const response = {
            nodeList: NodeList.getList(),
        };
        // Network genesis
        if (State.isFirst && NodeList.isEmpty()) {
            const ip = request.req.socket.remoteAddress;
            const port = request.body.nodeInfo.externalPort;
            const publicKey = request.body.nodeInfo.publicKey;
            if (ip && port) {
                const firstNode = {
                    ip,
                    port,
                    publicKey,
                };
                // Add first node to NodeList
                NodeList.addNodes(NodeList.Statuses.SYNCING, firstNode);
                // Set first node as dataSender
                Data.addDataSenders({
                    nodeInfo: firstNode,
                    type: Data.DataTypes.CYCLE,
                });
                // Add joinRequest to response
                response.joinRequest = P2P.createJoinRequest();
            }
        }
        Crypto.sign(response);
        reply.send(response);
    });
    server.get('/nodelist', (_request, reply) => {
        const response = {
            nodeList: NodeList.getList(),
        };
        Crypto.sign(response);
        reply.send(response);
    });
    server.get('/exit', (_request, reply) => {
        reply.send('Shutting down...');
        process.exit();
    });
    // POST /newdata
    server.route(Data.routePostNewdata);
    // Always bind to all interfaces on the desired port
    server.listen(Config_1.config.ARCHIVER_PORT, '0.0.0.0', (err, _address) => {
        if (err) {
            server.log.error(err);
            process.exit(1);
        }
    });
}
//# sourceMappingURL=server.js.map