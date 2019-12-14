"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
const path_1 = require("path");
const fastify = require("fastify");
const Config_1 = require("./Config");
const Crypto_1 = require("./Crypto");
const State_1 = require("./State");
const NodeList = require("./NodeList");
const P2P = require("./P2P");
const Storage = require("./Storage");
// Override default config params from config file, env vars, and cli args
const file = path_1.join(process.cwd(), 'archiver-config.json');
const env = process.env;
const args = process.argv;
Config_1.overrideDefaultConfig(file, env, args);
// Set crypto hash key from config
Crypto_1.setCryptoHashKey(Config_1.config.ARCHIVER_HASH_KEY);
// If no keypair provided, generate one
if (Config_1.config.ARCHIVER_SECRET_KEY === '' || Config_1.config.ARCHIVER_PUBLIC_KEY === '') {
    const keypair = Crypto_1.crypto.generateKeypair();
    Config_1.config.ARCHIVER_PUBLIC_KEY = keypair.publicKey;
    Config_1.config.ARCHIVER_SECRET_KEY = keypair.secretKey;
}
// Initialize state from config
State_1.initStateFromConfig(Config_1.config);
// Initialize storage
Storage.initStorage(State_1.state.dbFile);
if (State_1.state.isFirst === false) {
    /**
     * ENTRY POINT: Existing Shardus network
     */
    // [TODO] If your not the first archiver node, get a nodelist from the others
    // [TODO] Send a join request a consensus node from the nodelist
    // [TODO] After you've joined, select a consensus node to be your cycleSender
    startServer();
}
else {
    startServer();
}
function processNewCycle(cycle) {
    // Save the cycle to db
    Storage.storeCycle(cycle);
    // Update NodeList from cycle info
    function parseWarn(fallback, json, msg) {
        try {
            return JSON.parse(json);
        }
        catch (err) {
            console.warn(msg ? msg : err);
            return fallback;
        }
    }
    //   Add joined nodes
    const joinedConsensors = parseWarn([], cycle.joinedConsensors, `Error processing cycle ${cycle.counter}: failed to parse joinedConsensors`);
    NodeList.addNodes(...joinedConsensors);
    //   Remove removed nodes
    const removed = parseWarn([], cycle.removed, `Error processing cycle ${cycle.counter}: failed to parse removed`);
    for (const publicKey of removed) {
        NodeList.removeNode({ publicKey });
    }
    // [TODO] Get new cycleSender if current cycleSender leaves network
    console.log(`Processed cycle ${cycle.counter}`);
}
function startServer() {
    // Start REST server and register endpoints
    const server = fastify({
        logger: true,
    });
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
     * AZ adds CZ to nodelist, sets CZ as cycleSender, and responds with
     * nodelist + archiver join request
     *
     * CZ adds AZ's join reqeuest to cycle zero and sets AZ as cycleRecipient
     */
    server.post('/nodelist', (request, reply) => {
        const response = {
            nodeList: NodeList.getList(),
        };
        // Network genesis
        if (State_1.state.isFirst && NodeList.isEmpty()) {
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
                NodeList.addNodes(firstNode);
                // Set first node as cycleSender
                State_1.state.cycleSender = firstNode;
                // Add joinRequest to response
                response.joinRequest = P2P.createJoinRequest();
            }
        }
        Crypto_1.crypto.signObj(response, State_1.state.nodeInfo.secretKey, State_1.state.nodeInfo.publicKey);
        reply.send(response);
    });
    server.get('/nodelist', (_request, reply) => {
        const response = {
            nodeList: NodeList.getList(),
        };
        Crypto_1.crypto.signObj(response, State_1.state.nodeInfo.secretKey, State_1.state.nodeInfo.publicKey);
        reply.send(response);
    });
    server.post('/newcycle', (request, reply) => {
        // [TODO] verify that it came from cycleSender
        const cycle = request.body;
        console.log('GOT CYCLE:', cycle);
        processNewCycle(cycle);
        reply.send();
    });
    server.get('/exit', (_request, reply) => {
        reply.send('Shutting down...');
        process.exit();
    });
    // Always bind to all interfaces on the desired port
    server.listen(Config_1.config.ARCHIVER_PORT, '0.0.0.0', (err, _address) => {
        if (err) {
            server.log.error(err);
            process.exit(1);
        }
    });
}
//# sourceMappingURL=server.js.map