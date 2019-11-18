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
    // [TODO] If your not the first archiver node, get a nodelist from the others
    // [TODO] Send a join request a consensus node from the nodelist
    // [TODO] After you've joined, select a consensus node to be your cycleSender
    startServer();
}
else {
    startServer();
}
function processNewCycle(cycle) {
    // Update NodeList from cycle info
    // Get new cycleSender if current cycleSender leaves network
    Storage.storeCycle(cycle);
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
    server.post('/nodelist', (request, reply) => {
        // Network genesis
        if (State_1.state.isFirst && NodeList.isEmpty()) {
            const ip = request.req.socket.remoteAddress;
            const port = request.body.nodeInfo.externalPort;
            if (ip && port) {
                const firstNode = {
                    ip,
                    port,
                };
                // Add first node to NodeList
                NodeList.addNode(firstNode);
                // Set first node as cycleSender
                State_1.state.cycleSender = firstNode;
            }
        }
        const response = {
            nodeList: NodeList.getList(),
            joinRequest: P2P.createJoinRequest(),
        };
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
        processNewCycle(cycle);
        reply.send();
    });
    server.get('/exit', (_request, reply) => {
        reply.send('Shutting down...');
        process.exit();
    });
    server.listen(Config_1.config.ARCHIVER_PORT, (err, address) => {
        if (err) {
            server.log.error(err);
            process.exit(1);
        }
    });
}
//# sourceMappingURL=server.js.map