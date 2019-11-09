"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
const path_1 = require("path");
const fastify = require("fastify");
const Config_1 = require("./Config");
const Crypto_1 = require("./Crypto");
const NodeList = require("./NodeList");
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
    if (NodeList.isFirst()) {
        const ip = request.req.socket.remoteAddress;
        const port = request.body.nodeInfo.externalPort;
        if (ip && port) {
            NodeList.addNode({
                ip,
                port,
            });
        }
    }
    reply.send(NodeList.getSignedList());
});
server.get('/nodelist', (request, reply) => {
    reply.send(NodeList.getSignedList());
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
//# sourceMappingURL=server.js.map