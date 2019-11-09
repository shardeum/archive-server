import { join } from 'path';
import * as fastify from 'fastify';
import { Server, IncomingMessage, ServerResponse } from 'http';
import { overrideDefaultConfig, config } from './Config';
import { setCryptoHashKey, crypto } from './Crypto';
import * as NodeList from './NodeList';

// Override default config params from config file, env vars, and cli args
const file = join(process.cwd(), 'archiver-config.json');
const env = process.env;
const args = process.argv;
overrideDefaultConfig(file, env, args);

// Set crypto hash key from config
setCryptoHashKey(config.ARCHIVER_HASH_KEY);

// If no keypair provided, generate one
if (config.ARCHIVER_SECRET_KEY === '' || config.ARCHIVER_PUBLIC_KEY === '') {
  const keypair = crypto.generateKeypair();
  config.ARCHIVER_PUBLIC_KEY = keypair.publicKey;
  config.ARCHIVER_SECRET_KEY = keypair.secretKey;
}

// Start REST server and register endpoints
const server: fastify.FastifyInstance<
  Server,
  IncomingMessage,
  ServerResponse
> = fastify({
  logger: true,
});

server.get('/nodeinfo', (_request, reply) => {
  reply.send({
    publicKey: config.ARCHIVER_PUBLIC_KEY,
    ip: config.ARCHIVER_IP,
    port: config.ARCHIVER_PORT,
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

server.listen(config.ARCHIVER_PORT, (err, address) => {
  if (err) {
    server.log.error(err);
    process.exit(1);
  }
});
