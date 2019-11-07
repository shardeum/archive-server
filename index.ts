import * as fastify from 'fastify';
import { Server, IncomingMessage, ServerResponse } from 'http';
import crypto = require('shardus-crypto-utils');
import * as NodeList from './src/NodeList';

// Load config

const config = {
  ARCHIVER_IP: '127.0.0.1',
  ARCHIVER_PORT: 6000,
  ARCHIVER_HASH_KEY:
    '64f152869ca2d473e4ba64ab53f49ccdb2edae22da192c126850970e788af347',
  ARCHIVER_PUBLIC_KEY: '',
  ARCHIVER_SECRET_KEY: '',
  ARCHIVER_EXISTING_IP: '',
  ARCHIVER_EXISTING_PORT: '',
  ARCHIVER_EXISTING_PUBLIC_KEY: '',
};

// [TODO] Override config from config file

// [TODO] Override config from env vars

// [TODO] Override config from there cli args

// If no keypair provided, generate one

crypto(config.ARCHIVER_HASH_KEY);

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
    nodeInfo: {
      publicKey: config.ARCHIVER_PUBLIC_KEY,
      ip: config.ARCHIVER_IP,
      port: config.ARCHIVER_PORT,
      time: Date.now(),
    },
  });
});

server.get('/nodelist', (_request, reply) => {
  reply.send({
    nodeList: NodeList.getList(),
  });
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
