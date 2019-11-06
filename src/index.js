"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
const fastify = require("fastify");
// Create a http server. We pass the relevant typings for our http version used.
// By passing types we get correctly typed access to the underlying http objects in routes.
// If using http2 we'd pass <http2.Http2Server, http2.Http2ServerRequest, http2.Http2ServerResponse>
const server = fastify({
    logger: true
});
server.get('/', (_request, reply) => {
    reply.send({ hello: 'world' });
});
server.listen(3000, (err, address) => {
    if (err) {
        server.log.error(err);
        process.exit(1);
    }
    server.log.info(`server listening on ${address}`);
});
//# sourceMappingURL=index.js.map