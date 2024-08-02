import { FastifyPluginCallback } from 'fastify'

export const healthCheckRouter: FastifyPluginCallback = function (fastify, opts, done) {
  fastify.get('/is-alive', (req, res) => {
    return res.status(200).send('OK')
  })

  fastify.get('/is-healthy', (req, res) => {
    // TODO: Add actual health check logic
    return res.status(200).send('OK')
  })
  
  done()
}