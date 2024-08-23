import { join } from 'path'
import * as dbstore from './dbstore'
import * as txDigesterDB from './txDigester/index'
import { overrideDefaultConfig, config } from './Config'
import * as Crypto from './Crypto'
import { Utils as StringUtils } from '@shardus/types'
import { readFileSync } from 'fs'
import { resolve } from 'path'
import * as Logger from './Logger'
import { startSaving } from './saveConsoleOutput'
import fastify, { FastifyInstance } from 'fastify'
import fastifyCors from '@fastify/cors'
import fastifyRateLimit from '@fastify/rate-limit'
import { Server, IncomingMessage, ServerResponse } from 'http'
import { registerRoutes } from './txDigester/api'

const configFile = join(process.cwd(), 'archiver-config.json')

const start = async (): Promise<void> => {
  overrideDefaultConfig(configFile)

  const hashKey = config.ARCHIVER_HASH_KEY
  Crypto.setCryptoHashKey(hashKey)
  let logsConfig
  try {
    logsConfig = StringUtils.safeJsonParse(readFileSync(resolve(__dirname, '../archiver-log.json'), 'utf8'))
  } catch (err) {
    console.log('Failed to parse archiver log file:', err)
  }
  const logDir = `${config.ARCHIVER_LOGS}/txDigesterAPI`
  const baseDir = '.'
  logsConfig.dir = logDir
  Logger.initLogger(baseDir, logsConfig)
  if (logsConfig.saveConsoleOutput) {
    startSaving(join(baseDir, logsConfig.dir))
  }

  await dbstore.initializeDB(config)

  await txDigesterDB.initializeDB(config)

  console.log('Starting txDigest API server....')

  const server: FastifyInstance<Server, IncomingMessage, ServerResponse> = fastify({
    logger: false,
  })

  await server.register(fastifyCors)
  await server.register(fastifyRateLimit, {
    global: true,
    max: config.RATE_LIMIT,
    timeWindow: 10,
    allowList: ['127.0.0.1', '0.0.0.0'], // Excludes local IPs from rate limits
  })

  server.addContentTypeParser('application/json', { parseAs: 'string' }, (req, body, done) => {
    try {
      const jsonString = typeof body === 'string' ? body : body.toString('utf8')
      done(null, StringUtils.safeJsonParse(jsonString))
    } catch (err) {
      err.statusCode = 400
      done(err, undefined)
    }
  })

  server.setReplySerializer((payload) => {
    return StringUtils.safeStringify(payload)
  })

  // Register API routes
  registerRoutes(server as FastifyInstance<Server, IncomingMessage, ServerResponse>)

  // Start server and bind to port on all interfaces
  server.listen(
    {
      port: 8084,
      host: '0.0.0.0',
    },
    (err) => {
      Logger.mainLogger.debug('Listening', config.ARCHIVER_PORT)
      if (err) {
        server.log.error(err)
        process.exit(1)
      }
      Logger.mainLogger.debug('Archive-server has started.')
    }
  )
}

start()
