import * as fs from 'fs'
import * as Logger from './Logger'
import * as merge from 'deepmerge'
import * as minimist from 'minimist'
import { join } from 'path'

export interface Config {
  [index: string]: object | string | number | boolean
  ARCHIVER_IP: string
  ARCHIVER_PORT: number
  ARCHIVER_HASH_KEY: string
  ARCHIVER_PUBLIC_KEY: string
  ARCHIVER_SECRET_KEY: string
  ARCHIVER_DB: string // Archiver DB folder name
  EXISTING_ARCHIVER_DB_PATH: string
  DATASENDER_TIMEOUT: number
  RATE_LIMIT: number // number of allowed request per second,
  N_NODE_REJECT_PERCENT: number
  N_NODELIST: number
  N_RANDOM_NODELIST_BUCKETS: number // Number of random node lists in the NodeList cache
  RECEIPT_CONFIRMATIONS: number // Number of receipt confirmations (from other validators) before storing a tx receipt
  STATISTICS: {
    save: boolean
    interval: number
  }
  MODE: string
  DEBUG: {
    hashedDevAuth?: string
    devPublicKey?: string
  }
  dataLogWrite: boolean
  dataLogWriter: {
    dirName: string
    maxLogFiles: number
    maxReceiptEntries: number
    maxCycleEntries: number
    maxOriginalTxEntries: number
  }
  experimentalSnapshot: boolean
  VERBOSE: boolean
  useSerialization: boolean
  useSyncV2: boolean
  sendActiveMessage: boolean
  globalNetworkAccount: string
  maxValidatorsToServe: number
  verifyAccountData: boolean
  limitToArchiversOnly: boolean
  verifyAppReceiptData: boolean
}

let config: Config = {
  ARCHIVER_IP: '127.0.0.1',
  ARCHIVER_PORT: 4000,
  ARCHIVER_HASH_KEY: '',
  ARCHIVER_PUBLIC_KEY: '',
  ARCHIVER_SECRET_KEY: '',
  ARCHIVER_LOGS: 'archiver-logs',
  ARCHIVER_DB: 'archiver-db',
  EXISTING_ARCHIVER_DB_PATH: '',
  DATASENDER_TIMEOUT: 1000 * 60 * 5,
  RATE_LIMIT: 100, // 100 req per second,
  N_NODE_REJECT_PERCENT: 5, // Percentage of old nodes to remove from nodelist
  N_NODELIST: 30, // number of active node list GET /nodelist should emit but if the total active nodelist is less than said value it will emit all the node list.
  N_RANDOM_NODELIST_BUCKETS: 10,
  RECEIPT_CONFIRMATIONS: 5,
  STATISTICS: {
    save: true,
    interval: 1,
  },
  MODE: 'debug', // 'debug'/'release'
  DEBUG: {
    hashedDevAuth: '',
    devPublicKey: '',
  },
  dataLogWrite: true,
  dataLogWriter: {
    dirName: 'data-logs',
    maxLogFiles: 10,
    maxReceiptEntries: 10000, // Should be >= max TPS experienced by the network.
    maxCycleEntries: 500,
    maxOriginalTxEntries: 10000, // Should be >= max TPS experienced by the network.
  },
  experimentalSnapshot: true,
  VERBOSE: false,
  useSerialization: true,
  useSyncV2: true,
  sendActiveMessage: false,
  globalNetworkAccount: process.env.GLOBAL_ACCOUNT || '0'.repeat(64), //this address will change in the future
  maxValidatorsToServe: 10, // max number of validators to serve accounts data during restore mode
  verifyAccountData: true,
  limitToArchiversOnly: true,
  verifyAppReceiptData: true
}
// Override default config params from config file, env vars, and cli args
export async function overrideDefaultConfig(file: string): Promise<void> {
  const env = process.env
  const args = process.argv

  console.dir(config, { depth: null })

  // Override config from config file
  try {
    // eslint-disable-next-line security/detect-non-literal-fs-filename
    const fileConfig = JSON.parse(fs.readFileSync(file, { encoding: 'utf8' }))
    console.dir(fileConfig, { depth: null })
    const overwriteMerge = (target: [], source: []): [] => source
    config = merge(config, fileConfig, { arrayMerge: overwriteMerge })
    console.dir(config, { depth: null })
  } catch (err) {
    if (err && err.code !== 'ENOENT') {
      console.warn('Failed to parse config file:', err)
    }
  }

  // Override config from env vars
  for (const param in config) {
    /* eslint-disable security/detect-object-injection */
    if (env[param]) {
      switch (typeof config[param]) {
        case 'number': {
          config[param] = Number(env[param])
          break
        }
        case 'string': {
          config[param] = String(env[param])
          break
        }
        case 'object': {
          try {
            const parameterStr = env[param]
            if (parameterStr) {
              const parameterObj = JSON.parse(parameterStr)
              config[param] = parameterObj
            }
          } catch (e) {
            Logger.mainLogger.error(e)
            Logger.mainLogger.error('Unable to JSON parse', env[param])
          }
          break
        }
        case 'boolean': {
          config[param] = String(env[param]).toLowerCase() === 'true'
          break
        }
        default: {
          break
        }
      }
    }
  }

  // Override config from cli args
  const parsedArgs = minimist(args.slice(2))
  for (const param of Object.keys(config)) {
    /* eslint-disable security/detect-object-injection */
    if (parsedArgs[param]) {
      switch (typeof config[param]) {
        case 'number': {
          config[param] = Number(parsedArgs[param])
          break
        }
        case 'string': {
          config[param] = String(parsedArgs[param])
          break
        }
        case 'boolean': {
          if (typeof parsedArgs[param] === 'boolean') {
            config[param] = parsedArgs[param]
          } else {
            config[param] = String(parsedArgs[param]).toLowerCase() === 'true'
          }
          break
        }
        default: {
          break
        }
      }
    }
  }

  // Pull in secrets
  const secretsPath = join(__dirname, '../.secrets')
  const secrets = {}

  if (fs.existsSync(secretsPath)) {
    const lines = fs.readFileSync(secretsPath, 'utf-8').split('\n').filter(Boolean)

    lines.forEach((line) => {
      const [key, value] = line.split('=')
      secrets[key.trim()] = value.trim()
    })

    // Now, secrets contain your secrets, for example:
    // const apiKey = secrets.API_KEY;

    if (secrets['ARCHIVER_PUBLIC_KEY']) config.ARCHIVER_PUBLIC_KEY = secrets['ARCHIVER_PUBLIC_KEY']
    if (secrets['ARCHIVER_SECRET_KEY']) config.ARCHIVER_SECRET_KEY = secrets['ARCHIVER_SECRET_KEY']
    if (secrets['ARCHIVER_HASH_KEY']) config.ARCHIVER_HASH_KEY = secrets['ARCHIVER_HASH_KEY']
  }

  if (config.ARCHIVER_HASH_KEY === '') {
    // Use default hash key if none provided
    // pragma: allowlist nextline secret
    config.ARCHIVER_HASH_KEY = '69fa4195670576c0160d660c3be36556ff8d504725be8a59b5a96509e0c994bc'
  }
  if (config.DEBUG.devPublicKey === '') {
    // Use default dev public key if none provided
    // pragma: allowlist nextline secret
    config.DEBUG.devPublicKey = '774491f80f47fedb119bb861601490f42bc3ea3b57fc63906c0d08e6d777a592'
  }
}

export { config }
