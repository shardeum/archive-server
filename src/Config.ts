import * as fs from 'fs'
import * as Logger from './Logger'
import * as merge from 'deepmerge'
import * as minimist from 'minimist'
import { join } from 'path'
import { Utils as StringUtils } from '@shardus/types'

export interface Config {
  [index: string]: object | string | number | boolean
  ARCHIVER_IP: string
  ARCHIVER_PORT: number
  ARCHIVER_HASH_KEY: string
  ARCHIVER_PUBLIC_KEY: string
  ARCHIVER_SECRET_KEY: string
  ARCHIVER_DB: string // Archiver DB folder name
  ARCHIVER_DATA: {
    cycleDB: string
    accountDB: string
    transactionDB: string
    receiptDB: string
    originalTxDataDB: string
  }
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
  ARCHIVER_MODE: string
  DevPublicKey: string
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
  limitToArchiversOnly: boolean
  verifyReceiptData: boolean
  verifyAppReceiptData: boolean
  verifyAccountData: boolean
  skipGlobalTxReceiptVerification: boolean // To skip verification of global tx receipts for now
  REQUEST_LIMIT: {
    MAX_ACCOUNTS_PER_REQUEST: number
    MAX_RECEIPTS_PER_REQUEST: number
    MAX_ORIGINAL_TXS_PER_REQUEST: number
    MAX_CYCLES_PER_REQUEST: number
    MAX_BETWEEN_CYCLES_PER_REQUEST: number
  }
  cycleRecordsCache: {
    enabled: boolean
  }
  newPOQReceipt: boolean
  waitingTimeForMissingTxData: number // Wait time in ms for missing tx data before collecting from other archivers
  gossipToMoreArchivers: true // To gossip to more archivers in addition to adjacent archivers
  randomGossipArchiversCount: 2 // Number of random archivers to gossip to
  subscribeToMoreConsensors: boolean // To subscribe to more consensors when the number of active archivers is less than 4
  extraConsensorsToSubscribe: 1 // Number of extra consensors to subscribe to
  // For debugging gossip data, set this to true. This will save only the gossip data received from the gossip archivers.
  saveOnlyGossipData: boolean
  // For debugging purpose, set this to true to stop gossiping tx data
  stopGossipTxData: boolean
  usePOQo: boolean
  // The percentage of votes required to confirm transaction
  requiredVotesPercentage: number
  // max number of recent cycle shard data to keep
  maxCyclesShardDataToKeep: number
  // the number of cycles within which we want to keep \changes to a config*/
  configChangeMaxCyclesToKeep: number
  // the number of config changes to keep*/
  configChangeMaxChangesToKeep: number
  receiptLoadTrakerInterval: number // Interval to track the receipt load
  receiptLoadTrakerLimit: number // Limit to track the receipt load
  lastActivityCheckInterval: number // Interval to check last activity
  lastActivityCheckTimeout: number // Timeout to check last activity
}

let config: Config = {
  ARCHIVER_IP: '127.0.0.1',
  ARCHIVER_PORT: 4000,
  ARCHIVER_HASH_KEY: '',
  ARCHIVER_PUBLIC_KEY: '',
  ARCHIVER_SECRET_KEY: '',
  ARCHIVER_LOGS: 'archiver-logs',
  ARCHIVER_DB: 'archiver-db',
  ARCHIVER_DATA: {
    cycleDB: 'cycles.sqlite3',
    accountDB: 'accounts.sqlite3',
    transactionDB: 'transactions.sqlite3',
    receiptDB: 'receipts.sqlite3',
    originalTxDataDB: 'originalTxsData.sqlite3',
  },
  EXISTING_ARCHIVER_DB_PATH: '',
  DATASENDER_TIMEOUT: 1000 * 60 * 5,
  RATE_LIMIT: 100, // 100 req per second,
  N_NODE_REJECT_PERCENT: 5, // Percentage of old nodes to remove from nodelist
  N_NODELIST: 10, // number of active node list GET /nodelist should emit but if the total active nodelist is less than said value it will emit all the node list.
  N_RANDOM_NODELIST_BUCKETS: 100,
  RECEIPT_CONFIRMATIONS: 5,
  STATISTICS: {
    save: true,
    interval: 1,
  },
  ARCHIVER_MODE: 'release', // 'debug'/'release'
  DevPublicKey: '',
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
  globalNetworkAccount:
    process.env.GLOBAL_ACCOUNT || '1000000000000000000000000000000000000000000000000000000000000001', //this address will change in the future
  maxValidatorsToServe: 10, // max number of validators to serve accounts data during restore mode
  limitToArchiversOnly: true,
  verifyReceiptData: true,
  verifyAccountData: true,
  verifyAppReceiptData: true,
  skipGlobalTxReceiptVerification: true,
  REQUEST_LIMIT: {
    MAX_ACCOUNTS_PER_REQUEST: 1000,
    MAX_RECEIPTS_PER_REQUEST: 100,
    MAX_ORIGINAL_TXS_PER_REQUEST: 100,
    MAX_CYCLES_PER_REQUEST: 100,
    MAX_BETWEEN_CYCLES_PER_REQUEST: 100,
  },
  cycleRecordsCache: {
    enabled: false,
  },
  newPOQReceipt: false,
  waitingTimeForMissingTxData: 2000, // in ms
  gossipToMoreArchivers: true,
  randomGossipArchiversCount: 2,
  subscribeToMoreConsensors: true,
  extraConsensorsToSubscribe: 1,
  saveOnlyGossipData: false,
  stopGossipTxData: false,
  usePOQo: true,
  requiredVotesPercentage: 2 / 3,
  maxCyclesShardDataToKeep: 10,
  configChangeMaxCyclesToKeep: 5,
  configChangeMaxChangesToKeep: 1000,
  receiptLoadTrakerInterval: 15 * 1000,
  receiptLoadTrakerLimit: 10,
  lastActivityCheckInterval: 15 * 1000,
  lastActivityCheckTimeout: 30 * 1000,
}
// Override default config params from config file, env vars, and cli args
export async function overrideDefaultConfig(file: string): Promise<void> {
  const env = process.env
  const args = process.argv

  // Override config from config file
  try {
    // eslint-disable-next-line security/detect-non-literal-fs-filename
    const fileConfig = StringUtils.safeJsonParse(fs.readFileSync(file, { encoding: 'utf8' }))
    const overwriteMerge = (target: [], source: []): [] => source
    config = merge(config, fileConfig, { arrayMerge: overwriteMerge })
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
              const parameterObj = StringUtils.safeJsonParse(parameterStr)
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
  if (config.DevPublicKey === '') {
    // Use default dev public key if none provided
    // pragma: allowlist nextline secret
    config.DevPublicKey = '774491f80f47fedb119bb861601490f42bc3ea3b57fc63906c0d08e6d777a592'
  }
}

export function updateConfig(newConfig: Partial<Config>): Config {
  for (const key in newConfig) {
    if (typeof newConfig[key] !== typeof config[key])
      throw new Error(
        `Value with incorrect type passed to update the Archiver Config: ${key}:${
          newConfig[key]
        } of type ${typeof newConfig[key]}`
      )
  }
  config = merge(config, newConfig)
  Logger.mainLogger.info('Updated Archiver Config:', config)
  return config
}

export { config }
