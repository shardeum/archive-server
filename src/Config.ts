import { readFileSync } from 'fs'
import { ArchiverNodeInfo } from './State'
import * as Logger from './Logger'
import * as merge from 'deepmerge'
import * as minimist from 'minimist'

export interface Config {
  [index: string]: object | string | number | boolean
  ARCHIVER_IP: string
  ARCHIVER_PORT: number
  ARCHIVER_HASH_KEY: string
  ARCHIVER_PUBLIC_KEY: string
  ARCHIVER_SECRET_KEY: string
  ARCHIVER_EXISTING: ArchiverNodeInfo[]
  ARCHIVER_DB: string
  DATASENDER_TIMEOUT: number
  RATE_LIMIT: number // number of allowed request per second,
  N_NODELIST: number
  STATISTICS: {
    save: boolean
    interval: number
  }
  MODE: string
  DEBUG: {
    hashedDevAuth?: string
    devPublicKey?: string
  }
  experimentalSnapshot: boolean
  VERBOSE: boolean
}

let config: Config = {
  ARCHIVER_IP: 'localhost',
  ARCHIVER_PORT: 4000,
  ARCHIVER_HASH_KEY: '69fa4195670576c0160d660c3be36556ff8d504725be8a59b5a96509e0c994bc',
  ARCHIVER_PUBLIC_KEY: '758b1c119412298802cd28dbfa394cdfeecc4074492d60844cc192d632d84de3',
  ARCHIVER_SECRET_KEY:
    '3be00019f23847529bd63e41124864983175063bb524bd54ea3c155f2fa12969758b1c119412298802cd28dbfa394cdfeecc4074492d60844cc192d632d84de3',
  ARCHIVER_EXISTING: [],
  ARCHIVER_LOGS: 'archiver-logs',
  ARCHIVER_DB: 'archiver-db.sqlite',
  DATASENDER_TIMEOUT: 1000 * 60 * 5,
  RATE_LIMIT: 100, // 100 req per second,
  N_NODELIST: 30, // number of active node list GET /nodelist should emit but if the total active nodelist is less than said value it will emit all the node list.
  STATISTICS: {
    save: true,
    interval: 1,
  },
  MODE: 'debug', // 'debug'/'release'
  DEBUG: {
    hashedDevAuth: '',
    devPublicKey: '',
  },
  experimentalSnapshot: true,
  VERBOSE: false,
}

export function overrideDefaultConfig(file: string, env: NodeJS.ProcessEnv, args: string[]) {
  // Override config from config file
  try {
    const fileConfig = JSON.parse(readFileSync(file, { encoding: 'utf8' }))
    const overwriteMerge = (target: [], source: [], options: {}): [] => source
    config = merge(config, fileConfig, { arrayMerge: overwriteMerge })
  } catch (err) {
    if ((err as any).code !== 'ENOENT') {
      console.warn('Failed to parse config file:', err)
    }
  }

  // Override config from env vars
  for (const param in config) {
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
            var parameterStr = env[param]
            if (parameterStr) {
              let parameterObj = JSON.parse(parameterStr)
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
        }
      }
    }
  }

  // Override config from cli args
  const parsedArgs = minimist(args.slice(2))
  for (const param of Object.keys(config)) {
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
        }
      }
    }
  }
}

export { config }
