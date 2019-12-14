import { readFileSync } from 'fs'
import merge = require('deepmerge')
import minimist = require('minimist')

export interface Config {
  [index: string]: string | number | boolean
  ARCHIVER_IP: string
  ARCHIVER_PORT: number
  ARCHIVER_HASH_KEY: string
  ARCHIVER_PUBLIC_KEY: string
  ARCHIVER_SECRET_KEY: string
  ARCHIVER_EXISTING: string
  ARCHIVER_DB: string
}

let config: Config = {
  ARCHIVER_IP: 'localhost',
  ARCHIVER_PORT: 4000,
  ARCHIVER_HASH_KEY:
    '69fa4195670576c0160d660c3be36556ff8d504725be8a59b5a96509e0c994bc',
  ARCHIVER_PUBLIC_KEY:
    '758b1c119412298802cd28dbfa394cdfeecc4074492d60844cc192d632d84de3',
  ARCHIVER_SECRET_KEY:
    '3be00019f23847529bd63e41124864983175063bb524bd54ea3c155f2fa12969758b1c119412298802cd28dbfa394cdfeecc4074492d60844cc192d632d84de3',
  ARCHIVER_EXISTING: '[]',
  ARCHIVER_DB: './archiver-db.sqlite',
}

export function overrideDefaultConfig(
  file: string,
  env: NodeJS.ProcessEnv,
  args: string[]
) {
  // Override config from config file
  try {
    const fileConfig = JSON.parse(readFileSync(file, { encoding: 'utf8' }))
    const overwriteMerge = (target: [], source: [], options: {}): [] => source
    config = merge(config, fileConfig, { arrayMerge: overwriteMerge })
  } catch (err) {
    if (err.code !== 'ENOENT') {
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
