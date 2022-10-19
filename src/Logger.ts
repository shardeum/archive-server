import * as log4js from 'log4js'
import { existsSync, mkdirSync } from 'fs'
const stringify = require('fast-stable-stringify')
const log4jsExtend = require('log4js-extend')

interface Logger {
  baseDir: string
  config: LogsConfiguration
  logDir: string
  log4Conf: any
}

export interface LogsConfiguration {
  saveConsoleOutput?: boolean
  dir?: string
  files?: {
    main?: string
    fatal?: string
    net?: string
    app?: string
  }
  options?: {
    appenders?: {
      out?: {
        type?: string
      }
      main?: {
        type?: string
        maxLogSize?: number
        backups?: number
      }
      fatal?: {
        type?: string
        maxLogSize?: number
        backups?: number
      }
      errorFile?: {
        type?: string
        maxLogSize?: number
        backups?: number
      }
      errors?: {
        type?: string
        maxLogSize?: number
        backups?: number
      }
    }
  }
  categories?: {
    default?: {
      appenders?: string[]
      level?: string
    }
    main?: {
      appenders?: string[]
      level?: string
    }
    fatal?: {
      appenders?: string[]
      level?: string
    }
  }
}

class Logger {
  constructor(baseDir: string, config: LogsConfiguration) {
    this.baseDir = baseDir
    this.config = config
    this.logDir = ''
    this.log4Conf = null
    this._setupLogs()
  }

  // Checks if the configuration has the required components
  _checkValidConfig() {
    const config = this.config
    if (!config.dir) throw Error('Fatal Error: Log directory not defined.')
    if (!config.files || typeof config.files !== 'object')
      throw Error('Fatal Error: Valid log file locations not provided.')
  }

  // Add filenames to each appender of type 'file'
  _addFileNamesToAppenders() {
    const conf = this.log4Conf
    for (const key in conf.appenders) {
      const appender = conf.appenders[key]
      if (appender.type !== 'file') continue
      appender.filename = `${this.logDir}/${key}.log`
    }
  }

  _configureLogs() {
    return log4js.configure(this.log4Conf)
  }

  // Get the specified logger
  getLogger(logger: string) {
    return log4js.getLogger(logger)
  }

  // Setup the logs with the provided configuration using the base directory provided for relative paths
  _setupLogs() {
    const baseDir = this.baseDir
    const config = this.config

    if (!baseDir) throw Error('Fatal Error: Base directory not defined.')
    if (!config) throw Error('Fatal Error: No configuration provided.')
    this._checkValidConfig()

    // Makes specified directory if it doesn't exist
    if (config.dir) {
      let allArchiversLogDir = `${baseDir}/${config.dir.split('/')[0]}`
      this.getLogger('main').info('allArchiversLogDir', allArchiversLogDir)
      if (!existsSync(allArchiversLogDir)) mkdirSync(allArchiversLogDir)
    }

    this.logDir = `${baseDir}/${config.dir}`
    if (!existsSync(this.logDir)) mkdirSync(this.logDir)
    // Read the log config from log config file
    this.log4Conf = config.options
    log4jsExtend(log4js)
    this._addFileNamesToAppenders()
    this._configureLogs()
    this.getLogger('main').info('Logger initialized.')
  }

  // Tells this module that the server is shutting down, returns a Promise that resolves when all logs have been written to file, sockets are closed, etc.
  shutdown() {
    return new Promise((resolve) => {
      log4js.shutdown(() => {
        resolve('done')
      })
    })
  }
}

export let mainLogger: any
export let fatalLogger: any
export let errorLogger: any

export function initLogger(baseDir: string, logsConfig: LogsConfiguration) {
  let logger = new Logger(baseDir, logsConfig)
  mainLogger = logger.getLogger('main')
  fatalLogger = logger.getLogger('fatal')
  errorLogger = logger.getLogger('errorFile')
}

export default Logger
