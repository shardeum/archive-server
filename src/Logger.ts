import * as log4js from 'log4js'
import { existsSync, mkdirSync } from 'fs'
const log4jsExtend = require('log4js-extend') // eslint-disable-line @typescript-eslint/no-var-requires

interface Logger {
  baseDir: string
  config: LogsConfiguration
  logDir: string
  log4Conf: any // eslint-disable-line @typescript-eslint/no-explicit-any
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
    out?: {
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
  _checkValidConfig(): void {
    const config = this.config
    if (!config.dir) throw Error('Fatal Error: Log directory not defined.')
    if (!config.files || typeof config.files !== 'object')
      throw Error('Fatal Error: Valid log file locations not provided.')
  }

  // Add filenames to each appender of type 'file'
  _addFileNamesToAppenders(): void {
    const conf = this.log4Conf
    for (const key in conf.appenders) {
      const appender = conf.appenders[key] // eslint-disable-line security/detect-object-injection
      if (appender.type !== 'file') continue
      appender.filename = `${this.logDir}/${key}.log`
    }
  }

  _configureLogs(): log4js.Log4js {
    return log4js.configure(this.log4Conf)
  }

  // Get the specified logger
  getLogger(logger: string): log4js.Logger {
    return log4js.getLogger(logger)
  }

  // Setup the logs with the provided configuration using the base directory provided for relative paths
  _setupLogs(): void {
    const baseDir = this.baseDir
    const config = this.config

    if (!baseDir) throw Error('Fatal Error: Base directory not defined.')
    if (!config) throw Error('Fatal Error: No configuration provided.')
    this._checkValidConfig()

    // Makes specified directory if it doesn't exist
    if (config.dir) {
      const allArchiversLogDir = `${baseDir}/${config.dir.split('/')[0]}`
      this.getLogger('main').info('allArchiversLogDir', allArchiversLogDir)
      if (!existsSync(allArchiversLogDir)) mkdirSync(allArchiversLogDir) // eslint-disable-line security/detect-non-literal-fs-filename
    }

    this.logDir = `${baseDir}/${config.dir}`
    if (!existsSync(this.logDir)) mkdirSync(this.logDir) // eslint-disable-line security/detect-non-literal-fs-filename
    // Read the log config from log config file
    this.log4Conf = config.options
    log4jsExtend(log4js)
    this._addFileNamesToAppenders()
    this._configureLogs()
    this.getLogger('main').info('Logger initialized.')
  }

  // Tells this module that the server is shutting down, returns a Promise that resolves when all logs have been written to file, sockets are closed, etc.
  shutdown(): Promise<string> {
    return new Promise((resolve) => {
      log4js.shutdown(() => {
        resolve('done')
      })
    })
  }
}

export let mainLogger: log4js.Logger
export let fatalLogger: log4js.Logger
export let errorLogger: log4js.Logger

export function initLogger(baseDir: string, logsConfig: LogsConfiguration): void {
  const logger = new Logger(baseDir, logsConfig)
  mainLogger = logger.getLogger('main')
  fatalLogger = logger.getLogger('fatal')
  errorLogger = logger.getLogger('errorFile')
}

export default Logger
