/**
 * This class implements the log writer logic for all types of data that is received from the distributor, i.e. Receipt, Cycle & OriginalTxData
 */
import * as path from 'path'
import * as fs from 'fs/promises'
import { config } from '../Config'
import { createWriteStream, WriteStream, existsSync } from 'fs'

const LOG_WRITER_CONFIG = config.dataLogWriter

interface deleteOldLogFilesResponse {
  oldLogFiles: string[]
  promises: Promise<void>[]
}

class DataLogWriter {
  logDir: string
  maxLogCounter: number
  dataLogWriteStream: WriteStream | null
  dataWriteIndex: number
  dataLogFilePath: string
  totalNumberOfEntries: number
  activeLogFileName: string
  activeLogFilePath: string
  writeQueue: string[]
  isWriting: boolean

  constructor(
    public dataName: string,
    public logCounter: number,
    public maxNumberEntriesPerLog: number
  ) {
    this.logDir = `${LOG_WRITER_CONFIG.dirName}/${config.ARCHIVER_IP}_${config.ARCHIVER_PORT}`
    this.maxLogCounter = LOG_WRITER_CONFIG.maxLogFiles
    this.dataLogWriteStream = null
    this.dataWriteIndex = 0
    this.activeLogFileName = `active-${dataName}-log.txt`
    this.activeLogFilePath = path.join(this.logDir, this.activeLogFileName)
    this.dataLogFilePath = path.join(this.logDir, `${dataName}-log${logCounter}.txt`)
    this.totalNumberOfEntries = 0
    this.writeQueue = []
    this.isWriting = false
  }

  async init(): Promise<void> {
    // Create log directory if it does not exist.
    // eslint-disable-next-line security/detect-non-literal-fs-filename
    await fs.mkdir(this.logDir, { recursive: true })
    try {
      // eslint-disable-next-line security/detect-non-literal-fs-filename
      if (existsSync(this.activeLogFilePath)) {
        // Read the active log file name from active-log.txt.
        // eslint-disable-next-line security/detect-non-literal-fs-filename
        const activeLog = await fs.readFile(this.activeLogFilePath, 'utf8')
        const activeLogNumber = parseInt(activeLog.replace(`${this.dataName}-log`, '').replace('.txt', ''))
        if (activeLogNumber > 0) {
          this.logCounter = activeLogNumber
          console.log(`> DataLogWriter: Active log file: ${this.dataName}-log${this.logCounter}.txt`)
          this.dataLogFilePath = path.join(this.logDir, `${this.dataName}-log${this.logCounter}.txt`)
          // eslint-disable-next-line security/detect-non-literal-fs-filename
          const data = await fs.readFile(this.dataLogFilePath, {
            encoding: 'utf8',
          })
          this.totalNumberOfEntries += data.split('\n').length - 1
          console.log(`> DataLogWriter: Total ${this.dataName} Entries: ${this.totalNumberOfEntries}`)
          // eslint-disable-next-line security/detect-non-literal-fs-filename
          this.dataLogWriteStream = createWriteStream(this.dataLogFilePath, { flags: 'a' })
          if (this.totalNumberOfEntries >= this.maxNumberEntriesPerLog) {
            // Finish the log file with the total number of entries.
            await this.appendData(`End: Number of entries: ${this.totalNumberOfEntries}\n`)
            await this.endStream()
            this.totalNumberOfEntries = 0
            await this.rotateLogFile()
            await this.setActiveLog()
          }
        }
      } else {
        // eslint-disable-next-line security/detect-non-literal-fs-filename
        await fs.writeFile(this.activeLogFilePath, `${this.dataName}-log${this.logCounter}.txt`)
      }
    } catch (err) {
      console.log(`Failed to read active log file: ${err}`)
    }

    if (this.logCounter === 1) await this.setActiveLog()
  }

  async deleteOldLogFiles(prefix: string): Promise<deleteOldLogFilesResponse> {
    // eslint-disable-next-line security/detect-non-literal-fs-filename
    const logFiles = await fs.readdir(this.logDir)
    const oldLogFiles = logFiles.filter((file) => file.startsWith(`${prefix}-${this.dataName}-log`))

    if (config.VERBOSE) console.log(`> DataLogWriter: Rotating old log files: ${oldLogFiles}`)
    const promises: Promise<void>[] = []
    for (const file of oldLogFiles) {
      // eslint-disable-next-line security/detect-non-literal-fs-filename
      promises.push(fs.unlink(path.join(this.logDir, file)))
    }
    return { oldLogFiles, promises }
  }

  renameAsOldFiles(promises: Promise<void>[], start: number, end: number): void {
    for (let i = start; i <= end; i++) {
      promises.push(
        // eslint-disable-next-line security/detect-non-literal-fs-filename
        fs.rename(
          path.join(this.logDir, `${this.dataName}-log${i}.txt`),
          path.join(this.logDir, `old-${this.dataName}-log${i}.txt`)
        )
      )
    }
  }

  async rotateOldLogs(prefix: string, start: number, end: number): Promise<void> {
    const { oldLogFiles, promises } = await this.deleteOldLogFiles(prefix)
    if (start === this.maxLogCounter / 2 + 1) {
      if (oldLogFiles.length > 0) {
        // Renaming log files #6 to 10 from log[X].txt to old-log[X].txt.
        this.renameAsOldFiles(promises, start, end)
      }
    } else {
      // Renaming log files #1 to 5 from log[X].txt to old-log[X].txt.
      this.renameAsOldFiles(promises, start, end)
    }
    if (promises.length > 0) await Promise.all(promises)
  }

  async rotateLogFile(): Promise<void> {
    this.logCounter++
    if (this.logCounter === this.maxLogCounter / 2) {
      await this.rotateOldLogs('old', this.maxLogCounter / 2 + 1, this.maxLogCounter)
    }
    if (this.logCounter === this.maxLogCounter) {
      await this.rotateOldLogs('old', 1, this.maxLogCounter / 2)
    }
    if (this.logCounter > this.maxLogCounter) this.logCounter = 1
    console.log(`> DataLogWriter: Rotated log file: ${this.dataName}-log${this.logCounter}.txt`)
  }

  async writeToLog(data: string): Promise<void> {
    this.writeQueue.push(data)
    if (!this.isWriting) await this.insertDataLog()
    // else console.log('❌❌❌ Already writing...')
  }

  async insertDataLog(): Promise<void> {
    this.isWriting = true
    while (this.writeQueue.length) {
      try {
        for (let i = 0; i < this.writeQueue.length; i++) {
          if (this.totalNumberOfEntries === this.maxNumberEntriesPerLog) {
            await this.appendData(`End: Number of entries: ${this.totalNumberOfEntries}\n`)
            await this.endStream()
            this.totalNumberOfEntries = 0
            await this.rotateLogFile()
            await this.setActiveLog()
          }
          // eslint-disable-next-line security/detect-object-injection
          await this.appendData(this.writeQueue[i])
          this.dataWriteIndex += 1
          this.totalNumberOfEntries += 1
        }
        // console.log('-->> Write queue length: ', this.writeQueue.length)
        this.writeQueue.splice(0, this.dataWriteIndex)
        // console.log('-->> Data write Index: ', this.dataWriteIndex)
        this.dataWriteIndex = 0
      } catch (e) {
        console.error('Error while writing data to log file', e)
      }
    }
    this.isWriting = false
  }

  async setActiveLog(): Promise<void> {
    // Write the active log file name to active-log.txt.
    this.activeLogFilePath = path.join(this.logDir, this.activeLogFileName)
    // Initialising new data log file.
    this.dataLogFilePath = path.join(this.logDir, `${this.dataName}-log${this.logCounter}.txt`)
    // eslint-disable-next-line security/detect-non-literal-fs-filename
    await fs.appendFile(this.dataLogFilePath, '')
    // eslint-disable-next-line security/detect-non-literal-fs-filename
    this.dataLogWriteStream = createWriteStream(this.dataLogFilePath, { flags: 'a' })
    // Set the name of the new data log file name to the active-log file.
    // eslint-disable-next-line security/detect-non-literal-fs-filename
    await fs.writeFile(this.activeLogFilePath, `${this.dataName}-log${this.logCounter}.txt`)
  }

  appendData(data: string): Promise<void> {
    // Check if we should continue writing
    const canContinueToWrite = this.dataLogWriteStream!.write(data)

    if (!canContinueToWrite) {
      // Wait for drain event to continue writing
      return new Promise((resolve) => {
        this.dataLogWriteStream!.once('drain', resolve)
      })
    }
    return Promise.resolve()
  }

  endStream(): Promise<void> {
    return new Promise((resolve, reject) => {
      try {
        this.dataLogWriteStream!.end(() => {
          console.log(`✅ Finished writing ${this.totalNumberOfEntries}.`)
          resolve()
        })
      } catch (e) {
        console.error('Error while ending stream', e)
        reject(e)
      }
    })
  }
}

export let CycleLogWriter: DataLogWriter
export let ReceiptLogWriter: DataLogWriter
export let OriginalTxDataLogWriter: DataLogWriter

export async function initDataLogWriter(): Promise<void> {
  CycleLogWriter = new DataLogWriter('cycle', 1, LOG_WRITER_CONFIG.maxCycleEntries)
  ReceiptLogWriter = new DataLogWriter('receipt', 1, LOG_WRITER_CONFIG.maxReceiptEntries)
  OriginalTxDataLogWriter = new DataLogWriter('originalTx', 1, LOG_WRITER_CONFIG.maxOriginalTxEntries)
  await Promise.all([CycleLogWriter.init(), ReceiptLogWriter.init(), OriginalTxDataLogWriter.init()])
}
