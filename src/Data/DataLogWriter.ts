/**
 * This class implements the log writer logic for all types of data that is received from the distributor, i.e. Receipt, Cycle & OriginalTxData
 */
import * as path from 'path'
import * as fs from 'fs/promises'
import { config } from '../Config'

const LOG_WRITER_CONFIG = config.logWriter

class DataLogWriter {
  logDir: string
  maxLogCounter: number
  totalNumberOfEntries: number
  activeLogFileName: string
  writeQueue: any[]
  cloneWriteQueue: any[]
  isWriting: boolean

  constructor(
    public dataName: string,
    public logCounter: number,
    public maxNumberEntriesPerLog: number
  ) {
    this.logDir = LOG_WRITER_CONFIG.dirName
    this.maxLogCounter = LOG_WRITER_CONFIG.maxLogFiles
    this.totalNumberOfEntries = 0
    this.activeLogFileName = `active-${dataName}-log.txt`
    this.writeQueue = []
    this.cloneWriteQueue = []
    this.isWriting = false
  }

  async init(): Promise<void> {
    // Create log directory if it does not exist.
    // eslint-disable-next-line security/detect-non-literal-fs-filename
    await fs.mkdir(this.logDir, { recursive: true })

    // Check if the active log file exists.
    const activeLogFile = path.join(this.logDir, this.activeLogFileName)
    // console.log('activeLogFile', activeLogFile)

    // Read the active log file name from active-log.txt.
    try {
      // eslint-disable-next-line security/detect-non-literal-fs-filename
      const activeLog = await fs.readFile(activeLogFile, 'utf8')
      const activeLogNumber = parseInt(activeLog.replace(`${this.dataName}-log`, '').replace('.txt', ''))
      if (activeLogNumber > 0) {
        this.logCounter = activeLogNumber
        console.log(`> DataLogWriter: Active log file: ${this.dataName}-log${this.logCounter}.txt`)
        const logFile = path.join(this.logDir, `${this.dataName}-log${this.logCounter}.txt`)
        // eslint-disable-next-line security/detect-non-literal-fs-filename
        const data = await fs.readFile(logFile, { encoding: 'utf8' })
        this.totalNumberOfEntries += data.split('\n').length - 1
        console.log(`> DataLogWriter: Total ${this.dataName} Entries: ${this.totalNumberOfEntries}`)
        if (this.totalNumberOfEntries >= this.maxNumberEntriesPerLog) {
          // Finish the log file with the total number of entries.
          await fs.appendFile(logFile, `End: Number of entries: ${this.totalNumberOfEntries}\n`)
          this.totalNumberOfEntries = 0
          await this.rotateLogFile()
        }
      }
    } catch (err) {
      console.log(`Failed to read active log file: ${err}`)
    }

    if (this.logCounter === 1) await this.setActiveLog()
  }

  async deleteOldLogFiles(prefix: string): Promise<any> {
    // eslint-disable-next-line security/detect-non-literal-fs-filename
    const logFiles = await fs.readdir(this.logDir)
    const oldLogFiles = logFiles.filter((file) => file.startsWith(`${prefix}-${this.dataName}-log`))

    console.log(`> DataLogWriter: Rotating old log files: ${oldLogFiles}`)
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

  async writeLog(data: any): Promise<void> {
    this.writeQueue = [...this.writeQueue, ...data]
    if (!this.isWriting) {
      this.cloneWriteQueue = [...this.writeQueue]
      this.writeQueue = []
      await this.insertLog()
    }
    console.log(this.dataName, `Write queue length: ${this.writeQueue.length}`)
  }

  async insertLog(): Promise<void> {
    this.isWriting = true
    const logFile = path.join(this.logDir, `${this.dataName}-log${this.logCounter}.txt`)
    const timestamp = new Date().toISOString()
    console.log(`Writing: ${timestamp} ${this.cloneWriteQueue.length} times`)

    let appendPromises = this.cloneWriteQueue.map((entry) => fs.appendFile(logFile, entry))
    this.cloneWriteQueue = []
    await Promise.all(appendPromises)
    // clear the appendPromises array
    console.log(`Written: ${timestamp} ${appendPromises.length} times`)
    this.totalNumberOfEntries += appendPromises.length
    if (this.totalNumberOfEntries >= this.maxNumberEntriesPerLog) {
      // eslint-disable-next-line security/detect-non-literal-fs-filename
      await fs.appendFile(logFile, `End: Number of entries: ${this.totalNumberOfEntries}\n`)
      this.totalNumberOfEntries = 0
      await this.rotateLogFile()
      await this.setActiveLog()
    }
    appendPromises = []

    if (this.writeQueue.length > 0) {
      this.cloneWriteQueue = [...this.writeQueue]
      this.writeQueue = []
      await this.insertLog()
    } else this.isWriting = false
  }

  async setActiveLog(): Promise<void> {
    // Write the active log file name to active-log.txt.
    const activeLogFile = path.join(this.logDir, this.activeLogFileName)
    // Create the current logCounter file.
    const logFile = path.join(this.logDir, `${this.dataName}-log${this.logCounter}.txt`)
    // eslint-disable-next-line security/detect-non-literal-fs-filename
    await fs.appendFile(logFile, '')
    // eslint-disable-next-line security/detect-non-literal-fs-filename
    await fs.writeFile(activeLogFile, `${this.dataName}-log${this.logCounter}.txt`)
  }
}

export let CycleLogWriter: DataLogWriter
export let ReceiptLogWriter: DataLogWriter
export let OriginalTxDataLogWriter: DataLogWriter

export async function initLogWriter(): Promise<void> {
  CycleLogWriter = new DataLogWriter('CYCLE', 1, LOG_WRITER_CONFIG.maxCycleEntries)
  ReceiptLogWriter = new DataLogWriter('RECEIPT', 1, LOG_WRITER_CONFIG.maxReceiptEntries)
  OriginalTxDataLogWriter = new DataLogWriter('ORIGINAL_TX', 1, LOG_WRITER_CONFIG.maxOriginalTxEntries)
  await Promise.all([CycleLogWriter.init(), ReceiptLogWriter.init(), OriginalTxDataLogWriter.init()])
}
