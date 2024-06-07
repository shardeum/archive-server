import { readFileSync } from 'fs'
import { resolve } from 'path'
import { join } from 'path'
import { overrideDefaultConfig, config } from '../src/Config'
import * as Crypto from '../src/Crypto'
import * as db from '../src/dbstore/sqlite3storage'
import * as dbstore from '../src/dbstore'
import * as CycleDB from '../src/dbstore/cycles'
import { startSaving } from '../src/saveConsoleOutput'
import * as Logger from '../src/Logger'

const patchCycleData = false

const start = async (): Promise<void> => {
  // Override default config params from config file, env vars, and cli args
  const file = join(process.cwd(), 'archiver-config.json')
  overrideDefaultConfig(file)
  // Set crypto hash keys from config
  const hashKey = config.ARCHIVER_HASH_KEY
  Crypto.setCryptoHashKey(hashKey)
  let logsConfig
  try {
    logsConfig = StringUtils.safeJsonParse(readFileSync(resolve(__dirname, '../archiver-log.json'), 'utf8'))
  } catch (err) {
    console.log('Failed to parse archiver log file:', err)
  }
  const logDir = `${config.ARCHIVER_LOGS}/${config.ARCHIVER_IP}_${config.ARCHIVER_PORT}`
  const baseDir = '.'
  logsConfig.dir = logDir
  Logger.initLogger(baseDir, logsConfig)
  if (logsConfig.saveConsoleOutput) {
    startSaving(join(baseDir, logsConfig.dir))
  }
  await dbstore.initializeDB(config)

  const lastStoredCycleCount = await CycleDB.queryCyleCount()
  const lastStoredCycle = (await CycleDB.queryLatestCycleRecords(1))[0]
  console.log('lastStoredCycleCount', lastStoredCycleCount, 'lastStoredCycleCounter', lastStoredCycle.counter)

  if (lastStoredCycleCount > 0 && lastStoredCycle.counter !== lastStoredCycleCount - 1) {
    console.error('Stored cycle count does not match the last cycle counter')
  }
  await checkCycleData(0, lastStoredCycle.counter)
  console.log('Cycle data check complete.')
}

/**
 * Generate an array of numbers within a specified range.
 */
function generateNumberArray(startNumber: number, endNumber: number): number[] {
  const numberOfItems = endNumber - startNumber + 1
  const items = Array.from({ length: numberOfItems }, (_, i) => startNumber + i)
  return items
}

async function checkCycleData(startCycleNumber = 0, latestCycleNumber: number): Promise<void> {
  try {
    // Divide blocks into batches (e.g., batches of 1000 cycles each)
    const batchSize = 1000
    const cycleBatches: number[][] = []
    let end = startCycleNumber + batchSize
    for (let start = startCycleNumber; start <= latestCycleNumber; ) {
      if (end > latestCycleNumber) end = latestCycleNumber
      cycleBatches.push(generateNumberArray(start, end))
      start = end + 1
      end += batchSize
    }

    // Query cycle in batches in parallel using Promise.allSettled
    const promises = cycleBatches.map(async (cycleNumberBatch: number[]) => {
      const sql =
        'SELECT counter FROM cycles WHERE counter IN (' + cycleNumberBatch + ') ORDER BY counter ASC'
      return db.all(sql)
    })

    const results = await Promise.allSettled(promises)

    // Process results
    results.forEach((result, index) => {
      if (result.status === 'fulfilled') {
        const cycles = cycleBatches[index]
        const existingCycles = result.value.map((row: any) => (row ? row.counter : 0))
        if (existingCycles.length !== cycles.length) console.log(existingCycles)
        const missingCycles = cycles.filter((cycle) => !existingCycles.includes(cycle))
        if (missingCycles.length > 0) console.log('Missing cycles:', missingCycles)
      } else {
        console.error('Error checking cycles existence:', result.reason)
      }
    })
  } catch (error) {
    console.error('Error checking cycle data:', error)
  }
}

start()
