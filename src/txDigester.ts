import { join } from 'path'
import * as cron from 'node-cron'
import * as dbstore from './dbstore'
import * as txDigesterDB from './txDigester/index'
import * as txDigestFunctions from './txDigester/txDigestFunctions'
import { overrideDefaultConfig, config } from './Config'
import * as CycleDB from './dbstore/cycles'
import * as Crypto from './Crypto'
import { Utils as StringUtils } from '@shardus/types'
import { readFileSync } from 'fs'
import { resolve } from 'path'
import * as Logger from './Logger'
import { startSaving } from './saveConsoleOutput'
import axios from 'axios'

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
  const logDir = `${config.ARCHIVER_LOGS}/txDigester`
  const baseDir = '.'
  logsConfig.dir = logDir
  Logger.initLogger(baseDir, logsConfig)
  if (logsConfig.saveConsoleOutput) {
    startSaving(join(baseDir, logsConfig.dir))
  }

  await dbstore.initializeDB(config)

  await txDigesterDB.initializeDB(config)

  const ARCHIVER_STATUS_CHECK_URL = `http://${config.ARCHIVER_IP}:${config.ARCHIVER_PORT}/status`

  cron.schedule('*/5 * * * *', async () => {
    console.log('Running cron task....')
    console.log('Checking archiver status....')
    const archiverStatusResp = await axios.get(ARCHIVER_STATUS_CHECK_URL)
    const isArchiverActive: boolean = archiverStatusResp.data.statusResp.isActive
    console.log('isArchiverActive: ', isArchiverActive)

    if (isArchiverActive) {
      const lastProcessedTxDigest = await txDigestFunctions.getLastProcessedTxDigest()
      console.log('lastProcessedTxDigest by txDigester: ', lastProcessedTxDigest)
      const lastCheckedCycle = lastProcessedTxDigest ? lastProcessedTxDigest.cycleEnd : -1
      console.log('lastCheckedCycle by txDigester: ', lastCheckedCycle)

      const latestCycleRecords = await CycleDB.queryLatestCycleRecords(1)
      const latestCycleCounter = latestCycleRecords.length > 0 ? latestCycleRecords[0].counter : -1
      console.log('latestCycleCounter reported by Archiver: ', latestCycleCounter)

      const latestSyncedCycleCounter = latestCycleCounter - config.txDigest.syncDelay
      if (latestSyncedCycleCounter - lastCheckedCycle >= config.txDigest.cycleDiff) {
        await txDigestFunctions.processAndInsertTxDigests(lastCheckedCycle + 1, latestSyncedCycleCounter)
      }
    } else {
      console.log('Archiver is not active. Skipping txDigest processing....')
    }
  })
}

start()
