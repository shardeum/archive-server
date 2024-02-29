import { readFileSync } from 'fs'
import { resolve } from 'path'
import { join } from 'path'
import { overrideDefaultConfig, config } from '../src/Config'
import * as Crypto from '../src/Crypto'
import * as dbstore from '../src/dbstore'
import * as CycleDB from '../src/dbstore/cycles'
import { startSaving } from '../src/saveConsoleOutput'
import * as Logger from '../src/Logger'
import { P2P } from '@shardus/types'

const archiversAtShutdown = [
  {
    ip: '72.14.186.172',
    port: 4000,
    publicKey: '7af699dd711074eb96a8d1103e32b589e511613ebb0c6a789a9e8791b2b05f34',
  },
  {
    ip: '50.116.36.114',
    port: 4000,
    publicKey: '2db7c949632d26b87d7e7a5a4ad41c306f63ee972655121a37c5e4f52b00a542',
  },
  {
    ip: '192.155.85.143',
    port: 4000,
    publicKey: 'f8452228fa67578d6957392858fbbe3545ab98dbbc277e9b8b9f7a0f5177ca36',
  },
]

const runProgram = async (): Promise<void> => {
  // Override default config params from config file, env vars, and cli args
  const file = join(process.cwd(), 'archiver-config.json')
  overrideDefaultConfig(file)
  // Set crypto hash keys from config
  const hashKey = config.ARCHIVER_HASH_KEY
  Crypto.setCryptoHashKey(hashKey)
  let logsConfig
  try {
    logsConfig = JSON.parse(readFileSync(resolve(__dirname, '../archiver-log.json'), 'utf8'))
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

  let latestCycle = await CycleDB.queryLatestCycleRecords(1)
  let latestCycleRecord = latestCycle[0]
  console.log('latestCycleRecord before', latestCycleRecord)
  const newCycleRecord = {
    ...latestCycleRecord,
    counter: latestCycleRecord.counter + 1,
    mode: 'shutdown' as P2P.ModesTypes.Record['mode'],
    removed: ['all'],
    archiversAtShutdown: archiversAtShutdown.map((archiver) => {
      return { ...archiver, curvePk: Crypto.getOrCreateCurvePk(archiver.publicKey) }
    }),
    lostArchivers: [],
    refutedArchivers: [],
    removedArchivers: [],
    standbyAdd: [],
    standbyRemove: [],
  }
  // console.log('newCycleRecord', newCycleRecord)
  await CycleDB.insertCycle({
    counter: newCycleRecord.counter,
    cycleMarker: newCycleRecord.marker,
    cycleRecord: newCycleRecord,
  })
  latestCycle = await CycleDB.queryLatestCycleRecords(1)
  latestCycleRecord = latestCycle[0]
  console.log('latestCycleRecord after', latestCycleRecord)
}
runProgram()
