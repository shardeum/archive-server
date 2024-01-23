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

// Override default config params from config file, env vars, and cli args
const file = join(process.cwd(), 'archiver-config.json')
let logDir: string
const runProgram = async (): Promise<void> => {
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
  logDir = `${config.ARCHIVER_LOGS}/${config.ARCHIVER_IP}_${config.ARCHIVER_PORT}`
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
    archiversAtShutdown: [
      {
        curvePk: '363afebb8cca474bd4e3c29d0109ad068736b7802c34ed8b7038cd6a95bb1e24',
        ip: '127.0.0.1',
        port: 4000,
        publicKey: '758b1c119412298802cd28dbfa394cdfeecc4074492d60844cc192d632d84de3',
      },
      {
        curvePk: '478d2d552c3007c35367e27f1bd385d293d6b9c20019d414870bf523dfa1297f',
        ip: '127.0.0.1',
        port: 4001,
        publicKey: 'e8a5c26b9e2c3c31eb7c7d73eaed9484374c16d983ce95f3ab18a62521964a94',
      },
      {
        curvePk: '903115c5665346df730e85a828be46c5ae8af68ddc5bc6430b63d599f4dce151',
        ip: '127.0.0.1',
        port: 4002,
        publicKey: '9426b64e675cad739d69526bf7e27f3f304a8a03dca508a9180f01e9269ce447',
      },
    ],
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
