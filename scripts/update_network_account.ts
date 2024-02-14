import { readFileSync } from 'fs'
import { resolve } from 'path'
import { join } from 'path'
import { overrideDefaultConfig, config } from '../src/Config'
import * as Crypto from '../src/Crypto'
import * as dbstore from '../src/dbstore'
import * as AccountDB from '../src/dbstore/accounts'
import { startSaving } from '../src/saveConsoleOutput'
import * as Logger from '../src/Logger'
import { accountSpecificHash } from '../src/shardeum/calculateAccountHash'

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

  const networkAccountId = config.globalNetworkAccount
  const networkAccount = (await AccountDB.queryAccountByAccountId(networkAccountId)) as AccountDB.AccountCopy
  console.log('Network account before', networkAccount)

  networkAccount.data.current = {
    ...networkAccount.data.current,
    activeVersion: '1.9.0',
    latestVersion: '1.9.0',
    minVersion: '1.9.0',
  }
  // If there is a config in the listOfChanges that will override the validator config at the network restart, we can add it here
  // networkAccount.data.listOfChanges.push({ change: { p2p: { minNodes: 150 } }, cycle: 55037 })

  const calculatedAccountHash = accountSpecificHash(networkAccount.data)

  networkAccount.hash = calculatedAccountHash
  networkAccount.data.hash = calculatedAccountHash
  await AccountDB.insertAccount(networkAccount)
  console.log('Network account after', networkAccount)
}
runProgram()
