import { readFileSync } from 'fs'
import { resolve } from 'path'
import { join } from 'path'
import { overrideDefaultConfig, config } from '../src/Config'
import * as Crypto from '../src/Crypto'
import * as dbstore from '../src/dbstore'
import * as AccountDB from '../src/dbstore/accounts'
import { startSaving } from '../src/saveConsoleOutput'
import * as Logger from '../src/Logger'
import { AccountType, fixAccountUint8Arrays, accountSpecificHash } from '../src/shardeum/calculateAccountHash'
import { addSigListeners } from '../src/State'
import { Utils as StringUtils } from '@shardus/types'

const updateHash = false
const runProgram = async (): Promise<void> => {
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
  addSigListeners()

  const totalAccounts = await AccountDB.queryAccountCount()
  console.log(totalAccounts)
  const limit = 10000
  let validHashAccounts = 0
  for (let i = 0; i < totalAccounts; i += limit) {
    console.log('From', i, 'To', i + limit)
    const accounts = await AccountDB.queryAccounts(i, limit)
    for (const account of accounts) {
      const accountHash1 = account.hash
      const accountHash2 = account.data.hash
      if (accountHash1 !== accountHash2) {
        console.log(account.accountId, 'accountHash', accountHash1, 'accountHash2', accountHash2)
      }
      if (account.data.accountType === AccountType.Account) {
        fixAccountUint8Arrays(account.data.account)
        // console.dir(acc, { depth: null })
      } else if (
        account.data.accountType === AccountType.ContractCode ||
        account.data.accountType === AccountType.ContractStorage
      ) {
        fixAccountUint8Arrays(account.data)
        // console.dir(acc, { depth: null })
      }
      const calculatedAccountHash = accountSpecificHash(account.data)

      if (accountHash1 !== calculatedAccountHash) {
        console.log(
          account.accountId,
          'accountHash1',
          accountHash1,
          'calculatedAccountHash',
          calculatedAccountHash
        )
        if (updateHash) {
          account.hash = calculatedAccountHash
          account.data.hash = calculatedAccountHash
          await AccountDB.insertAccount(account)
        }
      } else {
        // console.log(accountHash1, accountHash2, calculatedAccountHash)
        validHashAccounts++
      }
    }
    // if (i > 20000) break
  }
  console.log('totalAccounts', totalAccounts, 'validHashAccounts', validHashAccounts)
  await dbstore.closeDatabase()
}
runProgram()
