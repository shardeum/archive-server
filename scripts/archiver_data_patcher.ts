import { readFileSync } from 'fs'
import { resolve } from 'path'
import * as dbstore from '../src/dbstore'
import { config, overrideDefaultConfig } from '../src/Config'
import { ArchiverNodeInfo } from '../src/State'
import { postJson } from '../src/P2P'
import * as ReceiptDB from '../src/dbstore/receipts'
import * as OriginalTxDataDB from '../src/dbstore/originalTxsData'
import * as AccountDB from '../src/dbstore/accounts'
import * as TransactionDB from '../src/dbstore/transactions'
import * as CycleDB from '../src/dbstore/cycles'
import { storeReceiptData, storeOriginalTxData, storeCycleData } from '../src/Data/Collector'
import { DataType } from '../src/Data/GossipData'
import * as Crypto from '../src/Crypto'
import { join } from 'path'
import * as Logger from '../src/Logger'
import { startSaving } from '../src/saveConsoleOutput'
const {
  MAX_RECEIPTS_PER_REQUEST,
  MAX_BETWEEN_CYCLES_PER_REQUEST,
  MAX_ORIGINAL_TXS_PER_REQUEST,
  MAX_CYCLES_PER_REQUEST,
} = config.REQUEST_LIMIT

export type ArchiverNode = Omit<ArchiverNodeInfo, 'publicKey' | 'curvePk'>

const devAccount = {
  publicKey: config.ARCHIVER_PUBLIC_KEY,
  secretKey: config.ARCHIVER_SECRET_KEY,
}

// Set startCycle and endCycle to the cycle range you want to patch data for
const startCycle = 0
const endCycle = 0

// Set the active archivers from which the data can be pulled and patched
// Note: Specifying one archiver that has the complete data is enough, if multiple archivers are specified, it will pull data from all of them and patch.
const archivers: ArchiverNode[] = [
  {
    ip: '127.0.0.1',
    port: 4000,
  },
  {
    ip: '127.0.0.1',
    port: 4001,
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

  for (const archiver of archivers) {
    // Receipts
    console.log(`Patching receipts data between ${startCycle} and ${endCycle}!`)
    let nextEnd = startCycle + MAX_BETWEEN_CYCLES_PER_REQUEST
    for (let i = startCycle; i <= endCycle; ) {
      if (nextEnd > endCycle) nextEnd = endCycle
      console.log(i, nextEnd)
      const downloadedReceiptCountByCycles = await fetchDataCountByCycles(
        archiver,
        DataType.RECEIPT,
        i,
        nextEnd
      )
      // console.log(downloadedReceiptCountByCycles)
      if (!downloadedReceiptCountByCycles || !downloadedReceiptCountByCycles.receipts) {
        console.log(`archiver ${archiver.ip}:${archiver.port} failed to respond`)
        break
      }
      const receiptsCountByCycles = await ReceiptDB.queryReceiptCountByCycles(i, nextEnd)
      // console.log(receiptsCountByCycles)
      for (let j = i; j <= nextEnd; j++) {
        const downloadedReceipts = downloadedReceiptCountByCycles.receipts.filter((d) => d.cycle === j)
        const existingReceipts = receiptsCountByCycles.filter((d) => d.cycle === j)
        // console.log(j, downloadedReceipts, existingReceipts)
        if (JSON.stringify(downloadedReceipts) !== JSON.stringify(existingReceipts)) {
          console.log('Unmatched', j, downloadedReceipts, existingReceipts)
          const receipts = await fetchDataForCycle(archiver, DataType.RECEIPT, j)
          console.log('Downloaded receipts for cycle', j, ' -> ', receipts.length)
          await storeReceiptData(receipts, '', false)
        }
      }
      i = nextEnd + 1
      nextEnd += MAX_BETWEEN_CYCLES_PER_REQUEST
    }

    // OriginalTxsData
    console.log(`Patching originalTxsData data between ${startCycle} and ${endCycle}!`)
    nextEnd = startCycle + MAX_BETWEEN_CYCLES_PER_REQUEST
    for (let i = startCycle; i <= endCycle; ) {
      if (nextEnd > endCycle) nextEnd = endCycle
      console.log(i, nextEnd)
      const downloadedOriginalTxsDataCountByCycles = await fetchDataCountByCycles(
        archiver,
        DataType.ORIGINAL_TX_DATA,
        i,
        nextEnd
      )
      // console.log(downloadedOriginalTxsDataCountByCycles)
      if (!downloadedOriginalTxsDataCountByCycles || !downloadedOriginalTxsDataCountByCycles.originalTxs) {
        console.log(`archiver ${archiver.ip}:${archiver.port} failed to respond`)
        break
      }
      const originalTxsDataCountByCycles = await OriginalTxDataDB.queryOriginalTxDataCountByCycles(i, nextEnd)
      // console.log(originalTxsDataCountByCycles)
      for (let j = i; j <= nextEnd; j++) {
        const downloadedOriginalTxsData = downloadedOriginalTxsDataCountByCycles.originalTxs.filter(
          (d) => d.cycle === j
        )
        const existingOriginalTxsData = originalTxsDataCountByCycles.filter((d) => d.cycle === j)
        // console.log(j, downloadedOriginalTxsData, existingOriginalTxsData)
        if (JSON.stringify(downloadedOriginalTxsData) !== JSON.stringify(existingOriginalTxsData)) {
          console.log('Unmatched', j, downloadedOriginalTxsData, existingOriginalTxsData)
          const originalTxsData = await fetchDataForCycle(archiver, DataType.ORIGINAL_TX_DATA, j)
          if (originalTxsData) {
            console.log('Downloaded originalTxsData for cycle', j, ' -> ', originalTxsData.length)
            await storeOriginalTxData(originalTxsData, '', false)
          }
        }
      }
      i = nextEnd + 1
      nextEnd += MAX_BETWEEN_CYCLES_PER_REQUEST
    }

    // Cycle
    console.log(`Patching cycles data between ${startCycle} and ${endCycle}!`)
    nextEnd = startCycle + MAX_CYCLES_PER_REQUEST
    for (let i = startCycle; i <= endCycle; ) {
      if (nextEnd > endCycle) nextEnd = endCycle
      console.log(i, nextEnd)
      const downloadedCycleData = await fetchDataCountByCycles(archiver, DataType.CYCLE, i, nextEnd)
      // console.log(downloadedCycleData)
      if (!downloadedCycleData || !downloadedCycleData.cycleInfo) {
        console.log(`archiver ${archiver.ip}:${archiver.port} failed to respond`)
        break
      }
      await storeCycleData(downloadedCycleData.cycleInfo)
      i = nextEnd + 1
      nextEnd += MAX_CYCLES_PER_REQUEST
    }
  }

  const totalCycles = await CycleDB.queryCyleCount()
  const totalAccounts = await AccountDB.queryAccountCount()
  const totalTransactions = await TransactionDB.queryTransactionCount()
  const totalReceipts = await ReceiptDB.queryReceiptCount()
  const totalOriginalTxs = await OriginalTxDataDB.queryOriginalTxDataCount()
  console.log(
    `totalCycles: ${totalCycles}`,
    `totalAccounts: ${totalAccounts}`,
    `totalTransactions: ${totalTransactions}`,
    `totalReceipts: ${totalReceipts}`,
    `totalOriginalTxs: ${totalOriginalTxs}`
  )
}

const fetchDataCountByCycles = async (
  archiver: ArchiverNode,
  dataType: DataType,
  startCycle: number,
  endCycle: number
): Promise<any> => {
  const route =
    dataType === DataType.RECEIPT
      ? 'receipt'
      : dataType === DataType.ORIGINAL_TX_DATA
      ? 'originalTx'
      : 'cycleinfo'
  const data =
    dataType === DataType.CYCLE
      ? {
          start: startCycle,
          end: endCycle,
          sender: devAccount.publicKey,
        }
      : {
          startCycle,
          endCycle,
          type: 'tally',
          sender: devAccount.publicKey,
        }

  Crypto.core.signObj(data, devAccount.secretKey, devAccount.publicKey)

  return await postJson(
    `http://${archiver.ip}:${archiver.port}/${route}`,
    data,
    30 // 30seconds
  )
}

// TODO: Update as pagination query
const fetchDataForCycle = async (
  archiver: ArchiverNode,
  dataType: DataType,
  cycleNumber: number
): Promise<any> => {
  const route = dataType === DataType.RECEIPT ? 'receipt' : 'originalTx'
  let page = 1
  let combinedData: any = []
  let complete = false
  while (!complete) {
    const data = {
      startCycle: cycleNumber,
      endCycle: cycleNumber,
      page,
      sender: devAccount.publicKey,
    }
    Crypto.core.signObj(data, devAccount.secretKey, devAccount.publicKey)
    const res: any = await postJson(
      `http://${archiver.ip}:${archiver.port}/${route}`,
      data,
      30 // 30seconds
    )
    // console.log(res)
    if (dataType === DataType.RECEIPT && res && res.receipts) {
      const downloadedData = res.receipts
      // console.log('downloadedData', downloadedData.length)
      combinedData = [...combinedData, ...downloadedData]
      if (downloadedData.length < MAX_RECEIPTS_PER_REQUEST) {
        complete = true
      }
    } else if (dataType === DataType.ORIGINAL_TX_DATA && res && res.originalTxs) {
      const downloadedData = res.originalTxs
      // console.log('downloadedData', downloadedData.length)
      combinedData = [...combinedData, ...downloadedData]
      if (downloadedData.length < MAX_ORIGINAL_TXS_PER_REQUEST) {
        complete = true
      }
    }
    page++
  }
  // console.log('combinedData', combinedData.length)
  return combinedData
}

runProgram()
