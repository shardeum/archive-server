import * as Account from '../dbstore/accounts'
import * as Transaction from '../dbstore/transactions'
import * as Receipt from '../dbstore/receipts'
import * as OriginalTxsData from '../dbstore/originalTxsData'
import * as Crypto from '../Crypto'
import { clearCombinedAccountsData, combineAccountsData, socketServer } from './Data'
import { config } from '../Config'
import * as Logger from '../Logger'
import { profilerInstance } from '../profiler/profiler'
import { Cycle, currentCycleCounter } from './Cycles'
import { bulkInsertCycles, Cycle as DbCycle, queryCycleByMarker, updateCycle } from '../dbstore/cycles'
import * as State from '../State'
import * as Utils from '../Utils'
import { TxDataType, GossipTxData, adjacentArchivers, sendDataToAdjacentArchivers } from './GossipTxData'
import { getJson } from '../P2P'

export let storingAccountData = false
export let receiptsMap: Map<string, number> = new Map()
export let lastReceiptMapResetTimestamp = 0
export let originalTxsMap: Map<string, number> = new Map()
export let missingReceiptsMap: Map<string, number> = new Map()
export let missingOriginalTxsMap: Map<string, number> = new Map()

export interface TxsData {
  txId: string
  cycle: number
}

// For debugging purpose, set this to true to stop saving tx data
const stopSavingTxData = false

export const storeReceiptData = async (receipts = [], senderInfo = '') => {
  if (receipts && receipts.length <= 0) return
  let bucketSize = 1000
  let combineReceipts = []
  let combineAccounts = []
  let combineTransactions = []
  let txsData: TxsData[] = []
  if (stopSavingTxData) return
  for (let i = 0; i < receipts.length; i++) {
    const { accounts, cycle, result, sign, tx, receipt } = receipts[i]
    if (config.VERBOSE) console.log(tx.txId, senderInfo)
    if (receiptsMap.has(tx.txId)) {
      // console.log('Skip', tx.txId, senderInfo)
      continue
    }
    // await Receipt.insertReceipt({
    //   ...receipts[i],
    //   receiptId: tx.txId,
    //   timestamp: tx.timestamp,
    // })
    receiptsMap.set(tx.txId, cycle)
    if (missingReceiptsMap.has(tx.txId)) missingReceiptsMap.delete(tx.txId)
    combineReceipts.push({
      ...receipts[i],
      receiptId: tx.txId,
      timestamp: tx.timestamp,
    })
    txsData.push({
      txId: tx.txId,
      cycle: cycle,
    })
    // console.log('Save', tx.txId, senderInfo)
    for (let j = 0; j < accounts.length; j++) {
      const account = accounts[j]
      const accObj: Account.AccountCopy = {
        accountId: account.accountId,
        data: account.data,
        timestamp: account.timestamp,
        hash: account.stateId,
        cycleNumber: cycle,
      }
      const accountExist = await Account.queryAccountByAccountId(account.accountId)
      if (accountExist) {
        if (accObj.timestamp > accountExist.timestamp) await Account.updateAccount(accObj.accountId, accObj)
      } else {
        // await Account.insertAccount(accObj)
        combineAccounts.push(accObj)
      }
    }
    // if (receipt) {
    //   const accObj: Account.AccountCopy = {
    //     accountId: receipt.accountId,
    //     data: receipt.data,
    //     timestamp: receipt.timestamp,
    //     hash: receipt.stateId,
    //     cycleNumber: cycle,
    //   }
    //   const accountExist = await Account.queryAccountByAccountId(
    //     receipt.accountId
    //   )
    //   if (accountExist) {
    //     if (accObj.timestamp > accountExist.timestamp)
    //       await Account.updateAccount(accObj.accountId, accObj)
    //   } else {
    //     // await Account.insertAccount(accObj)
    //     combineAccounts.push(accObj)
    //   }
    // }
    const txObj: Transaction.Transaction = {
      txId: tx.txId,
      accountId: receipt ? receipt.accountId : tx.txId, // Let set txId for now if app receipt is not forwarded
      timestamp: tx.timestamp,
      cycleNumber: cycle,
      data: receipt ? receipt.data : {},
      // keys: tx.keys,
      result: result,
      originTxData: tx.originalTxData,
      sign: sign,
    }
    // await Transaction.insertTransaction(txObj)
    combineTransactions.push(txObj)
    // Receipts size can be big, better to save per 100
    if (combineReceipts.length >= 100) {
      if (socketServer) {
        let signedDataToSend = Crypto.sign({
          receipts: combineReceipts,
        })
        socketServer.emit('RECEIPT', signedDataToSend)
      }
      await Receipt.bulkInsertReceipts(combineReceipts)
      await sendDataToAdjacentArchivers(TxDataType.RECEIPT, txsData)
      combineReceipts = []
      txsData = []
    }
    if (combineAccounts.length >= bucketSize) {
      await Account.bulkInsertAccounts(combineAccounts)
      combineAccounts = []
    }
    if (combineTransactions.length >= bucketSize) {
      await Transaction.bulkInsertTransactions(combineTransactions)
      combineTransactions = []
    }
  }
  // Receipts size can be big, better to save per 100
  if (combineReceipts.length > 0) {
    if (socketServer) {
      let signedDataToSend = Crypto.sign({
        receipts: combineReceipts,
      })
      socketServer.emit('RECEIPT', signedDataToSend)
    }
    await Receipt.bulkInsertReceipts(combineReceipts)
    await sendDataToAdjacentArchivers(TxDataType.RECEIPT, txsData)
  }
  if (combineAccounts.length > 0) await Account.bulkInsertAccounts(combineAccounts)
  if (combineTransactions.length > 0) await Transaction.bulkInsertTransactions(combineTransactions)
}

export const storeCycleData = async (cycles: Cycle[] = []) => {
  if (cycles && cycles.length <= 0) return
  const bucketSize = 1000
  let combineCycles = []
  for (let i = 0; i < cycles.length; i++) {
    const cycleRecord = cycles[i]

    const cycleObj: DbCycle = {
      counter: cycleRecord.counter,
      cycleMarker: cycleRecord.marker,
      cycleRecord,
    }
    if (socketServer) {
      let signedDataToSend = Crypto.sign({
        cycles: [cycleObj],
      })
      socketServer.emit('RECEIPT', signedDataToSend)
    }
    const cycleExist = await queryCycleByMarker(cycleObj.cycleMarker)
    if (cycleExist) {
      if (JSON.stringify(cycleObj) !== JSON.stringify(cycleExist))
        await updateCycle(cycleObj.cycleMarker, cycleObj)
    } else {
      // await Cycle.insertCycle(cycleObj)
      combineCycles.push(cycleObj)
    }
    if (combineCycles.length >= bucketSize || i === cycles.length - 1) {
      if (combineCycles.length > 0) await bulkInsertCycles(combineCycles)
      combineCycles = []
    }
  }
}

export const storeAccountData = async (restoreData: any = {}) => {
  const { accounts, receipts } = restoreData
  console.log(
    'RestoreData',
    'accounts',
    restoreData.accounts ? restoreData.accounts.length : 0,
    'receipts',
    restoreData.receipts ? restoreData.receipts.length : 0
  )
  if (profilerInstance) profilerInstance.profileSectionStart('store_account_data')
  storingAccountData = true
  if (!accounts && !receipts) return
  if (socketServer && accounts) {
    let signedDataToSend = Crypto.sign({
      accounts: accounts,
    })
    socketServer.emit('RECEIPT', signedDataToSend)
  }
  Logger.mainLogger.debug('Received Accounts Size', accounts ? accounts.length : 0)
  Logger.mainLogger.debug('Received Transactions Size', receipts ? receipts.length : 0)
  // for (let i = 0; i < accounts.length; i++) {
  //   const account = accounts[i]
  //   await Account.insertAccount(account)
  //   // const accountExist = await Account.queryAccountByAccountId(
  //   //   account.accountId
  //   // )
  //   // if (accountExist) {
  //   //   if (account.timestamp > accountExist.timestamp)
  //   //     await Account.updateAccount(account.accountId, account)
  //   // } else {
  //   //   await Account.insertAccount(account)
  //   // }
  // }
  if (accounts && accounts.length > 0) await Account.bulkInsertAccounts(accounts)
  if (receipts && receipts.length > 0) {
    Logger.mainLogger.debug('Received receipts Size', receipts.length)
    let combineTransactions = []
    for (let i = 0; i < receipts.length; i++) {
      const receipt = receipts[i]
      const txObj = {
        txId: receipt.data.txId || receipt.txId,
        accountId: receipt.accountId,
        timestamp: receipt.timestamp,
        cycleNumber: receipt.cycleNumber,
        data: receipt.data,
        // keys: {},
        result: {},
        originTxData: {},
        sign: {},
      } as Transaction.Transaction
      combineTransactions.push(txObj)
    }
    await Transaction.bulkInsertTransactions(combineTransactions)
  }
  if (profilerInstance) profilerInstance.profileSectionEnd('store_account_data')
  console.log('Combined Accounts Data', combineAccountsData.accounts.length)
  Logger.mainLogger.debug('Combined Accounts Data', combineAccountsData.accounts.length)
  if (combineAccountsData.accounts.length > 0 || combineAccountsData.receipts.length > 0) {
    console.log('Found combine accountsData')
    let accountData = { ...combineAccountsData }
    clearCombinedAccountsData()
    storeAccountData(accountData)
  } else {
    storingAccountData = false
  }
}

export const storeOriginalTxData = async (originalTxsData: OriginalTxsData.OriginalTxData[] = []) => {
  if (originalTxsData && originalTxsData.length <= 0) return
  const bucketSize = 1000
  let combineOriginalTxsData = []
  let txsData: TxsData[] = []
  if (stopSavingTxData) return
  for (const originalTxData of originalTxsData) {
    const txId = originalTxData.txId
    if (originalTxsMap.has(txId)) continue
    originalTxsMap.set(txId, originalTxData.cycle)
    if (missingOriginalTxsMap.has(txId)) missingOriginalTxsMap.delete(txId)
    // console.log('originalTxData', originalTxData)
    combineOriginalTxsData.push(originalTxData)
    txsData.push({
      txId: txId,
      cycle: originalTxData.cycle,
    })
    if (combineOriginalTxsData.length >= bucketSize) {
      if (socketServer) {
        let signedDataToSend = Crypto.sign({
          originalTxsData: combineOriginalTxsData,
        })
        socketServer.emit('RECEIPT', signedDataToSend)
      }
      await OriginalTxsData.bulkInsertOriginalTxsData(combineOriginalTxsData)
      await sendDataToAdjacentArchivers(TxDataType.ORIGINAL_TX_DATA, txsData)
      combineOriginalTxsData = []
      txsData = []
    }
  }
  if (combineOriginalTxsData.length > 0) {
    if (socketServer) {
      let signedDataToSend = Crypto.sign({
        originalTxsData: combineOriginalTxsData,
      })
      socketServer.emit('RECEIPT', signedDataToSend)
    }
    await OriginalTxsData.bulkInsertOriginalTxsData(combineOriginalTxsData)
    await sendDataToAdjacentArchivers(TxDataType.ORIGINAL_TX_DATA, txsData)
  }
}

export const validateGossipTxData = (data: GossipTxData) => {
  let err = Utils.validateTypes(data, {
    dataType: 's',
    txsData: 'a',
    sender: 's',
    sign: 'o',
  })
  if (err) {
    Logger.errorLogger.error('Invalid gossip data', data)
    return { success: false, reason: 'Invalid gossip data' + err }
  }
  err = Utils.validateTypes(data.sign, { owner: 's', sig: 's' })
  if (err) {
    Logger.errorLogger.error('Invalid gossip data signature', err)
    return { success: false, reason: 'Invalid gossip data signature' + err }
  }
  if (data.sign.owner !== data.sender) {
    Logger.errorLogger.error('Data sender publicKey and sign owner key does not match')
    return { success: false, error: 'Data sender publicKey and sign owner key does not match' }
  }
  if (adjacentArchivers.has(data.sender)) {
    Logger.errorLogger.error('Data sender is not the adjacent archiver')
    return { success: false, error: 'Data sender not the adjacent archiver' }
  }
  if (data.txDataType !== TxDataType.RECEIPT && data.txDataType !== TxDataType.ORIGINAL_TX_DATA) {
    Logger.errorLogger.error('Invalid dataType', data)
    return { success: false, error: 'Invalid dataType' }
  }
  if (!Crypto.verify(data)) {
    Logger.errorLogger.error('Invalid signature', data)
    return { success: false, error: 'Invalid signature' }
  }
  return { success: true }
}

export const processGossipTxData = (data: GossipTxData) => {
  const { txDataType, txsData, sender } = data
  if (txDataType === TxDataType.RECEIPT) {
    for (const txData of txsData) {
      const { txId, cycle } = txData
      if (receiptsMap.has(txId) && receiptsMap.get(txId) === cycle) {
        console.log('Skip Receipt', txId, sender)
        continue
      } else missingReceiptsMap.set(txId, cycle)
      console.log('Save Receipt', txId, sender)
    }
  }
  if (txDataType === TxDataType.ORIGINAL_TX_DATA) {
    for (const txData of txsData) {
      const { txId, cycle } = txData
      if (originalTxsMap.has(txId) && originalTxsMap.get(txId) === cycle) {
        console.log('Skip OriginalTx', txId, sender)
        continue
      } else missingOriginalTxsMap.set(txId, cycle)
      console.log('Save OriginalTx', txId, sender)
    }
  }
}

export const collectMissingReceipts = async () => {
  const bucketSize = 100
  if (missingReceiptsMap.size > 0) {
    const cloneMissingReceiptsMap = new Map()
    for (const [txId, cycle] of missingReceiptsMap) {
      cloneMissingReceiptsMap.set(txId, cycle)
      missingReceiptsMap.delete(txId)
    }
    let missingReceipts = [...cloneMissingReceiptsMap.keys()]
    for (let start = 0; start < missingReceipts.length; ) {
      let txIdList = []
      const end = start + bucketSize
      if (start > missingReceipts.length) txIdList = missingReceipts.slice(start, end)
      else txIdList = missingReceipts.slice(start)
      const receipts: any = await queryTxDataFromArchivers(TxDataType.RECEIPT, txIdList)
      if (receipts && receipts.length > -1) {
        const receiptsToSave = []
        for (const receipt of receipts) {
          const { tx, cycle } = receipt
          if (tx && cloneMissingReceiptsMap.has(tx.txId) && cloneMissingReceiptsMap.get(tx.txId) === cycle) {
            cloneMissingReceiptsMap.delete(tx.txId)
            receiptsToSave.push(receipt)
          }
        }
        await storeReceiptData(receiptsToSave)
      }
      start = end
    }

    if (cloneMissingReceiptsMap.size > 0) {
      // There are still some missing receipts; Retry to get them from other archivers
      missingReceipts = [...cloneMissingReceiptsMap.keys()]
      for (let start = 0; start < missingReceipts.length; ) {
        let txIdList = []
        const end = start + bucketSize
        if (start > missingReceipts.length) txIdList = missingReceipts.slice(start, end)
        else txIdList = missingReceipts.slice(start)
        const receipts: any = await queryTxDataFromArchivers(TxDataType.RECEIPT, txIdList)
        if (receipts && receipts.length > -1) {
          const receiptsToSave = []
          for (const receipt of receipts) {
            const { tx, cycle } = receipt
            if (
              tx &&
              cloneMissingReceiptsMap.has(tx.txId) &&
              cloneMissingReceiptsMap.get(tx.txId) === cycle
            ) {
              cloneMissingReceiptsMap.delete(tx.txId)
              receiptsToSave.push(receipt)
            }
          }
          await storeReceiptData(receiptsToSave)
        }
        start = end
      }
      if (cloneMissingReceiptsMap.size > 0) {
        Logger.mainLogger.debug(
          'Receipts TxId that are failed to get from other archivers',
          cloneMissingReceiptsMap
        )
      }
    }
  }
}

export const queryTxDataFromArchivers = async (txDataType: TxDataType, txIdList: string[]) => {
  let archiversToUse: State.ArchiverNodeInfo[] = []
  // Choosing 3 random archivers from the active archivers list
  if (State.activeArchivers.length <= 3) {
    State.activeArchivers.forEach(
      (archiver) => archiver.publicKey !== State.getNodeInfo().publicKey && archiversToUse.push(archiver)
    )
  } else {
    const activeArchivers = [...State.activeArchivers].filter(
      (archiver) =>
        adjacentArchivers.has(archiver.publicKey) || archiver.publicKey === State.getNodeInfo().publicKey
    )
    archiversToUse = Utils.getRandomItemFromArr(activeArchivers, 0, 3)
    if (archiversToUse.length < 3) {
      const adjacentArchiversToUse = [...adjacentArchivers.values()]
      archiversToUse.push(Utils.getRandomItemFromArr(adjacentArchiversToUse)[0])
    }
  }
  let api_route = ''
  if (txDataType === TxDataType.RECEIPT) {
    // Query from other archivers using receipt endpoint
    // Using the existing GET /receipts endpoint for now; might have to change to POST /receipts endpoint
    api_route = `receipts?txIdList=${JSON.stringify(txIdList)}`
  } else if (txDataType === TxDataType.ORIGINAL_TX_DATA) {
    api_route = `originTx?txIdList=${JSON.stringify(txIdList)}`
  }
  let maxRetryForEachBucket = 3
  let retry = 0
  // This means if fails to get from one archiver, it will try to get from another archiver; max 3 times for each bucket
  while (retry < maxRetryForEachBucket) {
    const randomArchiver = archiversToUse[retry]
    const response: any = await getJson(`http://${randomArchiver.ip}:${randomArchiver.port}/${api_route}`)
    if (response) {
      if (txDataType === TxDataType.RECEIPT) {
        const receipts = response.receipts || null
        if (receipts && receipts.length > -1) {
          return receipts
        } else retry++
      } else if (txDataType === TxDataType.ORIGINAL_TX_DATA) {
        const originalTxs = response.originalTxs || null
        if (originalTxs && originalTxs.length > -1) {
          return originalTxs
        } else retry++
      }
    } else retry++
  }
  return null
}

export const collectMissingOriginalTxsData = async () => {
  const bucketSize = 100
  if (missingOriginalTxsMap.size > 0) {
    const cloneMissingOriginalTxsMap = new Map()
    for (const [txId, cycle] of missingOriginalTxsMap) {
      cloneMissingOriginalTxsMap.set(txId, cycle)
      missingOriginalTxsMap.delete(txId)
    }
    let missingOriginalTxs = [...cloneMissingOriginalTxsMap.keys()]
    for (let start = 0; start < missingOriginalTxs.length; ) {
      let txIdList = []
      const end = start + bucketSize
      if (start > missingOriginalTxs.length) txIdList = missingOriginalTxs.slice(start, end)
      else txIdList = missingOriginalTxs.slice(start)
      const receipts: any = await queryTxDataFromArchivers(TxDataType.RECEIPT, txIdList)
      if (receipts && receipts.length > -1) {
        const receiptsToSave = []
        for (const receipt of receipts) {
          const { tx, cycle } = receipt
          if (
            tx &&
            cloneMissingOriginalTxsMap.has(tx.txId) &&
            cloneMissingOriginalTxsMap.get(tx.txId) === cycle
          ) {
            cloneMissingOriginalTxsMap.delete(tx.txId)
            receiptsToSave.push(receipt)
          }
        }
        await storeReceiptData(receiptsToSave)
      }
      start = end
    }

    if (cloneMissingOriginalTxsMap.size > 0) {
      // There are still some missing receipts; Retry to get them from other archivers
      missingOriginalTxs = [...cloneMissingOriginalTxsMap.keys()]
      for (let start = 0; start < missingOriginalTxs.length; ) {
        let txIdList = []
        const end = start + bucketSize
        if (start > missingOriginalTxs.length) txIdList = missingOriginalTxs.slice(start, end)
        else txIdList = missingOriginalTxs.slice(start)
        const originalTxs: any = await queryTxDataFromArchivers(TxDataType.RECEIPT, txIdList)
        if (originalTxs && originalTxs.length > -1) {
          const originalTxsDataToSave = []
          for (const originalTx of originalTxs) {
            const { tx, cycle } = originalTx
            if (
              tx &&
              cloneMissingOriginalTxsMap.has(tx.txId) &&
              cloneMissingOriginalTxsMap.get(tx.txId) === cycle
            ) {
              cloneMissingOriginalTxsMap.delete(tx.txId)
              originalTxsDataToSave.push(originalTx)
            }
          }
          await storeOriginalTxData(originalTxsDataToSave)
        }
        start = end
      }
      if (cloneMissingOriginalTxsMap.size > 0) {
        Logger.mainLogger.debug(
          'OriginalTxsData TxId that are failed to get from other archivers',
          cloneMissingOriginalTxsMap
        )
      }
    }
  }
}

export function cleanOldReceiptsMap() {
  for (let [key, value] of receiptsMap) {
    // Clean receipts that are older than current cycle
    if (value < currentCycleCounter) {
      receiptsMap.delete(key)
    }
  }
  if (config.VERBOSE) console.log('Clean old receipts map!', currentCycleCounter)
}

export function cleanOldOriginalTxsMap() {
  for (let [key, value] of originalTxsMap) {
    // Clean originalTxs that are older than current cycle
    if (value < currentCycleCounter) {
      originalTxsMap.delete(key)
    }
  }
  if (config.VERBOSE) console.log('Clean old originalTxs map!', currentCycleCounter)
}

export const scheduleCacheCleanup = () => {
  // Set to clean old receipts and originalTxs map every minute
  setInterval(() => {
    cleanOldReceiptsMap()
    cleanOldOriginalTxsMap()
  }, 60000)
}

export const scheduleMissingTxsDataQuery = () => {
  // Set to collect missing txs data in every 5 seconds
  setInterval(() => {
    collectMissingReceipts()
    collectMissingOriginalTxsData()
  }, 5000)
}
