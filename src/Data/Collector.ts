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

export const storeReceiptData = async (receipts = [], senderInfo = '', forceSaved = false) => {
  if (receipts && receipts.length <= 0) return
  let bucketSize = 1000
  let combineReceipts = []
  let combineAccounts = []
  let combineTransactions = []
  let txsData: TxsData[] = []
  if (!forceSaved) if (stopSavingTxData) return
  for (let i = 0; i < receipts.length; i++) {
    const { accounts, cycle, result, sign, tx, receipt } = receipts[i]
    if (config.VERBOSE) console.log(tx.txId, senderInfo)
    if (receiptsMap.has(tx.txId)) {
      console.log('RECEIPT', 'Skip', tx.txId, senderInfo)
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
    console.log('RECEIPT', 'Save', tx.txId, senderInfo)
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

export const storeOriginalTxData = async (
  originalTxsData: OriginalTxsData.OriginalTxData[] = [],
  senderInfo = '',
  forceSaved = false
) => {
  if (originalTxsData && originalTxsData.length <= 0) return
  const bucketSize = 1000
  let combineOriginalTxsData = []
  let txsData: TxsData[] = []
  if (!forceSaved) if (stopSavingTxData) return
  for (const originalTxData of originalTxsData) {
    const txId = originalTxData.txId
    if (originalTxsMap.has(txId)) {
      console.log('ORIGINAL_TX_DATA', 'Skip', txId, senderInfo)
      continue
    }
    originalTxsMap.set(txId, originalTxData.cycle)
    if (missingOriginalTxsMap.has(txId)) missingOriginalTxsMap.delete(txId)
    // console.log('originalTxData', originalTxData)
    combineOriginalTxsData.push(originalTxData)
    txsData.push({
      txId: txId,
      cycle: originalTxData.cycle,
    })
    console.log('ORIGINAL_TX_DATA', 'Save', txId, senderInfo)
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
    txDataType: 's',
    txsData: 'a',
    sender: 's',
    sign: 'o',
  })
  if (err) {
    Logger.mainLogger.error('Invalid gossip data', data)
    return { success: false, reason: 'Invalid gossip data' + err }
  }
  err = Utils.validateTypes(data.sign, { owner: 's', sig: 's' })
  if (err) {
    Logger.mainLogger.error('Invalid gossip data signature', err)
    return { success: false, reason: 'Invalid gossip data signature' + err }
  }
  if (data.sign.owner !== data.sender) {
    Logger.mainLogger.error('Data sender publicKey and sign owner key does not match')
    return { success: false, error: 'Data sender publicKey and sign owner key does not match' }
  }
  if (!adjacentArchivers.has(data.sender)) {
    Logger.mainLogger.error('Data sender is not the adjacent archiver')
    return { success: false, error: 'Data sender not the adjacent archiver' }
  }
  if (data.txDataType !== TxDataType.RECEIPT && data.txDataType !== TxDataType.ORIGINAL_TX_DATA) {
    Logger.mainLogger.error('Invalid dataType', data)
    return { success: false, error: 'Invalid dataType' }
  }
  if (!Crypto.verify(data)) {
    Logger.mainLogger.error('Invalid signature', data)
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
        console.log('GOSSIP', 'RECEIPT', 'SKIP', txId, sender)
        continue
      } else missingReceiptsMap.set(txId, cycle)
      console.log('GOSSIP', 'RECEIPT', 'MISS', txId, sender)
    }
  }
  if (txDataType === TxDataType.ORIGINAL_TX_DATA) {
    for (const txData of txsData) {
      const { txId, cycle } = txData
      if (originalTxsMap.has(txId) && originalTxsMap.get(txId) === cycle) {
        console.log('GOSSIP', 'ORIGINAL_TX_DATA', 'SKIP', txId, sender)
        continue
      } else missingOriginalTxsMap.set(txId, cycle)
      console.log('GOSSIP', 'ORIGINAL_TX_DATA', 'MISS', txId, sender)
    }
  }
}

export const collectMissingReceipts = async () => {
  const bucketSize = 100
  if (missingReceiptsMap.size === 0) return
  const cloneMissingReceiptsMap = new Map()
  for (const [txId, cycle] of missingReceiptsMap) {
    cloneMissingReceiptsMap.set(txId, cycle)
    missingReceiptsMap.delete(txId)
  }
  Logger.mainLogger.debug(
    'Collecting missing receipts',
    cloneMissingReceiptsMap.size,
    cloneMissingReceiptsMap
  )
  // Try to get missing receipts from 3 different archivers if one archiver fails to return some receipts
  let maxRetry = 3
  let retry = 0
  let archiversToUse: State.ArchiverNodeInfo[] = getArchiversToUse()
  while (cloneMissingReceiptsMap.size > 0 && retry < maxRetry) {
    let archiver = archiversToUse[retry]
    if (!archiver) archiver = archiversToUse[0]
    let missingReceipts = [...cloneMissingReceiptsMap.keys()]
    for (let start = 0; start < missingReceipts.length; ) {
      let txIdList = []
      const end = start + bucketSize
      if (start > missingReceipts.length) txIdList = missingReceipts.slice(start, end)
      else txIdList = missingReceipts.slice(start)
      const receipts: any = await queryTxDataFromArchivers(archiver, TxDataType.RECEIPT, txIdList)
      if (receipts && receipts.length > -1) {
        const receiptsToSave = []
        for (const receipt of receipts) {
          const { tx, cycle } = receipt
          if (tx && cloneMissingReceiptsMap.has(tx.txId) && cloneMissingReceiptsMap.get(tx.txId) === cycle) {
            cloneMissingReceiptsMap.delete(tx.txId)
            receiptsToSave.push(receipt)
          }
        }
        await storeReceiptData(receiptsToSave, archiver.ip + ':' + archiver.port, true)
      }
      start = end
    }
    retry++
  }
  if (cloneMissingReceiptsMap.size > 0) {
    Logger.mainLogger.debug(
      'Receipts TxId that are failed to get from other archivers',
      cloneMissingReceiptsMap
    )
  }
}

export const getArchiversToUse = () => {
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
    while (archiversToUse.length < 3) {
      let adjacentArchiversToUse = [...adjacentArchivers.values()]
      adjacentArchiversToUse = adjacentArchiversToUse.filter(
        (archiver) => !archiversToUse.find((archiverToUse) => archiverToUse.publicKey === archiver.publicKey)
      )
      if (adjacentArchiversToUse.length <= 0) break
      archiversToUse.push(Utils.getRandomItemFromArr(adjacentArchiversToUse)[0])
    }
  }
  return archiversToUse
}

export const queryTxDataFromArchivers = async (
  archiver: State.ArchiverNodeInfo,
  txDataType: TxDataType,
  txIdList: string[]
) => {
  let api_route = ''
  if (txDataType === TxDataType.RECEIPT) {
    // Query from other archivers using receipt endpoint
    // Using the existing GET /receipts endpoint for now; might have to change to POST /receipts endpoint
    api_route = `receipt?txIdList=${JSON.stringify(txIdList)}`
  } else if (txDataType === TxDataType.ORIGINAL_TX_DATA) {
    api_route = `originalTx?txIdList=${JSON.stringify(txIdList)}`
  }
  const response: any = await getJson(`http://${archiver.ip}:${archiver.port}/${api_route}`)
  if (response) {
    if (txDataType === TxDataType.RECEIPT) {
      const receipts = response.receipts || null
      if (receipts && receipts.length > -1) {
        return receipts
      }
    } else if (txDataType === TxDataType.ORIGINAL_TX_DATA) {
      const originalTxs = response.originalTxs || null
      if (originalTxs && originalTxs.length > -1) {
        return originalTxs
      }
    }
  }
  return null
}

export const collectMissingOriginalTxsData = async () => {
  const bucketSize = 100
  if (missingOriginalTxsMap.size === 0) return
  const cloneMissingOriginalTxsMap = new Map()
  for (const [txId, cycle] of missingOriginalTxsMap) {
    cloneMissingOriginalTxsMap.set(txId, cycle)
    missingOriginalTxsMap.delete(txId)
  }
  Logger.mainLogger.debug(
    'Collecting missing originalTxsData',
    cloneMissingOriginalTxsMap.size,
    cloneMissingOriginalTxsMap
  )
  // Try to get missing originalTxs from 3 different archivers if one archiver fails to return some receipts
  let maxRetry = 3
  let retry = 0
  let archiversToUse: State.ArchiverNodeInfo[] = getArchiversToUse()
  while (cloneMissingOriginalTxsMap.size > 0 && retry < maxRetry) {
    let archiver = archiversToUse[retry]
    if (!archiver) archiver = archiversToUse[0]
    let missingOriginalTxs = [...cloneMissingOriginalTxsMap.keys()]
    for (let start = 0; start < missingOriginalTxs.length; ) {
      let txIdList = []
      const end = start + bucketSize
      if (start > missingOriginalTxs.length) txIdList = missingOriginalTxs.slice(start, end)
      else txIdList = missingOriginalTxs.slice(start)
      const originalTxs: any = await queryTxDataFromArchivers(archiver, TxDataType.ORIGINAL_TX_DATA, txIdList)
      if (originalTxs && originalTxs.length > -1) {
        const originalTxsDataToSave = []
        for (const originalTx of originalTxs) {
          const { txId, cycle } = originalTx
          if (cloneMissingOriginalTxsMap.has(txId) && cloneMissingOriginalTxsMap.get(txId) === cycle) {
            cloneMissingOriginalTxsMap.delete(txId)
            originalTxsDataToSave.push(originalTx)
          }
        }
        await storeOriginalTxData(originalTxsDataToSave, archiver.ip + ':' + archiver.port, true)
      }
      start = end
    }
    retry++
  }
  if (cloneMissingOriginalTxsMap.size > 0) {
    Logger.mainLogger.debug(
      'OriginalTxsData TxId that are failed to get from other archivers',
      cloneMissingOriginalTxsMap
    )
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
