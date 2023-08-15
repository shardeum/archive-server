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

export let storingAccountData = false
export let receiptsMap: Map<string, number> = new Map()
export let lastReceiptMapResetTimestamp = 0
export let originalTxsMap: Map<string, number> = new Map()

export const storeReceiptData = async (receipts = [], senderInfo = '') => {
  if (receipts && receipts.length <= 0) return
  let bucketSize = 1000
  let combineReceipts = []
  let combineAccounts = []
  let combineTransactions = []
  // let receiptsToSend = []
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
    combineReceipts.push({
      ...receipts[i],
      receiptId: tx.txId,
      timestamp: tx.timestamp,
    })
    // receiptsToSend.push(receipts[i])
    receiptsMap.set(tx.txId, cycle)
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
      let cloneCombineReceipts = [...combineReceipts]
      // const cloneReceiptsToSend = [...receiptsToSend]
      combineReceipts = []
      // receiptsToSend = []
      if (socketServer) {
        let signedDataToSend = Crypto.sign({
          receipts: cloneCombineReceipts,
        })
        socketServer.emit('RECEIPT', signedDataToSend)
      }
      await Receipt.bulkInsertReceipts(cloneCombineReceipts)
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
    await Receipt.bulkInsertReceipts(combineReceipts)
    // if (receiptsToSend.length > 0) {
    if (socketServer) {
      let signedDataToSend = Crypto.sign({
        receipts: combineReceipts,
      })
      socketServer.emit('RECEIPT', signedDataToSend)
    }
    // }
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

export const storeOriginalTxData = async (originalTxsData = []) => {
  if (originalTxsData && originalTxsData.length <= 0) return
  const bucketSize = 1000
  let combineOriginalTxsData = []
  for (const originalTxData of originalTxsData) {
    const txId = originalTxData.txId
    if (originalTxsMap.has(txId)) continue
    originalTxsMap.set(txId, originalTxData.cycleNumber)
    // console.log('originalTxData', originalTxData)
    combineOriginalTxsData.push(originalTxData)
    if (combineOriginalTxsData.length >= bucketSize) {
      let cloneCombineOriginalTxsData = [...combineOriginalTxsData]
      combineOriginalTxsData = []
      if (socketServer) {
        let signedDataToSend = Crypto.sign({
          originalTxsData: cloneCombineOriginalTxsData,
        })
        socketServer.emit('RECEIPT', signedDataToSend)
      }
      await OriginalTxsData.bulkInsertOriginalTxsData(cloneCombineOriginalTxsData)
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

export const addCleanOldCacheMapInterval = () => {
  // Set to clean old receipts and originalTxs map every minute
  setInterval(() => {
    cleanOldReceiptsMap()
    cleanOldOriginalTxsMap()
  }, 60000)
}
