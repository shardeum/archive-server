import * as Account from '../dbstore/accounts'
import * as Transaction from '../dbstore/transactions'
import * as Cycle from '../dbstore/cycles'
import * as Receipt from '../dbstore/receipts'
import * as Crypto from '../Crypto'
import {
  clearCombinedAccountsData,
  combineAccountsData,
  socketServer,
} from './Data'
import { config } from '../Config'
import * as Logger from '../Logger'
import { profilerInstance } from '../profiler/profiler'

export let storingAccountData = false
export let receiptsMap: Map<string, boolean> = new Map()
export let newestReceiptsMap: Map<string, boolean> = new Map()
export let lastReceiptMapResetTimestamp = 0
export let newestReceiptsMapIsReset = false

export const storeReceiptData = async (receipts = []) => {
  if (receipts && receipts.length <= 0) return
  if (socketServer) {
    let signedDataToSend = Crypto.sign({
      receipts: receipts,
    })
    socketServer.emit('RECEIPT', signedDataToSend)
  }
  let currentTime = Date.now()
  if (
    currentTime - lastReceiptMapResetTimestamp >= 60000 &&
    !newestReceiptsMapIsReset
  ) {
    newestReceiptsMap = new Map() // To save 30s data; So even when receiptMap is reset, this still has the record and will skip saving if it finds one
    newestReceiptsMapIsReset = true
    console.log('Newest Receipts Map Reset!', newestReceiptsMap)
  }
  for (let i = 0; i < receipts.length; i++) {
    const { accounts, cycle, result, sign, tx } = receipts[i]
    if (config.VERBOSE) console.log(tx.txId)
    if (receiptsMap.has(tx.txId) || newestReceiptsMap.has(tx.txId)) {
      // console.log('Skip', tx.txId)
      continue
    }
    await Receipt.insertReceipt({
      ...receipts[i],
      receiptId: tx.txId,
      timestamp: tx.timestamp,
    })
    receiptsMap.set(tx.txId, true)
    newestReceiptsMap.set(tx.txId, true)
    // console.log('Save', tx.txId)
    for (let j = 0; j < accounts.length; j++) {
      const account = accounts[j]
      const accObj: Account.AccountCopy = {
        accountId: account.accountId,
        data: account.data,
        timestamp: account.timestamp,
        hash: account.hash,
        cycleNumber: cycle,
      }
      const accountExist = await Account.queryAccountByAccountId(
        account.accountId
      )
      if (accountExist) {
        if (accObj.timestamp > accountExist.timestamp)
          await Account.updateAccount(accObj.accountId, accObj)
      } else {
        await Account.insertAccount(accObj)
      }
    }
    const txObj: Transaction.Transaction = {
      txId: tx.txId,
      timestamp: tx.timestamp,
      cycleNumber: cycle,
      data: tx.data,
      keys: tx.keys,
      result: result,
      sign: sign,
    }
    await Transaction.insertTransaction(txObj)
  }
  resetReceiptsMap()
}

export const storeCycleData = async (cycles = []) => {
  if (cycles && cycles.length <= 0) return
  for (let i = 0; i < cycles.length; i++) {
    const cycleRecord = cycles[i]
    const cycleObj: Cycle.Cycle = {
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
    const cycleExist = await Cycle.queryCycleByMarker(cycleObj.cycleMarker)
    if (cycleExist) {
      if (JSON.stringify(cycleObj) !== JSON.stringify(cycleExist))
        await Cycle.updateCycle(cycleObj.cycleMarker, cycleObj)
    } else {
      await Cycle.insertCycle(cycleObj)
    }
  }
}

export const storeAccountData = async (accounts = []) => {
  if (profilerInstance)
    profilerInstance.profileSectionStart('store_account_data')
  storingAccountData = true
  if (accounts && accounts.length <= 0) return
  if (socketServer) {
    let signedDataToSend = Crypto.sign({
      accounts: accounts,
    })
    socketServer.emit('RECEIPT', signedDataToSend)
  }
  console.log(accounts.length)
  Logger.mainLogger.debug('Received Accounts Size', accounts.length)
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
  await Account.bulkInsertAccounts(accounts)
  if (profilerInstance) profilerInstance.profileSectionEnd('store_account_data')
  console.log('Combined Accounts Data', combineAccountsData.length)
  Logger.mainLogger.debug('Combined Accounts Data', combineAccountsData.length)
  if (combineAccountsData.length > 0) {
    let accountData = [...combineAccountsData]
    clearCombinedAccountsData()
    storeAccountData(accountData)
  } else {
    storingAccountData = false
  }
}

export function resetReceiptsMap() {
  if (Date.now() - lastReceiptMapResetTimestamp >= 120000) {
    receiptsMap = new Map()
    lastReceiptMapResetTimestamp = Date.now()
    newestReceiptsMapIsReset = false
    console.log('Receipts Map Reset!', receiptsMap)
    Logger.mainLogger.debug('Receipts Map Reset!', receiptsMap)
  }
}
