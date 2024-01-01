import { P2P as P2PTypes } from '@shardus/types'
import * as Account from '../dbstore/accounts'
import * as Transaction from '../dbstore/transactions'
import * as Receipt from '../dbstore/receipts'
import * as OriginalTxsData from '../dbstore/originalTxsData'
import * as Crypto from '../Crypto'
import { clearCombinedAccountsData, combineAccountsData, socketServer, collectCycleData } from './Data'
import { config } from '../Config'
import * as Logger from '../Logger'
import { profilerInstance } from '../profiler/profiler'
import { getCurrentCycleCounter, shardValuesByCycle } from './Cycles'
import { bulkInsertCycles, Cycle as DbCycle, queryCycleByMarker, updateCycle } from '../dbstore/cycles'
import * as State from '../State'
import * as Utils from '../Utils'
import { DataType, GossipData, adjacentArchivers, sendDataToAdjacentArchivers } from './GossipData'
import { getJson, postJson } from '../P2P'
import { globalAccountsMap, setGlobalNetworkAccount } from '../GlobalAccount'
import { CycleLogWriter, ReceiptLogWriter, OriginalTxDataLogWriter } from '../Data/DataLogWriter'
import * as OriginalTxDB from '../dbstore/originalTxsData'
import ShardFunction from '../ShardFunctions'
import { ConsensusNodeInfo } from '../NodeList'
import { verifyAccountHash } from '../shardeum/calculateAccountHash'

export let storingAccountData = false
export const receiptsMap: Map<string, number> = new Map()
export const lastReceiptMapResetTimestamp = 0
export const originalTxsMap: Map<string, number> = new Map()
export const missingReceiptsMap: Map<string, number> = new Map()
export const missingOriginalTxsMap: Map<string, number> = new Map()

export interface TxsData {
  txId: string
  cycle: number
}

/**
 * Calls the /get-tx-receipt endpoint of the nodes in the execution group of the receipt to verify the receipt. If "RECEIPT_CONFIRMATIONS" number of nodes return the same receipt, the receipt is deemed valid.
 * @param receipt
 * @param executionGroupNodes
 * @returns boolean
 */
const isReceiptRobust = async (
  receipt: Receipt.ArchiverReceipt,
  executionGroupNodes: ConsensusNodeInfo[],
  minConfirmations: number = config.RECEIPT_CONFIRMATIONS
): Promise<{ success: boolean; newReceipt?: Receipt.ArchiverReceipt }> => {
  const result = { success: false }
  // Created signedData with full_receipt = false outside of queryReceipt to avoid signing the same data multiple times
  let signedData = Crypto.sign({ txId: receipt.tx.txId, full_receipt: false })
  const queryReceipt = async (node: ConsensusNodeInfo) => {
    try {
      return postJson(`http://${node.ip}:${node.port}/get-tx-receipt`, signedData)
    } catch (error) {
      Logger.mainLogger.error('Error in /get-tx-receipt:', error)
      return null
    }
  }
  const robustQuery = await Utils.robustQuery(
    executionGroupNodes,
    (execNode) => queryReceipt(execNode),
    undefined,
    5,
    false // set shuffleNodes to false
  )
  if (!robustQuery) {
    Logger.mainLogger.error(
      `❌ 'null' response from all nodes in receipt-validation for txId: ${receipt.tx.txId}`
    )
    return result
  }

  if (robustQuery.count < minConfirmations) {
    // update signedData with full_receipt = true
    signedData = Crypto.sign({ txId: receipt.tx.txId, full_receipt: true })
    for (const node of robustQuery.nodes) {
      const fullReceipt: any = await queryReceipt(node)
      if (!fullReceipt) continue
      const isReceiptEqual = Utils.isDeepStrictEqual(
        fullReceipt.receipt.appliedReceipt,
        (robustQuery.value as any).receipt
      )
      if (isReceiptEqual && validateReceiptData(fullReceipt.receipt) && verifyAccountHash(fullReceipt)) {
        return { success: true, newReceipt: fullReceipt.receipt }
      }
    }
    return { success: false }
  }
  return { success: true }
}

// For debugging purpose, set this to true to stop saving tx data
const stopSavingTxData = false

export const validateReceiptData = (receipt: Receipt.ArchiverReceipt) => {
  // Add type and value existence check
  let err = Utils.validateTypes(receipt, {
    tx: 'o',
    cycle: 'n',
    beforeStateAccounts: 'a',
    accounts: 'a',
    appReceiptData: 'o?',
    appliedReceipt: 'o',
    executionShardKey: 's',
  })
  if (err) {
    Logger.mainLogger.error('Invalid receipt data', err)
    return false
  }
  err = Utils.validateTypes(receipt.tx, {
    originalTxData: 'o',
    txId: 's',
    timestamp: 'n',
  })
  if (err) {
    Logger.mainLogger.error('Invalid receipt tx data', err)
    return false
  }
  for (const account of receipt.beforeStateAccounts) {
    err = Utils.validateTypes(account, {
      accountId: 's',
      data: 'o',
      timestamp: 'n',
      hash: 's',
      // cycleNumber: 'n', it is not present in the beforeStateAccounts data
      isGlobal: 'b',
    })
    if (err) {
      Logger.mainLogger.error('Invalid receipt beforeStateAccounts data', err)
      return false
    }
  }
  for (const account of receipt.accounts) {
    err = Utils.validateTypes(account, {
      accountId: 's',
      data: 'o',
      timestamp: 'n',
      hash: 's',
      // cycleNumber: 'n', it is not present in the beforeStateAccounts data
      isGlobal: 'b',
    })
    if (err) {
      Logger.mainLogger.error('Invalid receipt accounts data', err)
      return false
    }
  }
  err = Utils.validateTypes(receipt.appliedReceipt, {
    txid: 's',
    result: 'b',
    appliedVote: 'o',
    confirmOrChallenge: 'o',
    signatures: 'a',
    app_data_hash: 's',
  })
  if (err) {
    Logger.mainLogger.error('Invalid receipt appliedReceipt data', err)
    return false
  }
  err = Utils.validateTypes(receipt.appliedReceipt.appliedVote, {
    txid: 's',
    transaction_result: 'b',
    account_id: 'a',
    account_state_hash_after: 'a',
    account_state_hash_before: 'a',
    cant_apply: 'b',
    node_id: 's',
    sign: 'o',
    app_data_hash: 's',
  })
  if (err) {
    Logger.mainLogger.error('Invalid receipt appliedReceipt appliedVote data', err)
    return false
  }
  err = Utils.validateTypes(receipt.appliedReceipt.appliedVote.sign, {
    owner: 's',
    sig: 's',
  })
  if (err) {
    Logger.mainLogger.error('Invalid receipt appliedReceipt appliedVote signature data', err)
    return false
  }
  err = Utils.validateTypes(receipt.appliedReceipt.confirmOrChallenge, {
    message: 's',
    nodeId: 's',
    appliedVote: 'o',
    sign: 'o',
  })
  if (err) {
    Logger.mainLogger.error('Invalid receipt appliedReceipt confirmOrChallenge data', err)
    return false
  }
  err = Utils.validateTypes(receipt.appliedReceipt.confirmOrChallenge.sign, {
    owner: 's',
    sig: 's',
  })
  if (err) {
    Logger.mainLogger.error('Invalid receipt appliedReceipt confirmOrChallenge signature data', err)
    return false
  }
  err = Utils.validateTypes(receipt.appliedReceipt.signatures[0], {
    owner: 's',
    sig: 's',
  })
  if (err) {
    Logger.mainLogger.error('Invalid receipt appliedReceipt signatures data', err)
    return false
  }
  return true
}

export const verifyReceiptData = async (
  receipt: Receipt.ArchiverReceipt
): Promise<{ success: boolean; newReceipt?: Receipt.ArchiverReceipt }> => {
  const result = { success: false }
  // Check the signed nodes are part of the execution group nodes of the tx
  const { executionShardKey, cycle, appliedReceipt } = receipt
  const { appliedVote, confirmOrChallenge } = appliedReceipt
  const cycleShardData = shardValuesByCycle.get(cycle)
  if (!cycleShardData) {
    Logger.mainLogger.error('Cycle shard data not found')
    return result
  }
  // Determine the home partition index of the primary account (executionShardKey)
  const { homePartition } = ShardFunction.addressToPartition(cycleShardData.shardGlobals, executionShardKey)
  // Check if the appliedVote node is in the execution group
  if (!cycleShardData.nodeShardDataMap.has(appliedVote.node_id)) {
    Logger.mainLogger.error('Invalid receipt appliedReceipt appliedVote node is not in the active nodesList')
    return result
  }
  if (appliedVote.sign.owner !== cycleShardData.nodeShardDataMap.get(appliedVote.node_id).node.publicKey) {
    Logger.mainLogger.error(
      'Invalid receipt appliedReceipt appliedVote node signature owner and node public key does not match'
    )
    return result
  }
  if (!cycleShardData.parititionShardDataMap.get(homePartition).coveredBy[appliedVote.node_id]) {
    Logger.mainLogger.error(
      'Invalid receipt appliedReceipt appliedVote node is not in the execution group of the tx'
    )
    return result
  }
  if (!Crypto.verify(appliedVote)) {
    Logger.mainLogger.error('Invalid receipt appliedReceipt appliedVote signature verification failed')
    return result
  }

  // Check if the confirmOrChallenge node is in the execution group
  if (!cycleShardData.nodeShardDataMap.has(confirmOrChallenge.nodeId)) {
    Logger.mainLogger.error(
      'Invalid receipt appliedReceipt confirmOrChallenge node is not in the active nodesList'
    )
    return result
  }
  if (
    confirmOrChallenge.sign.owner !==
    cycleShardData.nodeShardDataMap.get(confirmOrChallenge.nodeId).node.publicKey
  ) {
    Logger.mainLogger.error(
      'Invalid receipt appliedReceipt confirmOrChallenge node signature owner and node public key does not match'
    )
    return result
  }
  if (!cycleShardData.parititionShardDataMap.get(homePartition).coveredBy[confirmOrChallenge.nodeId]) {
    Logger.mainLogger.error(
      'Invalid receipt appliedReceipt confirmOrChallenge node is not in the execution group of the tx'
    )
    return result
  }
  if (Crypto.verify(confirmOrChallenge)) {
    Logger.mainLogger.error('Invalid receipt appliedReceipt confirmOrChallenge signature verification failed')
    return result
  }

  // List the execution group nodes of the tx, Use them to robustQuery to verify the receipt
  const executionGroupNodes = Object.values(
    cycleShardData.parititionShardDataMap.get(homePartition).coveredBy
  ) as unknown as ConsensusNodeInfo[]
  Logger.mainLogger.debug('executionGroupNodes', receipt.tx.txId, executionGroupNodes)
  // List only random 3 x Receipt Confirmations number of nodes from the execution group
  const filteredExecutionGroupNodes = Utils.getRandomItemFromArr(
    executionGroupNodes,
    0,
    3 * config.RECEIPT_CONFIRMATIONS
  )
  const minConfirmations =
    filteredExecutionGroupNodes.length > config.RECEIPT_CONFIRMATIONS
      ? config.RECEIPT_CONFIRMATIONS
      : filteredExecutionGroupNodes.length
  const { success, newReceipt } = await isReceiptRobust(
    receipt,
    filteredExecutionGroupNodes,
    minConfirmations
  )
  if (!success) {
    Logger.mainLogger.error('Invalid receipt: Robust check failed')
    return result
  }
  if (newReceipt) return { success: true, newReceipt }
  return { success: true }
}

export const storeReceiptData = async (
  receipts: Receipt.ArchiverReceipt[],
  senderInfo = '',
  forceSaved = false
): Promise<void> => {
  if (!receipts || !Array.isArray(receipts) || receipts.length <= 0) return
  const bucketSize = 1000
  let combineReceipts = []
  let combineAccounts = []
  let combineTransactions = []
  let txsData: TxsData[] = []
  if (!forceSaved) if (stopSavingTxData) return
  for (let receipt of receipts) {
    const txId = receipt?.tx?.txId
    if (receiptsMap.has(txId) || receiptsInValidationMap.has(txId)) {
      // console.log('RECEIPT', 'Skip', tx.txId, senderInfo)
      continue
    }
    receiptsInValidationMap.set(txId, receipt.cycle)
    if (!validateReceiptData(receipt)) {
      Logger.mainLogger.error('Invalid receipt: Validation failed', txId)
      receiptsInValidationMap.delete(txId)
      continue
    }

    if (State.isActive) {
      if (!verifyAccountHash(receipt)) {
        Logger.mainLogger.error('Invalid receipt: Account Verification failed', txId)
        receiptsInValidationMap.delete(txId)
        continue
      }
      const { success, newReceipt } = await verifyReceiptData(receipt)
      if (!success) {
        Logger.mainLogger.error('Invalid receipt: Verification failed', txId)
        receiptsInValidationMap.delete(txId)
        continue
      }
      if (newReceipt) receipt = newReceipt
    }
    // await Receipt.insertReceipt({
    //   ...receipts[i],
    //   receiptId: tx.txId,
    //   timestamp: tx.timestamp,
    // })
    const { accounts, cycle, tx, appReceiptData } = receipt
    if (config.VERBOSE) console.log(tx.txId, senderInfo)
    receiptsMap.set(tx.txId, cycle)
    receiptsInValidationMap.delete(tx.txId)
    if (missingReceiptsMap.has(tx.txId)) missingReceiptsMap.delete(tx.txId)
    combineReceipts.push({
      ...receipt,
      receiptId: tx.txId,
      timestamp: tx.timestamp,
    })
    if (config.dataLogWrite && ReceiptLogWriter)
      ReceiptLogWriter.writeToLog(
        `${JSON.stringify({
          ...receipt,
          receiptId: tx.txId,
          timestamp: tx.timestamp,
        })}\n`
      )
    txsData.push({
      txId: tx.txId,
      cycle: cycle,
    })
    // console.log('RECEIPT', 'Save', tx.txId, senderInfo)
    for (let j = 0; j < accounts.length; j++) {
      // eslint-disable-next-line security/detect-object-injection
      const account = accounts[j]
      const accObj: Account.AccountCopy = {
        accountId: account.accountId,
        data: account.data,
        timestamp: account.timestamp,
        hash: account.hash,
        cycleNumber: cycle,
        isGlobal: account.isGlobal || false,
      }
      const accountExist = await Account.queryAccountByAccountId(account.accountId)
      if (accountExist) {
        if (accObj.timestamp > accountExist.timestamp) await Account.updateAccount(accObj.accountId, accObj)
      } else {
        // await Account.insertAccount(accObj)
        combineAccounts.push(accObj)
      }

      //check global network account updates
      if (accObj.accountId === config.globalNetworkAccount) {
        setGlobalNetworkAccount(accObj)
      }
      if (accObj.isGlobal) {
        globalAccountsMap.set(accObj.accountId, {
          hash: accObj.hash,
          timestamp: accObj.timestamp,
        })
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
      appReceiptId: appReceiptData ? appReceiptData.accountId : tx.txId, // Set txId if appReceiptData lacks appReceiptId
      timestamp: tx.timestamp,
      cycleNumber: cycle,
      data: appReceiptData ? appReceiptData.data : {},
      originalTxData: tx.originalTxData,
    }
    // await Transaction.insertTransaction(txObj)
    combineTransactions.push(txObj)
    // Receipts size can be big, better to save per 100
    if (combineReceipts.length >= 100) {
      if (socketServer) {
        const signedDataToSend = Crypto.sign({
          receipts: combineReceipts,
        })
        socketServer.emit('RECEIPT', signedDataToSend)
      }
      await Receipt.bulkInsertReceipts(combineReceipts)
      sendDataToAdjacentArchivers(DataType.RECEIPT, txsData)
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
      const signedDataToSend = Crypto.sign({
        receipts: combineReceipts,
      })
      socketServer.emit('RECEIPT', signedDataToSend)
    }
    await Receipt.bulkInsertReceipts(combineReceipts)
    sendDataToAdjacentArchivers(DataType.RECEIPT, txsData)
  }
  if (combineAccounts.length > 0) await Account.bulkInsertAccounts(combineAccounts)
  if (combineTransactions.length > 0) await Transaction.bulkInsertTransactions(combineTransactions)
}

export const validateCycleData = (cycleRecord: P2PTypes.CycleCreatorTypes.CycleData) => {
  let err = Utils.validateTypes(cycleRecord, {
    activated: 'a',
    activatedPublicKeys: 'a',
    active: 'n',
    apoptosized: 'a',
    archiverListHash: 's',
    counter: 'n',
    desired: 'n',
    duration: 'n',
    expired: 'n',
    joined: 'a',
    joinedArchivers: 'a',
    joinedConsensors: 'a',
    leavingArchivers: 'a',
    lost: 'a',
    lostSyncing: 'a',
    marker: 's',
    maxSyncTime: 'n',
    mode: 's',
    networkConfigHash: 's',
    networkId: 's',
    nodeListHash: 's',
    previous: 's',
    refreshedArchivers: 'a',
    refreshedConsensors: 'a',
    refuted: 'a',
    removed: 'a',
    returned: 'a',
    standbyAdd: 'a',
    standbyNodeListHash: 's',
    standbyRemove: 'a',
    start: 'n',
    syncing: 'n',
    target: 'n',
    archiversAtShutdown: 'a?',
  })
  if (err) {
    Logger.mainLogger.error('Invalid Cycle Record', err)
    return false
  }
  const cycleRecordWithoutMarker = { ...cycleRecord }
  delete cycleRecordWithoutMarker.marker
  if (computeCycleMarker(cycleRecordWithoutMarker) !== cycleRecord.marker) {
    Logger.mainLogger.error('Invalid Cycle Record: cycle marker does not match with the computed marker')
    return false
  }
  return true
}

export const storeCycleData = async (cycles: P2PTypes.CycleCreatorTypes.CycleData[] = []): Promise<void> => {
  if (cycles && cycles.length <= 0) return
  const bucketSize = 1000
  let combineCycles = []
  for (let i = 0; i < cycles.length; i++) {
    // eslint-disable-next-line security/detect-object-injection
    const cycleRecord = cycles[i]

    const cycleObj: DbCycle = {
      counter: cycleRecord.counter,
      cycleMarker: cycleRecord.marker,
      cycleRecord,
    }
    if (config.dataLogWrite && CycleLogWriter) CycleLogWriter.writeToLog(`${JSON.stringify(cycleObj)}\n`)
    if (socketServer) {
      const signedDataToSend = Crypto.sign({
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

interface StoreAccountParam {
  accounts?: Account.AccountCopy[]
  receipts?: Transaction.Transaction[]
}

export const storeAccountData = async (restoreData: StoreAccountParam = {}): Promise<void> => {
  console.log(
    'RestoreData',
    'accounts',
    restoreData.accounts ? restoreData.accounts.length : 0,
    'receipts',
    restoreData.receipts ? restoreData.receipts.length : 0
  )
  const { accounts, receipts } = restoreData
  if (profilerInstance) profilerInstance.profileSectionStart('store_account_data')
  storingAccountData = true
  if (!accounts && !receipts) return
  if (socketServer && accounts) {
    const signedDataToSend = Crypto.sign({
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
    const combineTransactions = []
    for (let i = 0; i < receipts.length; i++) {
      // eslint-disable-next-line security/detect-object-injection
      const receipt = receipts[i]
      const txObj = {
        txId: receipt.data.txId || receipt.txId,
        appReceiptId: receipt.accountId,
        timestamp: receipt.timestamp,
        cycleNumber: receipt.cycleNumber,
        data: receipt.data,
        originalTxData: {},
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
    const accountData = { ...combineAccountsData }
    clearCombinedAccountsData()
    storeAccountData(accountData)
  } else {
    storingAccountData = false
  }
}

export const storeOriginalTxData = async (
  originalTxsData: OriginalTxsData.OriginalTxData[] = [],
  forceSaved = false
): Promise<void> => {
  if (!originalTxsData || !Array.isArray(originalTxsData) || originalTxsData.length <= 0) return
  const bucketSize = 1000
  let combineOriginalTxsData = []
  let txsData: TxsData[] = []
  if (!forceSaved) if (stopSavingTxData) return
  for (const originalTxData of originalTxsData) {
    const txId = originalTxData.txId
    if (originalTxsMap.has(txId)) {
      // console.log('ORIGINAL_TX_DATA', 'Skip', txId, senderInfo)
      continue
    }
    if (validateOriginalTxData(originalTxData) === false) continue
    originalTxsMap.set(txId, originalTxData.cycle)
    if (missingOriginalTxsMap.has(txId)) missingOriginalTxsMap.delete(txId)

    if (config.dataLogWrite && OriginalTxDataLogWriter)
      OriginalTxDataLogWriter.writeToLog(`${JSON.stringify(originalTxData)}\n`)
    combineOriginalTxsData.push(originalTxData)
    txsData.push({
      txId: txId,
      cycle: originalTxData.cycle,
    })
    // console.log('ORIGINAL_TX_DATA', 'Save', txId, senderInfo)
    if (combineOriginalTxsData.length >= bucketSize) {
      if (socketServer) {
        const signedDataToSend = Crypto.sign({
          originalTxsData: combineOriginalTxsData,
        })
        socketServer.emit('RECEIPT', signedDataToSend)
      }
      await OriginalTxsData.bulkInsertOriginalTxsData(combineOriginalTxsData)
      sendDataToAdjacentArchivers(DataType.ORIGINAL_TX_DATA, txsData)
      combineOriginalTxsData = []
      txsData = []
    }
  }
  if (combineOriginalTxsData.length > 0) {
    if (socketServer) {
      const signedDataToSend = Crypto.sign({
        originalTxsData: combineOriginalTxsData,
      })
      socketServer.emit('RECEIPT', signedDataToSend)
    }
    await OriginalTxsData.bulkInsertOriginalTxsData(combineOriginalTxsData)
    sendDataToAdjacentArchivers(DataType.ORIGINAL_TX_DATA, txsData)
  }
}
interface validateResponse {
  success: boolean
  reason?: string
  error?: string
}

export const validateOriginalTxData = (originalTxData: OriginalTxsData.OriginalTxData) => {
  let err = Utils.validateTypes(originalTxData, {
    txId: 's',
    timestamp: 'n',
    cycle: 'n',
    sign: 'o',
    originalTxData: 'o',
  })
  if (err) {
    Logger.mainLogger.error('Invalid originalTxsData', err)
    return false
  }
  err = Utils.validateTypes(originalTxData.sign, {
    owner: 's',
    sig: 's',
  })
  if (err) {
    Logger.mainLogger.error('Invalid originalTxsData signature', err)
    return false
  }
  return true
}

export const validateGossipData = (data: GossipData): validateResponse => {
  let err = Utils.validateTypes(data, {
    dataType: 's',
    data: 'a',
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
  if (
    data.dataType !== DataType.RECEIPT &&
    data.dataType !== DataType.ORIGINAL_TX_DATA &&
    data.dataType !== DataType.CYCLE
  ) {
    Logger.mainLogger.error('Invalid dataType', data)
    return { success: false, error: 'Invalid dataType' }
  }
  if (!Crypto.verify(data)) {
    Logger.mainLogger.error('Invalid signature', data)
    return { success: false, error: 'Invalid signature' }
  }
  return { success: true }
}

export const processGossipData = (gossipdata: GossipData): void => {
  const { dataType, data, sender } = gossipdata
  if (dataType === DataType.RECEIPT) {
    for (const txData of data as TxsData[]) {
      const { txId, cycle } = txData
      if (receiptsMap.has(txId) && receiptsMap.get(txId) === cycle) {
        // console.log('GOSSIP', 'RECEIPT', 'SKIP', txId, sender)
        continue
      } else missingReceiptsMap.set(txId, cycle)
      // console.log('GOSSIP', 'RECEIPT', 'MISS', txId, sender)
    }
  }
  if (dataType === DataType.ORIGINAL_TX_DATA) {
    for (const txData of data as TxsData[]) {
      const { txId, cycle } = txData
      if (originalTxsMap.has(txId) && originalTxsMap.get(txId) === cycle) {
        // console.log('GOSSIP', 'ORIGINAL_TX_DATA', 'SKIP', txId, sender)
        continue
      } else missingOriginalTxsMap.set(txId, cycle)
      // console.log('GOSSIP', 'ORIGINAL_TX_DATA', 'MISS', txId, sender)
    }
  }
  if (dataType === DataType.CYCLE) {
    collectCycleData(
      data as P2PTypes.CycleCreatorTypes.CycleData[],
      adjacentArchivers.get(sender).ip + ':' + adjacentArchivers.get(sender).port
    )
  }
}

export const collectMissingReceipts = async (): Promise<void> => {
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
  const maxRetry = 3
  let retry = 0
  const archiversToUse: State.ArchiverNodeInfo[] = getArchiversToUse()
  while (cloneMissingReceiptsMap.size > 0 && retry < maxRetry) {
    // eslint-disable-next-line security/detect-object-injection
    let archiver = archiversToUse[retry]
    if (!archiver) archiver = archiversToUse[0]
    const missingReceipts = [...cloneMissingReceiptsMap.keys()]
    for (let start = 0; start < missingReceipts.length; ) {
      let txIdList = []
      const end = start + bucketSize
      if (start > missingReceipts.length) txIdList = missingReceipts.slice(start, end)
      else txIdList = missingReceipts.slice(start)
      const receipts = (await queryTxDataFromArchivers(
        archiver,
        DataType.RECEIPT,
        txIdList
      )) as Receipt.Receipt[]
      if (receipts && receipts.length > -1) {
        const receiptsToSave = []
        for (const receipt of receipts) {
          const { tx, cycle } = receipt
          if (
            tx &&
            cloneMissingReceiptsMap.has(tx['txId']) &&
            cloneMissingReceiptsMap.get(tx['txId']) === cycle
          ) {
            cloneMissingReceiptsMap.delete(tx['txId'])
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

export const getArchiversToUse = (): State.ArchiverNodeInfo[] => {
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

type TxDataFromArchiversResponse = {
  receipts?: Receipt.Receipt[]
  originalTxs?: OriginalTxDB.OriginalTxData[]
}

type QueryTxDataFromArchiversResponse = Receipt.Receipt[] | OriginalTxDB.OriginalTxData[] | null

export const queryTxDataFromArchivers = async (
  archiver: State.ArchiverNodeInfo,
  txDataType: DataType,
  txIdList: string[]
): Promise<QueryTxDataFromArchiversResponse> => {
  let api_route = ''
  if (txDataType === DataType.RECEIPT) {
    // Query from other archivers using receipt endpoint
    // Using the existing GET /receipts endpoint for now; might have to change to POST /receipts endpoint
    api_route = `receipt?txIdList=${JSON.stringify(txIdList)}`
  } else if (txDataType === DataType.ORIGINAL_TX_DATA) {
    api_route = `originalTx?txIdList=${JSON.stringify(txIdList)}`
  }
  const response = (await getJson(
    `http://${archiver.ip}:${archiver.port}/${api_route}`
  )) as TxDataFromArchiversResponse
  if (response) {
    if (txDataType === DataType.RECEIPT) {
      const receipts = response.receipts || null
      if (receipts && receipts.length > -1) {
        return receipts
      }
    } else if (txDataType === DataType.ORIGINAL_TX_DATA) {
      const originalTxs = response.originalTxs || null
      if (originalTxs && originalTxs.length > -1) {
        return originalTxs
      }
    }
  }
  return null
}

export const collectMissingOriginalTxsData = async (): Promise<void> => {
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
  const maxRetry = 3
  let retry = 0
  const archiversToUse: State.ArchiverNodeInfo[] = getArchiversToUse()
  while (cloneMissingOriginalTxsMap.size > 0 && retry < maxRetry) {
    // eslint-disable-next-line security/detect-object-injection
    let archiver = archiversToUse[retry]
    if (!archiver) archiver = archiversToUse[0]
    const missingOriginalTxs = [...cloneMissingOriginalTxsMap.keys()]
    for (let start = 0; start < missingOriginalTxs.length; ) {
      let txIdList = []
      const end = start + bucketSize
      if (start > missingOriginalTxs.length) txIdList = missingOriginalTxs.slice(start, end)
      else txIdList = missingOriginalTxs.slice(start)
      const originalTxs = (await queryTxDataFromArchivers(
        archiver,
        DataType.ORIGINAL_TX_DATA,
        txIdList
      )) as OriginalTxDB.OriginalTxData[]
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

export function cleanOldReceiptsMap(): void {
  // Clean receipts that are older than last 2 cycles
  const cycleNumber = getCurrentCycleCounter() - 1
  for (let [key, value] of receiptsMap) {
    if (value < cycleNumber) {
      receiptsMap.delete(key)
    }
  }
  if (config.VERBOSE) console.log('Clean old receipts map!', getCurrentCycleCounter())
}

export function cleanOldOriginalTxsMap(): void {
  // Clean originalTxs that are older than last 2 cycles
  const cycleNumber = getCurrentCycleCounter() - 1
  for (let [key, value] of originalTxsMap) {
    if (value < cycleNumber) {
      originalTxsMap.delete(key)
    }
  }
  if (config.VERBOSE) console.log('Clean old originalTxs map!', getCurrentCycleCounter())
}

export const scheduleCacheCleanup = (): void => {
  // Set to clean old receipts and originalTxs map every minute
  setInterval(() => {
    cleanOldReceiptsMap()
    cleanOldOriginalTxsMap()
  }, 60000)
}

export const scheduleMissingTxsDataQuery = (): void => {
  // Set to collect missing txs data in every 5 seconds
  setInterval(() => {
    collectMissingReceipts()
    collectMissingOriginalTxsData()
  }, 5000)
}
