import { P2P as P2PTypes } from '@shardus/types'
import { isDeepStrictEqual } from 'util'
import * as Account from '../dbstore/accounts'
import * as Transaction from '../dbstore/transactions'
import * as Receipt from '../dbstore/receipts'
import * as OriginalTxsData from '../dbstore/originalTxsData'
import * as Crypto from '../Crypto'
import {
  clearCombinedAccountsData,
  combineAccountsData,
  collectCycleData,
  nodesPerConsensusGroup,
} from './Data'
import { config } from '../Config'
import * as Logger from '../Logger'
import { nestedCountersInstance } from '../profiler/nestedCounters'
import { profilerInstance } from '../profiler/profiler'
import { getCurrentCycleCounter, shardValuesByCycle, computeCycleMarker } from './Cycles'
import { bulkInsertCycles, queryCycleByMarker, updateCycle } from '../dbstore/cycles'
import * as State from '../State'
import * as Utils from '../Utils'
import { DataType, GossipData, adjacentArchivers, sendDataToAdjacentArchivers, TxData } from './GossipData'
import { postJson } from '../P2P'
import { globalAccountsMap, setGlobalNetworkAccount } from '../GlobalAccount'
import { CycleLogWriter, ReceiptLogWriter, OriginalTxDataLogWriter } from '../Data/DataLogWriter'
import * as OriginalTxDB from '../dbstore/originalTxsData'
import ShardFunction from '../ShardFunctions'
import { ConsensusNodeInfo } from '../NodeList'
import { verifyAccountHash } from '../shardeum/calculateAccountHash'
import { verifyAppReceiptData } from '../shardeum/verifyAppReceiptData'
import { Cycle as DbCycle } from '../dbstore/types'
import { Utils as StringUtils } from '@shardus/types'

export let storingAccountData = false
const processedReceiptsMap: Map<string, number> = new Map()
const receiptsInValidationMap: Map<string, number> = new Map()
const processedOriginalTxsMap: Map<string, number> = new Map()
const originalTxsInValidationMap: Map<string, number> = new Map()
const missingReceiptsMap: Map<string, MissingTx> = new Map()
const missingOriginalTxsMap: Map<string, MissingTx> = new Map()
const collectingMissingReceiptsMap: Map<string, number> = new Map()
const collectingMissingOriginalTxsMap: Map<string, number> = new Map()

interface MissingTx {
  txTimestamp: number
  receivedTimestamp: number
}

const WAIT_TIME_FOR_MISSING_TX_DATA = 2000 // in ms

// For debugging gossip data, set this to true. This will save only the gossip data received from the adjacent archivers.
export const saveOnlyGossipData = false

type GET_TX_RECEIPT_RESPONSE = {
  success: boolean
  receipt?: Receipt.ArchiverReceipt | Receipt.AppliedReceipt2
  reason?: string
}

/**
 * Calls the /get-tx-receipt endpoint of the nodes in the execution group of the receipt to verify the receipt. If "RECEIPT_CONFIRMATIONS" number of nodes return the same receipt, the receipt is deemed valid.
 * @param receipt
 * @param executionGroupNodes
 * @param minConfirmations
 * @returns boolean
 */
const isReceiptRobust = async (
  receipt: Receipt.ArchiverReceipt,
  executionGroupNodes: ConsensusNodeInfo[],
  minConfirmations: number = config.RECEIPT_CONFIRMATIONS
): Promise<{ success: boolean; newReceipt?: Receipt.ArchiverReceipt }> => {
  const result = { success: false }
  // Created signedData with full_receipt = false outside of queryReceipt to avoid signing the same data multiple times
  let signedData = Crypto.sign({
    txId: receipt.tx.txId,
    timestamp: receipt.tx.timestamp,
    full_receipt: false,
  })
  const queryReceipt = async (node: ConsensusNodeInfo): Promise<GET_TX_RECEIPT_RESPONSE | null> => {
    const QUERY_RECEIPT_TIMEOUT_SECOND = 2
    try {
      return (await postJson(
        `http://${node.ip}:${node.port}/get-tx-receipt`,
        signedData,
        QUERY_RECEIPT_TIMEOUT_SECOND
      )) as GET_TX_RECEIPT_RESPONSE
    } catch (error) {
      Logger.mainLogger.error('Error in /get-tx-receipt:', error)
      if (nestedCountersInstance) nestedCountersInstance.countEvent('receipt', 'Error_in_get-tx-receipt')
      return null
    }
  }
  // Use only random 5 x Receipt Confirmations number of nodes from the execution group to reduce the number of nodes to query in large execution groups
  const filteredExecutionGroupNodes = Utils.getRandomItemFromArr(
    executionGroupNodes,
    0,
    5 * config.RECEIPT_CONFIRMATIONS
  )
  const isReceiptEqual = (receipt1: any, receipt2: any): boolean => {
    if (!receipt1 || !receipt2) return false
    const r1 = Utils.deepCopy(receipt1)
    const r2 = Utils.deepCopy(receipt2)

    // The confirmOrChallenge node could be different in the two receipts, so we need to remove it before comparing
    delete r1?.confirmOrChallenge?.nodeId
    delete r1?.confirmOrChallenge?.sign
    delete r2?.confirmOrChallenge?.nodeId
    delete r2?.confirmOrChallenge?.sign

    const equivalent = isDeepStrictEqual(r1, r2)
    return equivalent
  }
  const robustQuery = await Utils.robustQuery(
    filteredExecutionGroupNodes,
    (execNode) => queryReceipt(execNode),
    (rec1: GET_TX_RECEIPT_RESPONSE, rec2: GET_TX_RECEIPT_RESPONSE) =>
      isReceiptEqual(rec1?.receipt, rec2?.receipt),
    minConfirmations,
    false, // set shuffleNodes to false,
    500, // Add 500 ms delay
    true
  )
  if (config.VERBOSE) Logger.mainLogger.debug('robustQuery', receipt.tx.txId, robustQuery)
  if (!robustQuery || !robustQuery.value || !(robustQuery.value as any).receipt) {
    Logger.mainLogger.error(
      `❌ 'null' response from all nodes in receipt-validation for txId: ${receipt.tx.txId} , ${receipt.cycle}, ${receipt.tx.timestamp})
      }`
    )
    if (nestedCountersInstance)
      nestedCountersInstance.countEvent('receipt', 'null_response_from_all_nodes_in_receipt-validation')
    return result
  }

  const robustQueryReceipt = (robustQuery.value as any).receipt as Receipt.AppliedReceipt2

  if (robustQuery.count < minConfirmations) {
    // Wait for 500ms and try fetching the receipt from the nodes that did not respond in the robustQuery
    await Utils.sleep(500)
    let requiredConfirmations = minConfirmations - robustQuery.count
    let nodesToQuery = executionGroupNodes.filter(
      (node) => !robustQuery.nodes.some((n) => n.publicKey === node.publicKey)
    )
    let retryCount = 5 // Retry 5 times
    while (requiredConfirmations > 0) {
      if (nodesToQuery.length === 0) {
        if (retryCount === 0) break
        // Wait for 500ms and try again
        await Utils.sleep(500)
        nodesToQuery = executionGroupNodes.filter(
          (node) => !robustQuery.nodes.some((n) => n.publicKey === node.publicKey)
        )
        retryCount--
        if (nodesToQuery.length === 0) break
      }
      const node = nodesToQuery[0]
      nodesToQuery.splice(0, 1)
      const receiptResult: any = await queryReceipt(node)
      if (!receiptResult || !receiptResult.receipt) continue
      if (isReceiptEqual(robustQueryReceipt, receiptResult.receipt)) {
        requiredConfirmations--
        robustQuery.nodes.push(node)
      }
    }
  }

  // Check if the robustQueryReceipt is the same as our receipt
  const sameReceipt = isReceiptEqual(receipt.appliedReceipt, robustQueryReceipt)

  if (!sameReceipt) {
    Logger.mainLogger.debug(
      `Found different receipt in robustQuery ${receipt.tx.txId} , ${receipt.cycle}, ${receipt.tx.timestamp}`
    )
    if (nestedCountersInstance)
      nestedCountersInstance.countEvent('receipt', 'Found_different_receipt_in_robustQuery')
    if (config.VERBOSE) Logger.mainLogger.debug(receipt.appliedReceipt)
    if (config.VERBOSE) Logger.mainLogger.debug(robustQueryReceipt)
    // update signedData with full_receipt = true
    signedData = Crypto.sign({ txId: receipt.tx.txId, timestamp: receipt.tx.timestamp, full_receipt: true })
    for (const node of robustQuery.nodes) {
      const fullReceiptResult: GET_TX_RECEIPT_RESPONSE = await queryReceipt(node)
      if (config.VERBOSE)
        Logger.mainLogger.debug(
          `'fullReceiptResult ${receipt.tx.txId} , ${receipt.cycle}, ${receipt.tx.timestamp}`,
          fullReceiptResult
        )
      if (!fullReceiptResult || !fullReceiptResult.receipt) {
        Logger.mainLogger.error(
          `Got fullReceiptResult null from robustQuery node for ${receipt.tx.txId} , ${receipt.cycle}, ${receipt.tx.timestamp}`
        )
        continue
      }
      const fullReceipt = fullReceiptResult.receipt as Receipt.ArchiverReceipt
      if (
        isReceiptEqual(fullReceipt.appliedReceipt, robustQueryReceipt) &&
        validateReceiptData(fullReceipt)
      ) {
        if (config.verifyAppReceiptData) {
          const { valid, needToSave } = await verifyAppReceiptData(receipt)
          if (!valid) {
            Logger.mainLogger.error(
              `The app receipt verification failed from robustQuery nodes ${receipt.tx.txId} , ${receipt.cycle}, ${receipt.tx.timestamp}`
            )
            continue
          }
          if (!needToSave) {
            Logger.mainLogger.debug(
              `Found valid full receipt in robustQuery ${receipt.tx.txId} , ${receipt.cycle}, ${receipt.tx.timestamp}`
            )
            Logger.mainLogger.error(
              `Found valid receipt from robustQuery: but no need to save ${receipt.tx.txId} , ${receipt.cycle}, ${receipt.tx.timestamp}`
            )
            return { success: false }
          }
        }
        if (config.verifyAccountData && !verifyAccountHash(fullReceipt)) {
          Logger.mainLogger.error(
            `The account verification failed from robustQuery nodes ${receipt.tx.txId} , ${receipt.cycle}, ${receipt.tx.timestamp}`
          )
          continue
        }
        Logger.mainLogger.debug(
          `Found valid full receipt in robustQuery ${receipt.tx.txId} , ${receipt.cycle}, ${receipt.tx.timestamp}`
        )
        if (nestedCountersInstance)
          nestedCountersInstance.countEvent('receipt', 'Found_valid_full_receipt_in_robustQuery')
        return { success: true, newReceipt: fullReceipt }
      } else {
        Logger.mainLogger.error(
          `The receipt validation failed from robustQuery nodes ${receipt.tx.txId} , ${receipt.cycle}, ${receipt.tx.timestamp}`
        )
        Logger.mainLogger.error(StringUtils.safeStringify(robustQueryReceipt), StringUtils.safeStringify(fullReceipt))
      }
    }
    Logger.mainLogger.error(
      `No valid full receipt found in robustQuery ${receipt.tx.txId} , ${receipt.cycle}, ${receipt.tx.timestamp}`
    )
    if (nestedCountersInstance)
      nestedCountersInstance.countEvent('receipt', 'No_valid_full_receipt_found_in_robustQuery')
    return { success: false }
  }
  return { success: true }
}

/**
 * Validate type and field existence of the receipt data before processing it further
 * @param receipt
 * @returns boolean
 */
export const validateReceiptData = (receipt: Receipt.ArchiverReceipt): boolean => {
  // Add type and field existence check
  let err = Utils.validateTypes(receipt, {
    tx: 'o',
    cycle: 'n',
    beforeStateAccounts: 'a',
    accounts: 'a',
    appReceiptData: 'o?',
    appliedReceipt: 'o',
    executionShardKey: 's',
    globalModification: 'b',
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
  if (receipt.globalModification) return true
  // Global Modification Tx does not have appliedReceipt
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
  const { executionShardKey, cycle, appliedReceipt, globalModification } = receipt
  if (globalModification && config.skipGlobalTxReceiptVerification) return { success: true }
  const { appliedVote, confirmOrChallenge } = appliedReceipt
  const cycleShardData = shardValuesByCycle.get(cycle)
  if (!cycleShardData) {
    Logger.mainLogger.error('Cycle shard data not found')
    if (nestedCountersInstance) nestedCountersInstance.countEvent('receipt', 'Cycle_shard_data_not_found')
    return result
  }
  // Determine the home partition index of the primary account (executionShardKey)
  const { homePartition } = ShardFunction.addressToPartition(cycleShardData.shardGlobals, executionShardKey)
  // Check if the appliedVote node is in the execution group
  if (!cycleShardData.nodeShardDataMap.has(appliedVote.node_id)) {
    Logger.mainLogger.error('Invalid receipt appliedReceipt appliedVote node is not in the active nodesList')
    if (nestedCountersInstance)
      nestedCountersInstance.countEvent('receipt', 'Invalid_receipt_appliedVote_node_not_in_active_nodesList')
    return result
  }
  if (appliedVote.sign.owner !== cycleShardData.nodeShardDataMap.get(appliedVote.node_id).node.publicKey) {
    Logger.mainLogger.error(
      'Invalid receipt appliedReceipt appliedVote node signature owner and node public key does not match'
    )
    if (nestedCountersInstance)
      nestedCountersInstance.countEvent(
        'receipt',
        'Invalid_receipt_appliedVote_node_signature_owner_and_node_public_key_does_not_match'
      )
    return result
  }
  if (!cycleShardData.parititionShardDataMap.get(homePartition).coveredBy[appliedVote.node_id]) {
    Logger.mainLogger.error(
      'Invalid receipt appliedReceipt appliedVote node is not in the execution group of the tx'
    )
    if (nestedCountersInstance)
      nestedCountersInstance.countEvent(
        'receipt',
        'Invalid_receipt_appliedVote_node_not_in_execution_group_of_tx'
      )
    return result
  }
  if (!Crypto.verify(appliedVote)) {
    Logger.mainLogger.error('Invalid receipt appliedReceipt appliedVote signature verification failed')
    if (nestedCountersInstance)
      nestedCountersInstance.countEvent(
        'receipt',
        'Invalid_receipt_appliedVote_signature_verification_failed'
      )
    return result
  }

  // Check if the confirmOrChallenge node is in the execution group
  if (!cycleShardData.nodeShardDataMap.has(confirmOrChallenge.nodeId)) {
    Logger.mainLogger.error(
      'Invalid receipt appliedReceipt confirmOrChallenge node is not in the active nodesList'
    )
    if (nestedCountersInstance)
      nestedCountersInstance.countEvent(
        'receipt',
        'Invalid_receipt_confirmOrChallenge_node_not_in_active_nodesList'
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
    if (nestedCountersInstance)
      nestedCountersInstance.countEvent(
        'receipt',
        'Invalid_receipt_confirmOrChallenge_signature_owner_and_node_public_key_does_not_match'
      )
    return result
  }
  if (!cycleShardData.parititionShardDataMap.get(homePartition).coveredBy[confirmOrChallenge.nodeId]) {
    Logger.mainLogger.error(
      'Invalid receipt appliedReceipt confirmOrChallenge node is not in the execution group of the tx'
    )
    if (nestedCountersInstance)
      nestedCountersInstance.countEvent(
        'receipt',
        'Invalid_receipt_confirmOrChallenge_node_not_in_execution_group_of_tx'
      )
    return result
  }
  if (!Crypto.verify(confirmOrChallenge)) {
    Logger.mainLogger.error('Invalid receipt appliedReceipt confirmOrChallenge signature verification failed')
    if (nestedCountersInstance)
      nestedCountersInstance.countEvent(
        'receipt',
        'Invalid_receipt_confirmOrChallenge_signature_verification_failed'
      )
    return result
  }

  // List the execution group nodes of the tx, Use them to robustQuery to verify the receipt
  const executionGroupNodes = Object.values(
    cycleShardData.parititionShardDataMap.get(homePartition).coveredBy
  ) as unknown as ConsensusNodeInfo[]
  if (config.VERBOSE) Logger.mainLogger.debug('executionGroupNodes', receipt.tx.txId, executionGroupNodes)
  const minConfirmations =
    nodesPerConsensusGroup > config.RECEIPT_CONFIRMATIONS
      ? config.RECEIPT_CONFIRMATIONS
      : Math.ceil(config.RECEIPT_CONFIRMATIONS / 2) // 3 out of 5 nodes
  const { success, newReceipt } = await isReceiptRobust(receipt, executionGroupNodes, minConfirmations)
  if (!success) {
    Logger.mainLogger.error('Invalid receipt: Robust check failed')
    if (nestedCountersInstance)
      nestedCountersInstance.countEvent('receipt', 'Invalid_receipt_robust_check_failed')
    return result
  }
  if (newReceipt) return { success: true, newReceipt }
  return { success: true }
}

export const storeReceiptData = async (
  receipts: Receipt.ArchiverReceipt[],
  senderInfo = '',
  verifyData = false,
  saveOnlyGossipData = false
): Promise<void> => {
  if (!receipts || !Array.isArray(receipts) || receipts.length <= 0) return
  const bucketSize = 1000
  let combineReceipts = []
  let combineAccounts = []
  let combineTransactions = []
  let txDataList: TxData[] = []
  if (saveOnlyGossipData) return
  for (let receipt of receipts) {
    const txId = receipt?.tx?.txId
    const timestamp = receipt?.tx?.timestamp
    if (!txId || !timestamp) continue
    if (
      (processedReceiptsMap.has(txId) && processedReceiptsMap.get(txId) === timestamp) ||
      (receiptsInValidationMap.has(txId) && receiptsInValidationMap.get(txId) === timestamp)
    ) {
      if (config.VERBOSE) console.log('RECEIPT', 'Skip', txId, timestamp, senderInfo)
      continue
    }
    if (config.VERBOSE) console.log('RECEIPT', 'Validate', txId, timestamp, senderInfo)
    receiptsInValidationMap.set(txId, timestamp)
    if (nestedCountersInstance) nestedCountersInstance.countEvent('receipt', 'Validate_receipt')
    if (!validateReceiptData(receipt)) {
      Logger.mainLogger.error('Invalid receipt: Validation failed', txId, receipt.cycle, timestamp)
      receiptsInValidationMap.delete(txId)
      if (nestedCountersInstance)
        nestedCountersInstance.countEvent('receipt', 'Invalid_receipt_validation_failed')
      if (profilerInstance) profilerInstance.profileSectionEnd('Validate_receipt')
      continue
    }

    if (verifyData) {
      const existingReceipt = await Receipt.queryReceiptByReceiptId(txId)
      if (
        existingReceipt &&
        receipt.appliedReceipt &&
        receipt.appliedReceipt.confirmOrChallenge &&
        receipt.appliedReceipt.confirmOrChallenge.message === 'challenge'
      ) {
        // If the existing receipt is confirmed, and the new receipt is challenged, then skip saving the new receipt
        if (existingReceipt.appliedReceipt.confirmOrChallenge.message === 'confirm') {
          Logger.mainLogger.error(
            `Existing receipt is confirmed, but new receipt is challenged ${txId}, ${receipt.cycle}, ${timestamp}`
          )
          receiptsInValidationMap.delete(txId)
          if (nestedCountersInstance)
            nestedCountersInstance.countEvent(
              'receipt',
              'Existing_receipt_is_confirmed_but_new_receipt_is_challenged'
            )
          if (profilerInstance) profilerInstance.profileSectionEnd('Validate_receipt')
          continue
        }
      }
      if (config.verifyAppReceiptData) {
        const { valid, needToSave } = await verifyAppReceiptData(receipt, existingReceipt)
        if (!valid) {
          Logger.mainLogger.error(
            'Invalid receipt: App Receipt Verification failed',
            txId,
            receipt.cycle,
            timestamp
          )
          receiptsInValidationMap.delete(txId)
          if (nestedCountersInstance)
            nestedCountersInstance.countEvent('receipt', 'Invalid_receipt_app_receipt_verification_failed')
          if (profilerInstance) profilerInstance.profileSectionEnd('Validate_receipt')
          continue
        }
        if (!needToSave) {
          Logger.mainLogger.error('Valid receipt: but no need to save', txId, receipt.cycle, timestamp)
          receiptsInValidationMap.delete(txId)
          if (nestedCountersInstance)
            nestedCountersInstance.countEvent('receipt', 'Valid_receipt_but_no_need_to_save')
          if (profilerInstance) profilerInstance.profileSectionEnd('Validate_receipt')
          continue
        }
      }
      if (config.verifyAccountData && !verifyAccountHash(receipt)) {
        Logger.mainLogger.error(
          'Invalid receipt: Account Verification failed',
          txId,
          receipt.cycle,
          timestamp
        )
        receiptsInValidationMap.delete(txId)
        if (nestedCountersInstance)
          nestedCountersInstance.countEvent('receipt', 'Invalid_receipt_account_verification_failed')
        if (profilerInstance) profilerInstance.profileSectionEnd('Validate_receipt')
        continue
      }
      const { success, newReceipt } = await verifyReceiptData(receipt)
      if (!success) {
        Logger.mainLogger.error('Invalid receipt: Verification failed', txId, receipt.cycle, timestamp)
        receiptsInValidationMap.delete(txId)
        if (nestedCountersInstance)
          nestedCountersInstance.countEvent('receipt', 'Invalid_receipt_verification_failed')
        if (profilerInstance) profilerInstance.profileSectionEnd('Validate_receipt')
        continue
      }
      if (newReceipt) receipt = newReceipt
      if (profilerInstance) profilerInstance.profileSectionEnd('Validate_receipt')
    }
    // await Receipt.insertReceipt({
    //   ...receipts[i],
    //   receiptId: tx.txId,
    //   timestamp: tx.timestamp,
    // })
    const { accounts, cycle, tx, appReceiptData, appliedReceipt } = receipt
    if (config.VERBOSE) console.log('RECEIPT', 'Save', txId, timestamp, senderInfo)
    processedReceiptsMap.set(tx.txId, tx.timestamp)
    receiptsInValidationMap.delete(tx.txId)
    if (missingReceiptsMap.has(tx.txId)) missingReceiptsMap.delete(tx.txId)
    combineReceipts.push({
      ...receipt,
      receiptId: tx.txId,
      timestamp: tx.timestamp,
    })
    if (config.dataLogWrite && ReceiptLogWriter)
      ReceiptLogWriter.writeToLog(
        `${StringUtils.safeStringify({
          ...receipt,
          receiptId: tx.txId,
          timestamp: tx.timestamp,
        })}\n`
      )
    txDataList.push({ txId, timestamp })
    // If the receipt is a challenge, then skip updating its accounts data or transaction data
    if (
      appliedReceipt &&
      appliedReceipt.confirmOrChallenge &&
      appliedReceipt.confirmOrChallenge.message === 'challenge'
    )
      continue
    for (const account of accounts) {
      const accObj: Account.AccountCopy = {
        accountId: account.accountId,
        data: account.data,
        timestamp: account.timestamp,
        hash: account.hash,
        cycleNumber: cycle,
        isGlobal: account.isGlobal || false,
      }
      if (account.timestamp !== account.data['timestamp'])
        Logger.mainLogger.error('Mismatched account timestamp', txId, account.accountId)
      if (account.hash !== account.data['hash'])
        Logger.mainLogger.error('Mismatched account hash', txId, account.accountId)

      const accountExist = await Account.queryAccountByAccountId(account.accountId)
      if (accountExist) {
        if (accObj.timestamp > accountExist.timestamp) await Account.updateAccount(accObj)
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
      await Receipt.bulkInsertReceipts(combineReceipts)
      if (State.isActive) sendDataToAdjacentArchivers(DataType.RECEIPT, txDataList)
      combineReceipts = []
      txDataList = []
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
    if (State.isActive) sendDataToAdjacentArchivers(DataType.RECEIPT, txDataList)
  }
  if (combineAccounts.length > 0) await Account.bulkInsertAccounts(combineAccounts)
  if (combineTransactions.length > 0) await Transaction.bulkInsertTransactions(combineTransactions)
}

export const validateCycleData = (cycleRecord: P2PTypes.CycleCreatorTypes.CycleData): boolean => {
  const err = Utils.validateTypes(cycleRecord, {
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
    if (config.dataLogWrite && CycleLogWriter) CycleLogWriter.writeToLog(`${StringUtils.safeStringify(cycleObj)}\n`)
    const cycleExist = await queryCycleByMarker(cycleObj.cycleMarker)
    if (cycleExist) {
      if (StringUtils.safeStringify(cycleObj) !== StringUtils.safeStringify(cycleExist))
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
    for (const receipt of receipts) {
      const txObj: Transaction.Transaction = {
        txId: receipt.data.txId || receipt.txId,
        appReceiptId: receipt.appReceiptId,
        timestamp: receipt.timestamp,
        cycleNumber: receipt.cycleNumber,
        data: receipt.data,
        originalTxData: {},
      }
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
  senderInfo = '',
  saveOnlyGossipData = false
): Promise<void> => {
  if (!originalTxsData || !Array.isArray(originalTxsData) || originalTxsData.length <= 0) return
  const bucketSize = 1000
  let combineOriginalTxsData = []
  let txDataList: TxData[] = []
  if (saveOnlyGossipData) return
  for (const originalTxData of originalTxsData) {
    const { txId, timestamp } = originalTxData
    if (!txId || !timestamp) continue
    if (
      (processedOriginalTxsMap.has(txId) && processedOriginalTxsMap.get(txId) === timestamp) ||
      (originalTxsInValidationMap.has(txId) && originalTxsInValidationMap.get(txId) === timestamp)
    ) {
      if (config.VERBOSE) console.log('ORIGINAL_TX_DATA', 'Skip', txId, timestamp, senderInfo)
      continue
    }
    if (config.VERBOSE) console.log('ORIGINAL_TX_DATA', 'Validate', txId, timestamp, senderInfo)
    if (validateOriginalTxData(originalTxData) === false) {
      Logger.mainLogger.error('Invalid originalTxData: Validation failed', txId)
      originalTxsInValidationMap.delete(txId)
      continue
    }
    if (config.VERBOSE) console.log('ORIGINAL_TX_DATA', 'Save', txId, timestamp, senderInfo)
    processedOriginalTxsMap.set(txId, timestamp)
    originalTxsInValidationMap.delete(txId)
    if (missingOriginalTxsMap.has(txId)) missingOriginalTxsMap.delete(txId)

    if (config.dataLogWrite && OriginalTxDataLogWriter)
      OriginalTxDataLogWriter.writeToLog(`${StringUtils.safeStringify(originalTxData)}\n`)
    combineOriginalTxsData.push(originalTxData)
    txDataList.push({ txId, timestamp })
    if (combineOriginalTxsData.length >= bucketSize) {
      await OriginalTxsData.bulkInsertOriginalTxsData(combineOriginalTxsData)
      if (State.isActive) sendDataToAdjacentArchivers(DataType.ORIGINAL_TX_DATA, txDataList)
      combineOriginalTxsData = []
      txDataList = []
    }
  }
  if (combineOriginalTxsData.length > 0) {
    await OriginalTxsData.bulkInsertOriginalTxsData(combineOriginalTxsData)
    if (State.isActive) sendDataToAdjacentArchivers(DataType.ORIGINAL_TX_DATA, txDataList)
  }
}
interface validateResponse {
  success: boolean
  reason?: string
  error?: string
}

export const validateOriginalTxData = (originalTxData: OriginalTxsData.OriginalTxData): boolean => {
  const err = Utils.validateTypes(originalTxData, {
    txId: 's',
    timestamp: 'n',
    cycle: 'n',
    // sign: 'o',
    originalTxData: 'o',
  })
  if (err) {
    Logger.mainLogger.error('Invalid originalTxsData', err)
    return false
  }
  // err = Utils.validateTypes(originalTxData.sign, {
  //   owner: 's',
  //   sig: 's',
  // })
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
  const receivedTimestamp = Date.now()
  if (dataType === DataType.RECEIPT) {
    for (const { txId, timestamp } of data as TxData[]) {
      if (
        (processedReceiptsMap.has(txId) && processedReceiptsMap.get(txId) === timestamp) ||
        (receiptsInValidationMap.has(txId) && receiptsInValidationMap.get(txId) === timestamp) ||
        (collectingMissingReceiptsMap.has(txId) && collectingMissingReceiptsMap.get(txId) === timestamp)
      ) {
        // console.log('GOSSIP', 'RECEIPT', 'SKIP', txId, sender)
        continue
      } else missingReceiptsMap.set(txId, { txTimestamp: timestamp, receivedTimestamp })
      // console.log('GOSSIP', 'RECEIPT', 'MISS', txId, sender)
    }
  }
  if (dataType === DataType.ORIGINAL_TX_DATA) {
    for (const { txId, timestamp } of data as TxData[]) {
      if (
        (processedOriginalTxsMap.has(txId) && processedOriginalTxsMap.get(txId) === timestamp) ||
        (originalTxsInValidationMap.has(txId) && originalTxsInValidationMap.get(txId) === timestamp) ||
        (collectingMissingOriginalTxsMap.has(txId) && collectingMissingOriginalTxsMap.get(txId) === timestamp)
      ) {
        // console.log('GOSSIP', 'ORIGINAL_TX_DATA', 'SKIP', txId, sender)
        continue
      } else missingOriginalTxsMap.set(txId, { txTimestamp: timestamp, receivedTimestamp })
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
  if (missingReceiptsMap.size === 0) return
  const bucketSize = 100
  const currentTimestamp = Date.now()
  const cloneMissingReceiptsMap: Map<string, number> = new Map()
  for (const [txId, { txTimestamp, receivedTimestamp }] of missingReceiptsMap) {
    if (currentTimestamp - receivedTimestamp > WAIT_TIME_FOR_MISSING_TX_DATA) {
      cloneMissingReceiptsMap.set(txId, txTimestamp)
      collectingMissingReceiptsMap.set(txId, txTimestamp)
      missingReceiptsMap.delete(txId)
    }
  }
  if (cloneMissingReceiptsMap.size === 0) return
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
    const txIdList: [string, number][] = []
    let totalEntries = cloneMissingReceiptsMap.size
    for (const [txId, txTimestamp] of cloneMissingReceiptsMap) {
      totalEntries--
      if (
        (processedReceiptsMap.has(txId) && processedReceiptsMap.get(txId) === txTimestamp) ||
        (receiptsInValidationMap.has(txId) && receiptsInValidationMap.get(txId) === txTimestamp)
      ) {
        cloneMissingReceiptsMap.delete(txId)
        collectingMissingReceiptsMap.delete(txId)
        if (totalEntries !== 0) continue
      } else txIdList.push([txId, txTimestamp])
      if (txIdList.length !== bucketSize && totalEntries !== 0) continue
      if (txIdList.length === 0) continue
      const receipts = (await queryTxDataFromArchivers(
        archiver,
        DataType.RECEIPT,
        txIdList
      )) as Receipt.Receipt[]
      if (receipts && receipts.length > -1) {
        const receiptsToSave = []
        for (const receipt of receipts) {
          const { receiptId, timestamp } = receipt
          if (
            cloneMissingReceiptsMap.has(receiptId) &&
            cloneMissingReceiptsMap.get(receiptId) === timestamp
          ) {
            cloneMissingReceiptsMap.delete(receiptId)
            collectingMissingReceiptsMap.delete(txId)
            receiptsToSave.push(receipt)
          }
        }
        await storeReceiptData(receiptsToSave, archiver.ip + ':' + archiver.port, true)
      }
    }
    retry++
  }
  if (cloneMissingReceiptsMap.size > 0) {
    Logger.mainLogger.debug(
      'Receipts TxId that are failed to get from other archivers',
      cloneMissingReceiptsMap
    )
    // Clear the failed txIds from the collectingMissingReceiptsMap
    for (const [txId] of cloneMissingReceiptsMap) {
      collectingMissingReceiptsMap.delete(txId)
    }
  }
}

export const getArchiversToUse = (): State.ArchiverNodeInfo[] => {
  let archiversToUse: State.ArchiverNodeInfo[] = []
  const MAX_ARCHIVERS_TO_SELECT = 3
  // Choosing MAX_ARCHIVERS_TO_SELECT random archivers from the active archivers list
  if (State.activeArchivers.length <= MAX_ARCHIVERS_TO_SELECT) {
    State.activeArchivers.forEach(
      (archiver) => archiver.publicKey !== State.getNodeInfo().publicKey && archiversToUse.push(archiver)
    )
  } else {
    // Filter out the adjacent archivers and self archiver from the active archivers list
    const activeArchivers = [...State.activeArchivers].filter(
      (archiver) =>
        adjacentArchivers.has(archiver.publicKey) || archiver.publicKey === State.getNodeInfo().publicKey
    )
    archiversToUse = Utils.getRandomItemFromArr(activeArchivers, 0, MAX_ARCHIVERS_TO_SELECT)
    if (archiversToUse.length < MAX_ARCHIVERS_TO_SELECT) {
      const requiredArchivers = MAX_ARCHIVERS_TO_SELECT - archiversToUse.length
      // If the required archivers are not selected, then get it from the adjacent archivers
      archiversToUse = [
        ...archiversToUse,
        ...Utils.getRandomItemFromArr([...adjacentArchivers.values()], requiredArchivers),
      ]
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
  txIdList: [string, number][]
): Promise<QueryTxDataFromArchiversResponse> => {
  let api_route = ''
  if (txDataType === DataType.RECEIPT) {
    api_route = `receipt`
  } else if (txDataType === DataType.ORIGINAL_TX_DATA) {
    api_route = `originalTx`
  }
  const signedData = Crypto.sign({ txIdList, sender: State.getNodeInfo().publicKey })
  const response = (await postJson(
    `http://${archiver.ip}:${archiver.port}/${api_route}`,
    signedData
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
  if (missingOriginalTxsMap.size === 0) return
  const bucketSize = 100
  const currentTimestamp = Date.now()
  const cloneMissingOriginalTxsMap: Map<string, number> = new Map()
  for (const [txId, { txTimestamp, receivedTimestamp }] of missingOriginalTxsMap) {
    if (currentTimestamp - receivedTimestamp > WAIT_TIME_FOR_MISSING_TX_DATA) {
      cloneMissingOriginalTxsMap.set(txId, txTimestamp)
      collectingMissingOriginalTxsMap.set(txId, txTimestamp)
      missingOriginalTxsMap.delete(txId)
    }
  }
  if (cloneMissingOriginalTxsMap.size === 0) return
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
    const txIdList: [string, number][] = []
    let totalEntries = cloneMissingOriginalTxsMap.size
    for (const [txId, txTimestamp] of cloneMissingOriginalTxsMap) {
      totalEntries--
      if (
        (processedOriginalTxsMap.has(txId) && processedOriginalTxsMap.get(txId) === txTimestamp) ||
        (originalTxsInValidationMap.has(txId) && originalTxsInValidationMap.get(txId) === txTimestamp)
      ) {
        cloneMissingOriginalTxsMap.delete(txId)
        collectingMissingOriginalTxsMap.delete(txId)
        if (totalEntries !== 0) continue
      } else txIdList.push([txId, txTimestamp])
      if (txIdList.length !== bucketSize && totalEntries !== 0) continue
      if (txIdList.length === 0) continue
      const originalTxs = (await queryTxDataFromArchivers(
        archiver,
        DataType.ORIGINAL_TX_DATA,
        txIdList
      )) as OriginalTxDB.OriginalTxData[]
      if (originalTxs && originalTxs.length > -1) {
        const originalTxsDataToSave = []
        for (const originalTx of originalTxs) {
          const { txId, timestamp } = originalTx
          if (cloneMissingOriginalTxsMap.has(txId) && cloneMissingOriginalTxsMap.get(txId) === timestamp) {
            cloneMissingOriginalTxsMap.delete(txId)
            collectingMissingOriginalTxsMap.delete(txId)
            originalTxsDataToSave.push(originalTx)
          }
        }
        await storeOriginalTxData(originalTxsDataToSave, archiver.ip + ':' + archiver.port)
      }
    }
    retry++
  }
  if (cloneMissingOriginalTxsMap.size > 0) {
    Logger.mainLogger.debug(
      'OriginalTxsData TxId that are failed to get from other archivers',
      cloneMissingOriginalTxsMap
    )
    // Clear the failed txIds from the collectingMissingOriginalTxsMap
    for (const [txId] of cloneMissingOriginalTxsMap) {
      collectingMissingOriginalTxsMap.delete(txId)
    }
  }
}

export function cleanOldReceiptsMap(timestamp: number): void {
  let savedReceiptsCount = 0
  for (const [key, value] of processedReceiptsMap) {
    if (value < timestamp) {
      processedReceiptsMap.delete(key)
      savedReceiptsCount++
    }
  }
  Logger.mainLogger.debug(
    `Clean ${savedReceiptsCount} old receipts from the processed receipts cache on cycle ${getCurrentCycleCounter()}`
  )
}

export function cleanOldOriginalTxsMap(timestamp: number): void {
  let savedOriginalTxsCount = 0
  for (const [key, value] of processedOriginalTxsMap) {
    if (value < timestamp) {
      if (!processedReceiptsMap.has(key))
        Logger.mainLogger.error('The processed receipt is not found for originalTx', key, value)
      processedOriginalTxsMap.delete(key)
      savedOriginalTxsCount++
    }
  }
  Logger.mainLogger.debug(
    `Clean ${savedOriginalTxsCount} old originalTxsData from the processed originalTxsData cache on cycle ${getCurrentCycleCounter()}`
  )
}

export const scheduleMissingTxsDataQuery = (): void => {
  // Set to collect missing txs data in every 5 seconds
  setInterval(() => {
    collectMissingReceipts()
    collectMissingOriginalTxsData()
  }, 1000)
}
