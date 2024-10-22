import { P2P as P2PTypes } from '@shardus/types'
import { isDeepStrictEqual } from 'util'
import * as Account from '../dbstore/accounts'
import * as Transaction from '../dbstore/transactions'
import * as Receipt from '../dbstore/receipts'
import * as OriginalTxsData from '../dbstore/originalTxsData'
import * as ProcessedTransaction from '../dbstore/processedTxs'
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
import { DataType, GossipData, sendDataToAdjacentArchivers, TxData } from './GossipData'
import { postJson } from '../P2P'
import { globalAccountsMap, setGlobalNetworkAccount } from '../GlobalAccount'
import { CycleLogWriter, ReceiptLogWriter, OriginalTxDataLogWriter } from '../Data/DataLogWriter'
import * as OriginalTxDB from '../dbstore/originalTxsData'
import ShardFunction from '../ShardFunctions'
import { ConsensusNodeInfo } from '../NodeList'
import { accountSpecificHash, verifyAccountHash } from '../shardeum/calculateAccountHash'
import { verifyAppReceiptData } from '../shardeum/verifyAppReceiptData'
import { Cycle as DbCycle } from '../dbstore/types'
import { Utils as StringUtils } from '@shardus/types'
import { offloadReceipt } from '../primary-process'

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
  senders: string[]
}

type GET_TX_RECEIPT_RESPONSE = {
  success: boolean
  receipt?: Receipt.ArchiverReceipt | Receipt.SignedReceipt
  reason?: string
}

export interface ReceiptVerificationResult {
  success: boolean
  failedReasons?: string[]
  nestedCounterMessages?: string[]
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
      `❌ 'null' response from all nodes in receipt-validation for txId: ${receipt.tx.txId} , ${receipt.cycle}, ${receipt.tx.timestamp}
      }`
    )
    if (nestedCountersInstance)
      nestedCountersInstance.countEvent('receipt', 'null_response_from_all_nodes_in_receipt-validation')
    return result
  }

  const robustQueryReceipt = (robustQuery.value as any).receipt as Receipt.SignedReceipt

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
  const sameReceipt = isReceiptEqual(receipt.signedReceipt, robustQueryReceipt)

  if (!sameReceipt) {
    Logger.mainLogger.debug(
      `Found different receipt in robustQuery ${receipt.tx.txId} , ${receipt.cycle}, ${receipt.tx.timestamp}`
    )
    if (nestedCountersInstance)
      nestedCountersInstance.countEvent('receipt', 'Found_different_receipt_in_robustQuery')
    if (config.VERBOSE) Logger.mainLogger.debug(receipt.signedReceipt)
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
        isReceiptEqual(fullReceipt.signedReceipt, robustQueryReceipt) &&
        validateArchiverReceipt(fullReceipt)
      ) {
        if (config.verifyAppReceiptData) {
          if (profilerInstance) profilerInstance.profileSectionStart('Verify_app_receipt_data')
          if (nestedCountersInstance) nestedCountersInstance.countEvent('receipt', 'Verify_app_receipt_data')
          const { valid, needToSave } = await verifyAppReceiptData(receipt)
          if (profilerInstance) profilerInstance.profileSectionEnd('Verify_app_receipt_data')
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
        if (config.verifyAccountData) {
          if (profilerInstance) profilerInstance.profileSectionStart('Verify_receipt_account_data')
          if (nestedCountersInstance)
            nestedCountersInstance.countEvent('receipt', 'Verify_receipt_account_data')
          const result = verifyAccountHash(fullReceipt)
          if (profilerInstance) profilerInstance.profileSectionEnd('Verify_receipt_account_data')
          if (!result) {
            Logger.mainLogger.error(
              `The account verification failed from robustQuery nodes ${receipt.tx.txId} , ${receipt.cycle}, ${receipt.tx.timestamp}`
            )
            continue
          }
        }
        if (config.verifyReceiptData) {
          if (profilerInstance) profilerInstance.profileSectionStart('Verify_app_receipt_data')
          if (nestedCountersInstance) nestedCountersInstance.countEvent('receipt', 'Verify_app_receipt_data')
          const { success } = await verifyReceiptData(fullReceipt, false)
          if (profilerInstance) profilerInstance.profileSectionEnd('Verify_app_receipt_data')
          if (!success) {
            Logger.mainLogger.error(
              `The receipt validation failed from robustQuery nodes ${receipt.tx.txId} , ${receipt.cycle}, ${receipt.tx.timestamp}`
            )
            continue
          }
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
        Logger.mainLogger.error(
          StringUtils.safeStringify(robustQueryReceipt),
          StringUtils.safeStringify(fullReceipt)
        )
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
export const validateArchiverReceipt = (receipt: Receipt.ArchiverReceipt): boolean => {
  console.log('[debug-log] Received validateArchiverReceipt: ', StringUtils.safeStringify(receipt))

  // Add type and field existence check
  let err = Utils.validateTypes(receipt, {
    tx: 'o',
    cycle: 'n',
    afterStates: 'a',
    beforeStates: 'a',
    signedReceipt: 'o',
    appReceiptData: 'o?',
    executionShardKey: 's',
    globalModification: 'b',
  })
  if (err) {
    Logger.mainLogger.error('Invalid receipt data', err)
    return false
  }
  err = Utils.validateTypes(receipt.tx, {
    txId: 's',
    timestamp: 'n',
    originalTxData: 'o',
  })
  if (err) {
    Logger.mainLogger.error('Invalid receipt tx data', err)
    return false
  }
  for (const account of receipt.beforeStates) {
    err = Utils.validateTypes(account, {
      hash: 's',
      data: 'o',
      isGlobal: 'b',
      accountId: 's',
      timestamp: 'n',
      // cycleNumber: 'n', it is not present in the beforeStateAccounts data
    })
    if (err) {
      Logger.mainLogger.error('Invalid receipt beforeStateAccounts data', err)
      return false
    }
  }
  for (const account of receipt.afterStates) {
    err = Utils.validateTypes(account, {
      hash: 's',
      data: 'o',
      isGlobal: 'b',
      accountId: 's',
      timestamp: 'n',
      // cycleNumber: 'n', it is not present in the beforeStateAccounts data
    })
    if (err) {
      Logger.mainLogger.error('Invalid receipt accounts data', err)
      return false
    }
  }
  if (receipt.globalModification) return true
  // Global Modification Tx does not have appliedReceipt
  const signedReceiptToValidate = {
    proposal: 'o',
    proposalHash: 's',
    signaturePack: 'a',
  }
  // if (config.newPOQReceipt === false) delete appliedReceiptToValidate.confirmOrChallenge
  err = Utils.validateTypes(receipt.signedReceipt, signedReceiptToValidate)
  if (err) {
    Logger.mainLogger.error('Invalid receipt appliedReceipt data', err)
    return false
  }
  const proposalToValidate = {
    txid: 's',
    applied: 'b',
    accountIDs: 'a',
    cant_preApply: 'b',
    afterStateHashes: 'a',
    beforeStateHashes: 'a',
    appReceiptDataHash: 's',
  }
  // if (config.newPOQReceipt === false) {
  // delete appliedVoteToValidate.node_id
  // delete appliedVoteToValidate.sign
  // }
  err = Utils.validateTypes(receipt.signedReceipt.proposal, proposalToValidate)
  if (err) {
    Logger.mainLogger.error('Invalid receipt appliedReceipt appliedVote data', err)
    return false
  }
  for (const signature of receipt.signedReceipt.signaturePack) {
    err = Utils.validateTypes(signature, {
      owner: 's',
      sig: 's',
    })
    if (err) {
      Logger.mainLogger.error('Invalid receipt appliedReceipt signatures data', err)
      return false
    }
  }
  // if (config.newPOQReceipt === false) return true
  // err = Utils.validateTypes(receipt.appliedReceipt.appliedVote.sign, {
  //   owner: 's',
  //   sig: 's',
  // })
  // if (err) {
  //   Logger.mainLogger.error('Invalid receipt appliedReceipt appliedVote signature data', err)
  //   return false
  // }
  // err = Utils.validateTypes(receipt.appliedReceipt.confirmOrChallenge, {
  //   message: 's',
  //   nodeId: 's',
  //   appliedVote: 'o',
  //   sign: 'o',
  // })
  // if (err) {
  //   Logger.mainLogger.error('Invalid receipt appliedReceipt confirmOrChallenge data', err)
  //   return false
  // }
  // err = Utils.validateTypes(receipt.appliedReceipt.confirmOrChallenge.sign, {
  //   owner: 's',
  //   sig: 's',
  // })
  // if (err) {
  //   Logger.mainLogger.error('Invalid receipt appliedReceipt confirmOrChallenge signature data', err)
  //   return false
  // }
  return true
}

export const verifyReceiptData = async (
  receipt: Receipt.ArchiverReceipt,
  checkReceiptRobust = true
): Promise<{ success: boolean; requiredSignatures?: number; newReceipt?: Receipt.ArchiverReceipt }> => {
  const result = { success: false }
  // Check the signed nodes are part of the execution group nodes of the tx
  const { executionShardKey, cycle, signedReceipt, globalModification } = receipt
  if (globalModification && config.skipGlobalTxReceiptVerification) return { success: true }
  const { signaturePack } = signedReceipt
  const { txId, timestamp } = receipt.tx
  if (config.VERBOSE) {
    const currentTimestamp = Date.now()
    // Console log the timetaken between the receipt timestamp and the current time ( both in ms and s)
    console.log(
      `Time taken between receipt timestamp and current time: ${txId}`,
      `${currentTimestamp - timestamp} ms`,
      `${(currentTimestamp - timestamp) / 1000} s`
    )
  }
  const currentCycle = getCurrentCycleCounter()
  if (currentCycle - cycle > 2) {
    Logger.mainLogger.error(
      `Found receipt with cycle older than 2 cycles ${txId}, ${cycle}, ${timestamp}, ${currentCycle}`
    )
  }
  const cycleShardData = shardValuesByCycle.get(cycle)
  if (!cycleShardData) {
    Logger.mainLogger.error('Cycle shard data not found')
    if (nestedCountersInstance) nestedCountersInstance.countEvent('receipt', 'Cycle_shard_data_not_found')
    return result
  }
  // Determine the home partition index of the primary account (executionShardKey)
  const { homePartition } = ShardFunction.addressToPartition(cycleShardData.shardGlobals, executionShardKey)
  if (config.newPOQReceipt === false) {
    // Refer to https://github.com/shardeum/shardus-core/blob/f7000c36faa0cd1e0832aa1e5e3b1414d32dcf66/src/state-manager/TransactionConsensus.ts#L1406
    let votingGroupCount = cycleShardData.shardGlobals.nodesPerConsenusGroup
    if (votingGroupCount > cycleShardData.nodes.length) {
      votingGroupCount = cycleShardData.nodes.length
    }
    const requiredSignatures =
      config.usePOQo === true
        ? Math.ceil(votingGroupCount * config.requiredVotesPercentage)
        : Math.round(votingGroupCount * config.requiredVotesPercentage)
    if (signaturePack.length < requiredSignatures) {
      Logger.mainLogger.error(
        `Invalid receipt appliedReceipt signatures count is less than requiredSignatures, ${signaturePack.length}, ${requiredSignatures}`
      )
      if (nestedCountersInstance)
        nestedCountersInstance.countEvent(
          'receipt',
          'Invalid_receipt_appliedReceipt_signatures_count_less_than_requiredSignatures'
        )
      return result
    }
    // Using a set to store the unique signatures to avoid duplicates
    const uniqueSigners = new Set()
    for (const signature of signaturePack) {
      const { owner: nodePubKey } = signature
      // Get the node id from the public key
      const node = cycleShardData.nodes.find((node) => node.publicKey === nodePubKey)
      if (node == null) {
        Logger.mainLogger.error(
          `The node with public key ${nodePubKey} of the receipt ${txId}} with ${timestamp} is not in the active nodesList of cycle ${cycle}`
        )
        if (nestedCountersInstance)
          nestedCountersInstance.countEvent(
            'receipt',
            'appliedReceipt_signature_owner_not_in_active_nodesList'
          )
        continue
      }
      // Check if the node is in the execution group
      if (!cycleShardData.parititionShardDataMap.get(homePartition).coveredBy[node.id]) {
        Logger.mainLogger.error(
          `The node with public key ${nodePubKey} of the receipt ${txId} with ${timestamp} is not in the execution group of the tx`
        )
        if (nestedCountersInstance)
          nestedCountersInstance.countEvent(
            'receipt',
            'appliedReceipt_signature_node_not_in_execution_group_of_tx'
          )
        continue
      }
      uniqueSigners.add(nodePubKey)
    }
    if (uniqueSigners.size < requiredSignatures) {
      Logger.mainLogger.error(
        `Invalid receipt appliedReceipt valid signatures count is less than requiredSignatures ${uniqueSigners.size}, ${requiredSignatures}`
      )
      if (nestedCountersInstance)
        nestedCountersInstance.countEvent(
          'receipt',
          'Invalid_receipt_appliedReceipt_valid_signatures_count_less_than_requiredSignatures'
        )
      return result
    }
    return { success: true, requiredSignatures }
  }
  // const { confirmOrChallenge } = appliedReceipt
  // // Check if the appliedVote node is in the execution group
  // if (!cycleShardData.nodeShardDataMap.has(appliedVote.node_id)) {
  //   Logger.mainLogger.error('Invalid receipt appliedReceipt appliedVote node is not in the active nodesList')
  //   if (nestedCountersInstance)
  //     nestedCountersInstance.countEvent('receipt', 'Invalid_receipt_appliedVote_node_not_in_active_nodesList')
  //   return result
  // }
  // if (appliedVote.sign.owner !== cycleShardData.nodeShardDataMap.get(appliedVote.node_id).node.publicKey) {
  //   Logger.mainLogger.error(
  //     'Invalid receipt appliedReceipt appliedVote node signature owner and node public key does not match'
  //   )
  //   if (nestedCountersInstance)
  //     nestedCountersInstance.countEvent(
  //       'receipt',
  //       'Invalid_receipt_appliedVote_node_signature_owner_and_node_public_key_does_not_match'
  //     )
  //   return result
  // }
  // if (!cycleShardData.parititionShardDataMap.get(homePartition).coveredBy[appliedVote.node_id]) {
  //   Logger.mainLogger.error(
  //     'Invalid receipt appliedReceipt appliedVote node is not in the execution group of the tx'
  //   )
  //   if (nestedCountersInstance)
  //     nestedCountersInstance.countEvent(
  //       'receipt',
  //       'Invalid_receipt_appliedVote_node_not_in_execution_group_of_tx'
  //     )
  //   return result
  // }
  // if (!Crypto.verify(appliedVote)) {
  //   Logger.mainLogger.error('Invalid receipt appliedReceipt appliedVote signature verification failed')
  //   if (nestedCountersInstance)
  //     nestedCountersInstance.countEvent(
  //       'receipt',
  //       'Invalid_receipt_appliedVote_signature_verification_failed'
  //     )
  //   return result
  // }

  // // Check if the confirmOrChallenge node is in the execution group
  // if (!cycleShardData.nodeShardDataMap.has(confirmOrChallenge.nodeId)) {
  //   Logger.mainLogger.error(
  //     'Invalid receipt appliedReceipt confirmOrChallenge node is not in the active nodesList'
  //   )
  //   if (nestedCountersInstance)
  //     nestedCountersInstance.countEvent(
  //       'receipt',
  //       'Invalid_receipt_confirmOrChallenge_node_not_in_active_nodesList'
  //     )
  //   return result
  // }
  // if (
  //   confirmOrChallenge.sign.owner !==
  //   cycleShardData.nodeShardDataMap.get(confirmOrChallenge.nodeId).node.publicKey
  // ) {
  //   Logger.mainLogger.error(
  //     'Invalid receipt appliedReceipt confirmOrChallenge node signature owner and node public key does not match'
  //   )
  //   if (nestedCountersInstance)
  //     nestedCountersInstance.countEvent(
  //       'receipt',
  //       'Invalid_receipt_confirmOrChallenge_signature_owner_and_node_public_key_does_not_match'
  //     )
  //   return result
  // }
  // if (!cycleShardData.parititionShardDataMap.get(homePartition).coveredBy[confirmOrChallenge.nodeId]) {
  //   Logger.mainLogger.error(
  //     'Invalid receipt appliedReceipt confirmOrChallenge node is not in the execution group of the tx'
  //   )
  //   if (nestedCountersInstance)
  //     nestedCountersInstance.countEvent(
  //       'receipt',
  //       'Invalid_receipt_confirmOrChallenge_node_not_in_execution_group_of_tx'
  //     )
  //   return result
  // }
  // if (!Crypto.verify(confirmOrChallenge)) {
  //   Logger.mainLogger.error('Invalid receipt appliedReceipt confirmOrChallenge signature verification failed')
  //   if (nestedCountersInstance)
  //     nestedCountersInstance.countEvent(
  //       'receipt',
  //       'Invalid_receipt_confirmOrChallenge_signature_verification_failed'
  //     )
  //   return result
  // }

  if (!checkReceiptRobust) return { success: true }
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
  if (newReceipt) return { success: true, requiredSignatures: 0, newReceipt }
  return { success: true }
}

const verifyAppliedReceiptSignatures = (
  receipt: Receipt.ArchiverReceipt,
  requiredSignatures: number,
  failedReasons = [],
  nestedCounterMessages = []
): { success: boolean } => {
  const result = { success: false, failedReasons, nestedCounterMessages }
  const { signedReceipt, globalModification } = receipt
  if (globalModification && config.skipGlobalTxReceiptVerification) return { success: true }
  const { proposal, signaturePack, voteOffsets } = signedReceipt
  const { txId: txid } = receipt.tx
  // Refer to https://github.com/shardeum/shardus-core/blob/50b6d00f53a35996cd69210ea817bee068a893d6/src/state-manager/TransactionConsensus.ts#L2799
  const voteHash = calculateVoteHash(proposal, failedReasons, nestedCounterMessages)
  // Refer to https://github.com/shardeum/shardus-core/blob/50b6d00f53a35996cd69210ea817bee068a893d6/src/state-manager/TransactionConsensus.ts#L2663
  const appliedVoteHash = {
    txid,
    voteHash,
  }
  // Using a map to store the good signatures to avoid duplicates
  const goodSignatures = new Map()
  for (const [index, signature] of signaturePack.entries()) {
    if (Crypto.verify({ ...appliedVoteHash, sign: signature, voteTime: voteOffsets.at(index) })) {
      goodSignatures.set(signature.owner, signature)
      // Break the loop if the required number of good signatures are found
      if (goodSignatures.size >= requiredSignatures) break
    } else {
      failedReasons.push(
        `Found invalid signature in receipt signedReceipt ${txid}, ${signature.owner}, ${index}`
      )
      nestedCounterMessages.push('Found_invalid_signature_in_receipt_signedReceipt')
    }
  }
  if (goodSignatures.size < requiredSignatures) {
    failedReasons.push(
      `Invalid receipt signedReceipt valid signatures count is less than requiredSignatures ${goodSignatures.size}, ${requiredSignatures}`
    )
    nestedCounterMessages.push(
      'Invalid_receipt_signedReceipt_valid_signatures_count_less_than_requiredSignatures'
    )
    return result
  }
  return { success: true }
}

const calculateVoteHash = (
  vote: Receipt.AppliedVote | Receipt.Proposal,
  failedReasons = [],
  nestedCounterMessages = []
): string => {
  try {
    if (config.usePOQo === true && (vote as Receipt.Proposal).applied !== undefined) {
      const proposal = vote as Receipt.Proposal
      const applyStatus = {
        applied: proposal.applied,
        cantApply: proposal.cant_preApply,
      }
      const accountsHash = Crypto.hash(
        Crypto.hashObj(proposal.accountIDs) +
          Crypto.hashObj(proposal.beforeStateHashes) +
          Crypto.hashObj(proposal.afterStateHashes)
      )
      const proposalHash = Crypto.hash(
        Crypto.hashObj(applyStatus) + accountsHash + proposal.appReceiptDataHash
      )
      return proposalHash
    } else if (config.usePOQo === true) {
      const appliedVote = vote as Receipt.AppliedVote
      const appliedHash = {
        applied: appliedVote.transaction_result,
        cantApply: appliedVote.cant_apply,
      }
      const stateHash = {
        account_id: appliedVote.account_id,
        account_state_hash_after: appliedVote.account_state_hash_after,
        account_state_hash_before: appliedVote.account_state_hash_before,
      }
      const appDataHash = {
        app_data_hash: appliedVote.app_data_hash,
      }
      const voteToHash = {
        appliedHash: Crypto.hashObj(appliedHash),
        stateHash: Crypto.hashObj(stateHash),
        appDataHash: Crypto.hashObj(appDataHash),
      }
      return Crypto.hashObj(voteToHash)
    }
    return Crypto.hashObj({ ...vote, node_id: '' })
  } catch {
    failedReasons.push('Error in calculateVoteHash', vote)
    nestedCounterMessages.push('Error_in_calculateVoteHash')
    return ''
  }
}

export const verifyArchiverReceipt = async (
  receipt: Receipt.ArchiverReceipt,
  requiredSignatures: number
): Promise<ReceiptVerificationResult> => {
  const { txId, timestamp } = receipt.tx
  const existingReceipt = await Receipt.queryReceiptByReceiptId(txId)
  const failedReasons = []
  const nestedCounterMessages = []
  if (config.verifyAppReceiptData) {
    // if (profilerInstance) profilerInstance.profileSectionStart('Verify_app_receipt_data')
    // if (nestedCountersInstance) nestedCountersInstance.countEvent('receipt', 'Verify_app_receipt_data')
    const { valid, needToSave } = await verifyAppReceiptData(
      receipt,
      existingReceipt,
      failedReasons,
      nestedCounterMessages
    )
    // if (profilerInstance) profilerInstance.profileSectionEnd('Verify_app_receipt_data')
    if (!valid) {
      failedReasons.push(
        `Invalid receipt: App Receipt Verification failed ${txId}, ${receipt.cycle}, ${timestamp}`
      )
      nestedCounterMessages.push('Invalid_receipt_app_receipt_verification_failed')
      return { success: false, failedReasons, nestedCounterMessages }
    }
    if (!needToSave) {
      failedReasons.push(`Valid receipt: but no need to save ${txId}, ${receipt.cycle}, ${timestamp}`)
      nestedCounterMessages.push('Valid_receipt_but_no_need_to_save')
      return { success: false, failedReasons, nestedCounterMessages }
    }
  }
  if (config.verifyAccountData) {
    // if (profilerInstance) profilerInstance.profileSectionStart('Verify_receipt_account_data')
    // if (nestedCountersInstance) nestedCountersInstance.countEvent('receipt', 'Verify_receipt_account_data')
    const result = verifyAccountHash(receipt, failedReasons, nestedCounterMessages)
    // if (profilerInstance) profilerInstance.profileSectionEnd('Verify_receipt_account_data')
    if (!result) {
      failedReasons.push(
        `Invalid receipt: Account Verification failed ${txId}, ${receipt.cycle}, ${timestamp}`
      )
      nestedCounterMessages.push('Invalid_receipt_account_verification_failed')
      return { success: false, failedReasons, nestedCounterMessages }
    }
  }
  if (config.verifyReceiptSignaturesSeparately) {
    // if (profilerInstance) profilerInstance.profileSectionStart('Verify_receipt_signatures_data')
    // if (nestedCountersInstance) nestedCountersInstance.countEvent('receipt', 'Verify_receipt_signatures_data')
    const { success } = verifyAppliedReceiptSignatures(
      receipt,
      requiredSignatures,
      failedReasons,
      nestedCounterMessages
    )
    // if (profilerInstance) profilerInstance.profileSectionEnd('Verify_receipt_signatures_data')
    if (!success) {
      failedReasons.push(`Invalid receipt: Verification failed ${txId}, ${receipt.cycle}, ${timestamp}`)
      nestedCounterMessages.push('Invalid_receipt_verification_failed')
      return { success: false, failedReasons, nestedCounterMessages }
    }
  }
  return { success: true, failedReasons, nestedCounterMessages }
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
  let combineProcessedTxs = []
  let txDataList: TxData[] = []
  if (saveOnlyGossipData) return
  for (let receipt of receipts) {
    if (receipt.globalModification) {
      console.log('[debug-log] Received storeReceiptData: ', StringUtils.safeStringify(receipt))
    }
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
    if (profilerInstance) profilerInstance.profileSectionStart('Validate_receipt')
    if (nestedCountersInstance) nestedCountersInstance.countEvent('receipt', 'Validate_receipt')
    if (!validateArchiverReceipt(receipt)) {
      Logger.mainLogger.error('Invalid receipt: Validation failed', txId, receipt.cycle, timestamp)
      receiptsInValidationMap.delete(txId)
      if (nestedCountersInstance)
        nestedCountersInstance.countEvent('receipt', 'Invalid_receipt_validation_failed')
      if (profilerInstance) profilerInstance.profileSectionEnd('Validate_receipt')
      continue
    }

    if (verifyData) {
      // if (config.usePOQo === false) {
      // const existingReceipt = await Receipt.queryReceiptByReceiptId(txId)
      // if (
      //   existingReceipt &&
      //   receipt.appliedReceipt &&
      //   receipt.appliedReceipt.confirmOrChallenge &&
      //   receipt.appliedReceipt.confirmOrChallenge.message === 'challenge'
      // ) {
      //   // If the existing receipt is confirmed, and the new receipt is challenged, then skip saving the new receipt
      //   if (existingReceipt.appliedReceipt.confirmOrChallenge.message === 'confirm') {
      //     Logger.mainLogger.error(
      //       `Existing receipt is confirmed, but new receipt is challenged ${txId}, ${receipt.cycle}, ${timestamp}`
      //     )
      //     receiptsInValidationMap.delete(txId)
      //     if (nestedCountersInstance)
      //       nestedCountersInstance.countEvent(
      //         'receipt',
      //         'Existing_receipt_is_confirmed_but_new_receipt_is_challenged'
      //       )
      //     if (profilerInstance) profilerInstance.profileSectionEnd('Validate_receipt')
      //     continue
      //   }
      // }
      // }

      if (config.verifyReceiptData) {
        const { success, requiredSignatures, newReceipt } = await verifyReceiptData(receipt)
        if (!success) {
          Logger.mainLogger.error('Invalid receipt: Verification failed', txId, receipt.cycle, timestamp)
          receiptsInValidationMap.delete(txId)
          if (nestedCountersInstance)
            nestedCountersInstance.countEvent('receipt', 'Invalid_receipt_verification_failed')
          if (profilerInstance) profilerInstance.profileSectionEnd('Validate_receipt')
          continue
        }
        if (newReceipt) receipt = newReceipt

        if (profilerInstance) profilerInstance.profileSectionStart('Offload_receipt')
        if (nestedCountersInstance) nestedCountersInstance.countEvent('receipt', 'Offload_receipt')
        const start_time = process.hrtime()
        // console.log('offloading receipt', txId, timestamp)
        const result = await offloadReceipt(txId, timestamp, requiredSignatures, receipt)
        // console.log('offload receipt result', txId, timestamp, result)
        const end_time = process.hrtime(start_time)
        const time_taken = end_time[0] * 1000 + end_time[1] / 1000000
        if (time_taken > 100) {
          console.log(`Time taken for receipt verification in millisecond is: `, txId, timestamp, time_taken)
        }
        if (profilerInstance) profilerInstance.profileSectionEnd('Offload_receipt')
        for (const message of result.failedReasons) {
          Logger.mainLogger.error(message)
        }
        for (const message of result.nestedCounterMessages) {
          if (nestedCountersInstance) nestedCountersInstance.countEvent('receipt', message)
        }
        if (result.success === false) {
          receiptsInValidationMap.delete(txId)
          if (profilerInstance) profilerInstance.profileSectionEnd('Validate_receipt')
          continue
        }
      }
    }
    if (profilerInstance) profilerInstance.profileSectionEnd('Validate_receipt')
    // await Receipt.insertReceipt({
    //   ...receipts[i],
    //   receiptId: tx.txId,
    //   timestamp: tx.timestamp,
    // })
    const { afterStates, cycle, tx, appReceiptData, signedReceipt } = receipt
    receipt.beforeStates = config.storeReceiptBeforeStates ? receipt.beforeStates : []

    const sortedVoteOffsets = (signedReceipt.voteOffsets ?? []).sort()
    const medianOffset = sortedVoteOffsets[Math.floor(sortedVoteOffsets.length / 2)] ?? 0
    const applyTimestamp = tx.timestamp + medianOffset * 1000
    if (config.VERBOSE) console.log('RECEIPT', 'Save', txId, timestamp, senderInfo)
    processedReceiptsMap.set(tx.txId, tx.timestamp)
    receiptsInValidationMap.delete(tx.txId)
    if (missingReceiptsMap.has(tx.txId)) missingReceiptsMap.delete(tx.txId)
    combineReceipts.push({
      ...receipt,
      receiptId: tx.txId,
      timestamp: tx.timestamp,
      applyTimestamp,
    })
    if (config.dataLogWrite && ReceiptLogWriter)
      ReceiptLogWriter.writeToLog(
        `${StringUtils.safeStringify({
          ...receipt,
          receiptId: tx.txId,
          timestamp: tx.timestamp,
          applyTimestamp,
        })}\n`
      )
    txDataList.push({ txId, timestamp })
    // If the receipt is a challenge, then skip updating its accounts data or transaction data
    // if (
    //   config.newPOQReceipt === true &&
    //   appliedReceipt &&
    //   appliedReceipt.confirmOrChallenge &&
    //   appliedReceipt.confirmOrChallenge.message === 'challenge'
    // )
    //   continue
    for (const account of afterStates) {
      const accObj: Account.AccountsCopy = {
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
    //   const accObj: Account.AccountsCopy = {
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

    const processedTx: ProcessedTransaction.ProcessedTransaction = {
      txId: tx.txId,
      cycle: cycle,
      txTimestamp: tx.timestamp,
      applyTimestamp,
    }

    // await Transaction.insertTransaction(txObj)
    combineTransactions.push(txObj)
    combineProcessedTxs.push(processedTx)
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
    if (combineProcessedTxs.length >= bucketSize) {
      await ProcessedTransaction.bulkInsertProcessedTxs(combineProcessedTxs)
      combineProcessedTxs = []
    }
  }
  // Receipts size can be big, better to save per 100
  if (combineReceipts.length > 0) {
    await Receipt.bulkInsertReceipts(combineReceipts)
    if (State.isActive) sendDataToAdjacentArchivers(DataType.RECEIPT, txDataList)
  }
  if (combineAccounts.length > 0) await Account.bulkInsertAccounts(combineAccounts)
  if (combineTransactions.length > 0) await Transaction.bulkInsertTransactions(combineTransactions)
  if (combineProcessedTxs.length > 0) await ProcessedTransaction.bulkInsertProcessedTxs(combineProcessedTxs)
  // If the archiver is not active, good to clean up the processed receipts map if it exceeds 2000
  if (!State.isActive && processedReceiptsMap.size > 2000) processedReceiptsMap.clear()
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
    if (config.dataLogWrite && CycleLogWriter)
      CycleLogWriter.writeToLog(`${StringUtils.safeStringify(cycleObj)}\n`)
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
  accounts?: Account.AccountsCopy[]
  receipts?: Transaction.Transaction[]
}

export const storeAccountData = async (restoreData: StoreAccountParam = {}): Promise<void> => {
  Logger.mainLogger.debug(
    `storeAccountData: ${restoreData.accounts ? restoreData.accounts.length : 0} accounts`
  )
  Logger.mainLogger.debug(
    `storeAccountData: ${restoreData.receipts ? restoreData.receipts.length : 0} receipts`
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
  //
  if (accounts && accounts.length > 0) {
    const combineAccounts = []
    for (const account of accounts) {
      try {
        const calculatedAccountHash = accountSpecificHash(account.data)
        if (calculatedAccountHash !== account.hash) {
          Logger.mainLogger.error(
            'Invalid account hash',
            account.accountId,
            account.hash,
            calculatedAccountHash
          )
          continue
        }
        combineAccounts.push(account)
      } catch (error) {
        Logger.mainLogger.error('Error in calculating genesis account hash', error)
      }
    }
    if (combineAccounts.length > 0) await Account.bulkInsertAccounts(accounts)
  }
  if (receipts && receipts.length > 0) {
    Logger.mainLogger.debug('Received receipts Size', receipts.length)
    const combineTransactions = []
    const combineProcessedTxs = []
    for (const receipt of receipts) {
      const txObj: Transaction.Transaction = {
        txId: receipt.data.txId || receipt.txId,
        appReceiptId: receipt.appReceiptId,
        timestamp: receipt.timestamp,
        cycleNumber: receipt.cycleNumber,
        data: receipt.data,
        originalTxData: {},
      }
      const processedTx: ProcessedTransaction.ProcessedTransaction = {
        txId: receipt.data.txId || receipt.txId,
        cycle: receipt.cycleNumber,
        txTimestamp: receipt.timestamp,
        applyTimestamp: receipt.timestamp,
      }
      combineTransactions.push(txObj)
      combineProcessedTxs.push(processedTx)
    }
    await Transaction.bulkInsertTransactions(combineTransactions)
    await ProcessedTransaction.bulkInsertProcessedTxs(combineProcessedTxs)
  }
  if (profilerInstance) profilerInstance.profileSectionEnd('store_account_data')
  Logger.mainLogger.debug('Combined Accounts Data', combineAccountsData.accounts.length)
  if (combineAccountsData.accounts.length > 0 || combineAccountsData.receipts.length > 0) {
    Logger.mainLogger.debug('Found combine accountsData', combineAccountsData.accounts.length)
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
  // If the archiver is not active yet, good to clean up the processed originalTxs map if it exceeds 2000
  if (!State.isActive && processedOriginalTxsMap.size > 2000) processedOriginalTxsMap.clear()
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
  if (!State.activeArchivers.some((archiver) => archiver.publicKey === data.sign.owner)) {
    Logger.mainLogger.error('Data sender is not the active archivers')
    return { success: false, error: 'Data sender not the active archivers' }
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
  const { dataType, data, sign } = gossipdata
  const senderArchiver = State.activeArchivers.find((archiver) => archiver.publicKey === sign.owner)
  const receivedTimestamp = Date.now()
  if (dataType === DataType.RECEIPT) {
    for (const { txId, timestamp } of data as TxData[]) {
      if (
        (processedReceiptsMap.has(txId) && processedReceiptsMap.get(txId) === timestamp) ||
        (receiptsInValidationMap.has(txId) && receiptsInValidationMap.get(txId) === timestamp) ||
        (collectingMissingReceiptsMap.has(txId) && collectingMissingReceiptsMap.get(txId) === timestamp)
      ) {
        // console.log('GOSSIP', 'RECEIPT', 'SKIP', txId, 'sender', sign.owner)
        continue
      } else {
        if (missingReceiptsMap.has(txId)) {
          if (
            missingReceiptsMap.get(txId).txTimestamp === timestamp &&
            !missingReceiptsMap.get(txId).senders.some((sender) => sender === sign.owner)
          )
            missingReceiptsMap.get(txId).senders.push(sign.owner)
          else {
            // Not expected to happen, but log error if it happens <-- could be malicious act of the sender
            if (missingReceiptsMap.get(txId).txTimestamp !== timestamp)
              Logger.mainLogger.error(
                `Received gossip for receipt ${txId} with different timestamp ${timestamp} from archiver ${sign.owner}`
              )
            if (missingReceiptsMap.get(txId).senders.some((sender) => sender === sign.owner))
              Logger.mainLogger.error(
                `Received gossip for receipt ${txId} from the same sender ${sign.owner}`
              )
          }
        } else
          missingReceiptsMap.set(txId, { txTimestamp: timestamp, receivedTimestamp, senders: [sign.owner] })
        // console.log('GOSSIP', 'RECEIPT', 'MISS', txId, 'sender', sign.owner)
      }
    }
  }
  if (dataType === DataType.ORIGINAL_TX_DATA) {
    for (const { txId, timestamp } of data as TxData[]) {
      if (
        (processedOriginalTxsMap.has(txId) && processedOriginalTxsMap.get(txId) === timestamp) ||
        (originalTxsInValidationMap.has(txId) && originalTxsInValidationMap.get(txId) === timestamp) ||
        (collectingMissingOriginalTxsMap.has(txId) && collectingMissingOriginalTxsMap.get(txId) === timestamp)
      ) {
        // console.log('GOSSIP', 'ORIGINAL_TX_DATA', 'SKIP', txId, 'sender', sign.owner)
        continue
      } else {
        if (missingOriginalTxsMap.has(txId)) {
          if (
            missingOriginalTxsMap.get(txId).txTimestamp === timestamp &&
            !missingOriginalTxsMap.get(txId).senders.some((sender) => sender === sign.owner)
          )
            missingOriginalTxsMap.get(txId).senders.push(sign.owner)
          else {
            // Not expected to happen, but log error if it happens <-- could be malicious act of the sender
            if (missingOriginalTxsMap.get(txId).txTimestamp !== timestamp)
              Logger.mainLogger.error(
                `Received gossip for originalTxData ${txId} with different timestamp ${timestamp} from archiver ${sign.owner}`
              )
            if (missingOriginalTxsMap.get(txId).senders.some((sender) => sender === sign.owner))
              Logger.mainLogger.error(
                `Received gossip for originalTxData ${txId} from the same sender ${sign.owner}`
              )
          }
        } else
          missingOriginalTxsMap.set(txId, {
            txTimestamp: timestamp,
            receivedTimestamp,
            senders: [sign.owner],
          })
        // console.log('GOSSIP', 'ORIGINAL_TX_DATA', 'MISS', txId, 'sender', sign.owner)
      }
    }
  }
  if (dataType === DataType.CYCLE) {
    collectCycleData(
      data as P2PTypes.CycleCreatorTypes.CycleData[],
      senderArchiver?.ip + ':' + senderArchiver?.port
    )
  }
}

export const collectMissingTxDataFromArchivers = async (): Promise<void> => {
  const currentTimestamp = Date.now()
  if (missingReceiptsMap.size > 0) {
    const cloneMissingReceiptsMap: Map<string, Omit<MissingTx, 'receivedTimestamp'>> = new Map()
    for (const [txId, { txTimestamp, receivedTimestamp, senders }] of missingReceiptsMap) {
      if (currentTimestamp - receivedTimestamp > config.waitingTimeForMissingTxData) {
        cloneMissingReceiptsMap.set(txId, { txTimestamp, senders })
        collectingMissingReceiptsMap.set(txId, txTimestamp)
        missingReceiptsMap.delete(txId)
      }
    }
    if (cloneMissingReceiptsMap.size > 0)
      Logger.mainLogger.debug('Collecting missing receipts', cloneMissingReceiptsMap.size)
    for (const [txId, { txTimestamp, senders }] of cloneMissingReceiptsMap) {
      collectMissingReceipts(senders, txId, txTimestamp)
    }
    cloneMissingReceiptsMap.clear()
  }
  if (missingOriginalTxsMap.size > 0) {
    const cloneMissingOriginalTxsMap: Map<string, Omit<MissingTx, 'receivedTimestamp'>> = new Map()
    for (const [txId, { txTimestamp, receivedTimestamp, senders }] of missingOriginalTxsMap) {
      if (currentTimestamp - receivedTimestamp > config.waitingTimeForMissingTxData) {
        cloneMissingOriginalTxsMap.set(txId, { txTimestamp, senders })
        collectingMissingOriginalTxsMap.set(txId, txTimestamp)
        missingOriginalTxsMap.delete(txId)
      }
    }
    if (cloneMissingOriginalTxsMap.size > 0)
      Logger.mainLogger.debug('Collecting missing originalTxsData', cloneMissingOriginalTxsMap.size)
    for (const [txId, { txTimestamp, senders }] of cloneMissingOriginalTxsMap) {
      collectMissingOriginalTxsData(senders, txId, txTimestamp)
    }
    cloneMissingOriginalTxsMap.clear()
  }
}

export const collectMissingReceipts = async (
  senders: string[],
  txId: string,
  txTimestamp: number
): Promise<void> => {
  const txIdList: [string, number][] = [[txId, txTimestamp]]
  let foundTxData = false
  const senderArchivers = State.activeArchivers.filter((archiver) => senders.includes(archiver.publicKey))
  Logger.mainLogger.debug(
    `Collecting missing receipt for txId ${txId} with timestamp ${txTimestamp} from archivers`,
    senderArchivers.map((a) => a.ip + ':' + a.port)
  )
  if (nestedCountersInstance) nestedCountersInstance.countEvent('receipt', 'Collect_missing_receipt')
  if (profilerInstance) profilerInstance.profileSectionStart('Collect_missing_receipt')
  for (const senderArchiver of senderArchivers) {
    if (
      (processedReceiptsMap.has(txId) && processedReceiptsMap.get(txId) === txTimestamp) ||
      (receiptsInValidationMap.has(txId) && receiptsInValidationMap.get(txId) === txTimestamp)
    ) {
      foundTxData = true
      break
    }
    const receipts = (await queryTxDataFromArchivers(
      senderArchiver,
      DataType.RECEIPT,
      txIdList
    )) as Receipt.Receipt[]
    if (receipts && receipts.length > 0) {
      for (const receipt of receipts) {
        const { receiptId, timestamp } = receipt
        if (txId === receiptId && txTimestamp === timestamp) {
          storeReceiptData([receipt], senderArchiver.ip + ':' + senderArchiver.port, true)
          foundTxData = true
        }
      }
    }
    if (foundTxData) break
  }
  if (!foundTxData) {
    if (nestedCountersInstance)
      nestedCountersInstance.countEvent('receipt', 'Failed to collect missing receipt from archivers')
    Logger.mainLogger.error(
      `Failed to collect receipt for txId ${txId} with timestamp ${txTimestamp} from archivers ${senders}`
    )
  }
  collectingMissingReceiptsMap.delete(txId)
  if (profilerInstance) profilerInstance.profileSectionEnd('Collect_missing_receipt')
}

const collectMissingOriginalTxsData = async (
  senders: string[],
  txId: string,
  txTimestamp: number
): Promise<void> => {
  const txIdList: [string, number][] = [[txId, txTimestamp]]
  let foundTxData = false
  const senderArchivers = State.activeArchivers.filter((archiver) => senders.includes(archiver.publicKey))
  Logger.mainLogger.debug(
    `Collecting missing originalTxData for txId ${txId} with timestamp ${txTimestamp} from archivers`,
    senderArchivers.map((a) => a.ip + ':' + a.port)
  )
  if (nestedCountersInstance)
    nestedCountersInstance.countEvent('originalTxData', 'Collect_missing_originalTxData')
  if (profilerInstance) profilerInstance.profileSectionStart('Collect_missing_originalTxData')
  for (const senderArchiver of senderArchivers) {
    if (
      (processedOriginalTxsMap.has(txId) && processedOriginalTxsMap.get(txId) === txTimestamp) ||
      (originalTxsInValidationMap.has(txId) && originalTxsInValidationMap.get(txId) === txTimestamp)
    ) {
      foundTxData = true
      break
    }
    const originalTxs = (await queryTxDataFromArchivers(
      senderArchiver,
      DataType.ORIGINAL_TX_DATA,
      txIdList
    )) as OriginalTxDB.OriginalTxData[]
    if (originalTxs && originalTxs.length > 0) {
      for (const originalTx of originalTxs)
        if (txId === originalTx.txId && txTimestamp === originalTx.timestamp) {
          storeOriginalTxData([originalTx], senderArchiver.ip + ':' + senderArchiver.port)
          foundTxData = true
        }
    }
    if (foundTxData) break
  }
  if (!foundTxData) {
    if (nestedCountersInstance)
      nestedCountersInstance.countEvent('originalTxData', 'Failed to collect_missing_originalTxData')
    Logger.mainLogger.error(
      `Failed to collect originalTxData for txId ${txId} with timestamp ${txTimestamp} from archivers ${senders}`
    )
  }
  collectingMissingOriginalTxsMap.delete(txId)
  if (profilerInstance) profilerInstance.profileSectionEnd('Collect_missing_originalTxData')
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

export function cleanOldReceiptsMap(timestamp: number): void {
  let savedReceiptsCount = 0
  for (const [key, value] of processedReceiptsMap) {
    if (value < timestamp) {
      processedReceiptsMap.delete(key)
      savedReceiptsCount++
    }
  }
  if (savedReceiptsCount > 0)
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
  if (savedOriginalTxsCount > 0)
    Logger.mainLogger.debug(
      `Clean ${savedOriginalTxsCount} old originalTxsData from the processed originalTxsData cache on cycle ${getCurrentCycleCounter()}`
    )
}

export const scheduleMissingTxsDataQuery = (): void => {
  // Set to collect missing txs data in every 1 second
  setInterval(() => {
    collectMissingTxDataFromArchivers()
  }, 1000)
}
