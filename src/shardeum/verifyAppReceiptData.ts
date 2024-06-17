import { config } from '../Config'
import * as crypto from '../Crypto'
import * as Logger from '../Logger'
import { ArchiverReceipt, Receipt } from '../dbstore/receipts'
import { Utils as StringUtils } from '@shardus/types'

export type ShardeumReceipt = object & {
  amountSpent: string
  readableReceipt: { status: number }
}

export const verifyAppReceiptData = async (
  receipt: ArchiverReceipt,
  existingReceipt?: Receipt | null
): Promise<{ valid: boolean; needToSave: boolean }> => {
  let result = { valid: false, needToSave: false }
  const { appReceiptData, globalModification, appliedReceipt } = receipt
  const newShardeumReceipt = appReceiptData.data as ShardeumReceipt
  if (!newShardeumReceipt.amountSpent || !newShardeumReceipt.readableReceipt) {
    Logger.mainLogger.error(`appReceiptData missing amountSpent or readableReceipt`)
    return result
  }
  if (
    newShardeumReceipt.amountSpent === '0x0' &&
    newShardeumReceipt.readableReceipt.status === 0 &&
    appliedReceipt.appliedVote.account_state_hash_after.length > 0
  ) {
    for (let i = 0; i < appliedReceipt.appliedVote.account_id.length; i++) {
      if (
        // eslint-disable-next-line security/detect-object-injection
        !appliedReceipt.appliedVote.account_state_hash_before[i] ||
        // eslint-disable-next-line security/detect-object-injection
        !appliedReceipt.appliedVote.account_state_hash_after[i]
      ) {
        Logger.mainLogger.error(
          `The account state hash before or after is missing in the receipt! ${receipt.tx.txId} , ${receipt.cycle} , ${receipt.tx.timestamp}`
        )
      }
      if (
        // eslint-disable-next-line security/detect-object-injection
        appliedReceipt.appliedVote.account_state_hash_before[i] !==
        // eslint-disable-next-line security/detect-object-injection
        appliedReceipt.appliedVote.account_state_hash_after[i]
      ) {
        Logger.mainLogger.error(
          `The receipt has 0 amountSpent and status 0 but has state updated accounts! ${receipt.tx.txId} , ${receipt.cycle} , ${receipt.tx.timestamp}`
        )
        break
      }
    }
  }
  result = { valid: true, needToSave: false }
  if (existingReceipt && existingReceipt.timestamp !== receipt.tx.timestamp) {
    // If the existing receipt is challenged and the new receipt is confirmed, overwrite the existing receipt
    let skipAppReceiptCheck = false
    if (
      config.newPOQReceipt === true &&
      existingReceipt.appliedReceipt &&
      existingReceipt.appliedReceipt.confirmOrChallenge &&
      receipt.appliedReceipt &&
      receipt.appliedReceipt.confirmOrChallenge &&
      existingReceipt.appliedReceipt.confirmOrChallenge.message === 'challenge' &&
      receipt.appliedReceipt.confirmOrChallenge.message === 'confirm'
    ) {
      result = { valid: true, needToSave: true }
      skipAppReceiptCheck = true
    }

    if (!skipAppReceiptCheck) {
      const existingShardeumReceipt = existingReceipt.appReceiptData.data as ShardeumReceipt
      /**
       * E: existing receipt, N: new receipt, X: any value
       * E: status = 0, N: status = 1, E: amountSpent = 0, N: amountSpent = X, needToSave = true
       * E: status = 0, N: status = 1, E: amountSpent > 0, N: amountSpent > 0, needToSave = false (success and failed receipts with gas charged)
       * E: status = 0, N: status = 0, E: amountSpent = 0, N: amountSpent = 0, needToSave = false
       * E: status = 0, N: status = 0, E: amountSpent = 0, N: amountSpent > 0, needToSave = true
       * E: status = 0, N: status = 0, E: amountSpent > 0, N: amountSpent = 0, needToSave = false
       * E: status = 0, N: status = 0, E: amountSpent > 0, N: amountSpent > 0, needToSave = false (both failed receipts with gas charged)
       * E: status = 1, N: status = 0, E: amountSpent = X, N: amountSpent = X, needToSave = false
       * E: status = 1, N: status = 1, E: amountSpent = X, N: amountSpent = X, needToSave = false (duplicate success receipt)
       *
       **/
      // Added only logging of unexpected cases and needToSave = true cases ( check `else` condition )
      if (existingShardeumReceipt.readableReceipt.status === 0) {
        if (newShardeumReceipt.readableReceipt.status === 1) {
          if (existingShardeumReceipt.amountSpent !== '0x0') {
            Logger.mainLogger.error(
              `Success and failed receipts with gas charged`,
              StringUtils.safeStringify(existingReceipt),
              StringUtils.safeStringify(receipt)
            )
          } else result = { valid: true, needToSave: true } // Success receipt
        } else {
          if (existingShardeumReceipt.amountSpent !== '0x0' && newShardeumReceipt.amountSpent !== '0x0') {
            Logger.mainLogger.error(
              `Both failed receipts with gas charged`,
              StringUtils.safeStringify(existingReceipt),
              StringUtils.safeStringify(receipt)
            )
          } else if (newShardeumReceipt.amountSpent !== '0x0') {
            // Failed receipt with gas charged
            result = { valid: true, needToSave: true }
          }
        }
      } else if (newShardeumReceipt.readableReceipt.status === 1) {
        Logger.mainLogger.error(
          `Duplicate success receipt`,
          StringUtils.safeStringify(existingReceipt),
          StringUtils.safeStringify(receipt)
        )
      }
    }
  } else result = { valid: true, needToSave: true }
  if (globalModification && config.skipGlobalTxReceiptVerification) return { valid: true, needToSave: true }
  // Finally verify appReceiptData hash
  const appReceiptDataCopy = { ...appReceiptData }
  const calculatedAppReceiptDataHash = calculateAppReceiptDataHash(appReceiptDataCopy)
  if (calculatedAppReceiptDataHash !== receipt.appliedReceipt.app_data_hash) {
    Logger.mainLogger.error(
      `appReceiptData hash mismatch: ${crypto.hashObj(appReceiptData)} != ${
        receipt.appliedReceipt.app_data_hash
      }`
    )
    result = { valid: false, needToSave: false }
  }
  return result
}

// Converting the correct appReceipt data format to get the correct hash
const calculateAppReceiptDataHash = (appReceiptData): string => {
  try {
    if (appReceiptData.data && appReceiptData.data.receipt) {
      if (appReceiptData.data.receipt.bitvector)
        appReceiptData.data.receipt.bitvector = Uint8Array.from(
          Object.values(appReceiptData.data.receipt.bitvector)
        )
      if (appReceiptData.data.receipt.logs && appReceiptData.data.receipt.logs.length > 0) {
        appReceiptData.data.receipt.logs = appReceiptData.data.receipt.logs.map((log) => {
          return log.map((log1) => {
            if (Array.isArray(log1)) {
              return log1.map((log2) => {
                log2 = Uint8Array.from(Object.values(log2))
                return log2
              })
            } else {
              log1 = Uint8Array.from(Object.values(log1))
              return log1
            }
          })
        })
      }
    }
    const hash = crypto.hashObj(appReceiptData)
    return hash
  } catch (err) {
    Logger.mainLogger.error(`calculateAppReceiptDataHash error: ${err}`)
    return ''
  }
}
