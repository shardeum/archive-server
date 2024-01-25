import * as crypto from '../Crypto'
import * as Logger from '../Logger'
import * as Receipt from '../dbstore/receipts'

import { ArchiverReceipt } from '../dbstore/receipts'

type ShardeumReceipt = object & {
  amountSpent: string
  readableReceipt: { status: number }
}

export const verifyAppReceiptData = async (
  receipt: ArchiverReceipt
): Promise<{ valid: boolean; needToSave: boolean }> => {
  let result = { valid: false, needToSave: false }
  const { appReceiptData, tx } = receipt
  const newShardeumReceipt = appReceiptData.data as ShardeumReceipt
  if (!newShardeumReceipt.amountSpent || !newShardeumReceipt.readableReceipt) {
    Logger.mainLogger.error(`appReceiptData missing amountSpent or readableReceipt`)
    return result
  }
  result = { valid: true, needToSave: false }
  const receiptExist = await Receipt.queryReceiptByReceiptId(tx.txId)
  if (receiptExist) {
    const existingShardeumReceipt = receiptExist.appReceiptData.data as ShardeumReceipt
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
        if (existingShardeumReceipt.amountSpent !== '0') {
          Logger.mainLogger.error(`Success and failed receipts with gas charged`, receiptExist, receipt)
        } else result = { valid: true, needToSave: true }
      } else {
        if (existingShardeumReceipt.amountSpent !== '0' && newShardeumReceipt.amountSpent !== '0') {
          Logger.mainLogger.error(`Both failed receipts with gas charged`, receiptExist, receipt)
        } else result = { valid: true, needToSave: true }
      }
    } else {
      if (newShardeumReceipt.readableReceipt.status === 1) {
        Logger.mainLogger.error(`Duplicate success receipt`, receiptExist, receipt)
      } else result = { valid: true, needToSave: true }
    }
  } else result = { valid: true, needToSave: true }
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

const calculateAppReceiptDataHash = (appReceiptData: any): string => {
  if (appReceiptData.data && appReceiptData.data.receipt && appReceiptData.data.receipt.bitvector)
    appReceiptData.data.receipt.bitvector = Uint8Array.from(
      Object.values(appReceiptData.data.receipt.bitvector)
    )
  const hash = crypto.hashObj(appReceiptData)
  return hash
}
