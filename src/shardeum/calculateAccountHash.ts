import { config } from '../Config'
import * as crypto from '../Crypto'
import * as Logger from '../Logger'
import { ArchiverReceipt } from '../dbstore/receipts'

// account types in Shardeum
export enum AccountType {
  Account = 0, //  EOA or CA
  ContractStorage = 1, // Contract storage key value pair
  ContractCode = 2, // Contract code bytes
  Receipt = 3, //This holds logs for a TX
  Debug = 4,
  NetworkAccount = 5,
  NodeAccount = 6,
  NodeRewardReceipt = 7,
  DevAccount = 8,
  NodeAccount2 = 9,
  StakeReceipt = 10,
  UnstakeReceipt = 11,
  InternalTxReceipt = 12,
}

export const accountSpecificHash = (account: any): string => {
  let hash: string
  delete account.hash
  if (
    account.accountType === AccountType.NetworkAccount ||
    account.accountType === AccountType.NodeAccount ||
    account.accountType === AccountType.NodeAccount2 ||
    account.accountType === AccountType.NodeRewardReceipt ||
    account.accountType === AccountType.StakeReceipt ||
    account.accountType === AccountType.UnstakeReceipt ||
    account.accountType === AccountType.InternalTxReceipt ||
    account.accountType === AccountType.DevAccount
  ) {
    account.hash = crypto.hashObj(account)
    return account.hash
  }
  if (account.accountType === AccountType.Account) {
    const { account: EVMAccountInfo, operatorAccountInfo, timestamp } = account
    const accountData = operatorAccountInfo
      ? { EVMAccountInfo, operatorAccountInfo, timestamp }
      : { EVMAccountInfo, timestamp }
    hash = crypto.hashObj(accountData)
  } else if (account.accountType === AccountType.Debug) {
    hash = crypto.hashObj(account)
  } else if (account.accountType === AccountType.ContractStorage) {
    hash = crypto.hashObj({ key: account.key, value: account.value })
  } else if (account.accountType === AccountType.ContractCode) {
    hash = crypto.hashObj({ key: account.codeHash, value: account.codeByte })
  } else if (account.accountType === AccountType.Receipt) {
    hash = crypto.hashObj({ key: account.txId, value: account.receipt })
  }

  // hash = hash + '0'.repeat(64 - hash.length)
  account.hash = hash
  return hash
}

export const verifyAccountHash = (
  receipt: ArchiverReceipt,
  failedReasons = [],
  nestedCounterMessages = []
): boolean => {
  try {
    if (receipt.globalModification && config.skipGlobalTxReceiptVerification) return true // return true if global modification
    if (receipt.accounts.length !== receipt.appliedReceipt.appliedVote.account_state_hash_after.length) {
      failedReasons.push(
        `Modified account count specified in the receipt and the actual updated account count does not match! ${receipt.tx.txId} , ${receipt.cycle} , ${receipt.tx.timestamp}`
      )
      return false
    }
    if (
      receipt.appliedReceipt.appliedVote.account_state_hash_before.length !==
      receipt.appliedReceipt.appliedVote.account_state_hash_after.length
    ) {
      failedReasons.push(
        `Account state hash before and after count does not match! ${receipt.tx.txId} , ${receipt.cycle} , ${receipt.tx.timestamp}`
      )
      return false
    }
    for (const account of receipt.accounts) {
      const calculatedAccountHash = accountSpecificHash(account.data)
      const indexOfAccount = receipt.appliedReceipt.appliedVote.account_id.indexOf(account.accountId)
      if (indexOfAccount === -1) {
        failedReasons.push(
          `Account not found in the receipt ${account.accountId} , ${receipt.tx.txId} , ${receipt.cycle} , ${receipt.tx.timestamp}`
        )
        return false
      }
      // eslint-disable-next-line security/detect-object-injection
      const expectedAccountHash = receipt.appliedReceipt.appliedVote.account_state_hash_after[indexOfAccount]
      if (calculatedAccountHash !== expectedAccountHash) {
        failedReasons.push(
          `Account hash does not match ${account.accountId} , ${receipt.tx.txId} , ${receipt.cycle} , ${receipt.tx.timestamp}`
        )
        return false
      }
    }
    return true
  } catch (e) {
    failedReasons.push('Error in verifyAccountHash', e)
    return false
  }
}
