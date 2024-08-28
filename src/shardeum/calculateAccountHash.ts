import * as crypto from '../Crypto'
import { ArchiverReceipt, SignedReceipt } from '../dbstore/receipts'
import { verifyGlobalTxAccountChange } from './verifyGlobalTxReceipt'

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
    if (receipt.globalModification) {
      const result = verifyGlobalTxAccountChange(receipt, failedReasons, nestedCounterMessages)
      if (!result) return false
      return true
    }
    const signedReceipt = receipt.signedReceipt as SignedReceipt
    const { accountIDs, afterStateHashes, beforeStateHashes } = signedReceipt.proposal
    if (accountIDs.length !== afterStateHashes.length) {
      failedReasons.push(
        `Modified account count specified in the receipt and the actual updated account count does not match! ${receipt.tx.txId} , ${receipt.cycle} , ${receipt.tx.timestamp}`
      )
      nestedCounterMessages.push(
        `Modified account count specified in the receipt and the actual updated account count does not match!`
      )
      return false
    }
    if (beforeStateHashes.length !== afterStateHashes.length) {
      failedReasons.push(
        `Account state hash before and after count does not match! ${receipt.tx.txId} , ${receipt.cycle} , ${receipt.tx.timestamp}`
      )
      nestedCounterMessages.push(`Account state hash before and after count does not match!`)
      return false
    }
    for (const [index, accountId] of accountIDs.entries()) {
      const accountData = receipt.afterStates.find((acc) => acc.accountId === accountId)
      if (accountData === undefined) {
        failedReasons.push(
          `Account not found in the receipt's afterStates | Acc-ID: ${accountId}, txId: ${receipt.tx.txId}, Cycle: ${receipt.cycle}, timestamp: ${receipt.tx.timestamp}`
        )
        nestedCounterMessages.push(`Account not found in the receipt`)
        return false
      }
      const calculatedAccountHash = accountSpecificHash(accountData.data)
      // eslint-disable-next-line security/detect-object-injection
      const expectedAccountHash = afterStateHashes[index]
      if (calculatedAccountHash !== expectedAccountHash) {
        failedReasons.push(
          `Account hash does not match | Acc-ID: ${accountId}, txId: ${receipt.tx.txId}, Cycle: ${receipt.cycle}, timestamp: ${receipt.tx.timestamp}`
        )
        nestedCounterMessages.push(`Account hash does not match`)
        return false
      }
    }
    return true
  } catch (e) {
    console.error(`Error in verifyAccountHash`, e)
    failedReasons.push(`Error in verifyAccountHash ${e}`)
    nestedCounterMessages.push('Error in verifyAccountHash')
    return false
  }
}
