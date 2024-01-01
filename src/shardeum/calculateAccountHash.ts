import * as crypto from '../Crypto'
import * as Logger from '../Logger'
import { ArchiverReceipt } from '../dbstore/receipts'

// account types in Shardeum
enum AccountType {
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

const accountSpecificHash = (account: any): string => {
  let hash
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
    //Hash the full account, if we knew EOA vs CA we could mabe skip some steps.
    hash = crypto.hashObj(account.account)
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

// Converting the correct account data format to get the correct hash
const fixAccountUint8Arrays = (account: any): void => {
  if (account) return // if account is null, return
  if (account.storageRoot) account.storageRoot = Uint8Array.from(Object.values(account.storageRoot)) // Account
  if (account.codeHash) account.codeHash = Uint8Array.from(Object.values(account.codeHash)) // Account and ContractCode
  if (account.codeByte) account.codeByte = Uint8Array.from(Object.values(account.codeByte)) // ContractCode
  if (account.value) account.value = Uint8Array.from(Object.values(account.value)) // ContractByte
}

export const verifyAccountHash = (receipt: ArchiverReceipt): boolean => {
  for (const account of receipt.accounts) {
    if (account.data.accountType === AccountType.Account) {
      fixAccountUint8Arrays(account.data.account)
      // console.dir(acc, { depth: null })
    } else if (
      account.data.accountType === AccountType.ContractCode ||
      account.data.accountType === AccountType.ContractStorage
    ) {
      fixAccountUint8Arrays(account.data)
      // console.dir(acc, { depth: null })
    }
    const calculatedAccountHash = accountSpecificHash(account.data)
    const indexOfAccount = receipt.appliedReceipt.appliedVote.account_id.indexOf(account.accountId)
    if (indexOfAccount === -1) {
      Logger.mainLogger.error('Account not found', account.data.accountId)
      return false
    }
    const expectedAccountHash = receipt.appliedReceipt.appliedVote.account_state_hash_after[indexOfAccount]
    if (calculatedAccountHash !== expectedAccountHash) {
      Logger.mainLogger.error('Account hash does not match', account.data.accountId)
      return false // return false if any account hash does not match
    }
  }
  return true
}
