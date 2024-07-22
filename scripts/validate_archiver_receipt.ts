import { join } from 'path'
import { overrideDefaultConfig, config } from '../src/Config'
import * as Crypto from '../src/Crypto'
import * as Utils from '../src/Utils'
import * as Receipt from '../src/dbstore/receipts'
import { AccountType, accountSpecificHash } from '../src/shardeum/calculateAccountHash'
import { ShardeumReceipt } from '../src/shardeum/verifyAppReceiptData'

// Add the full receipt data here
const receipt: any = {}

const runProgram = async (): Promise<void> => {
  // Override default config params from config file, env vars, and cli args
  const file = join(process.cwd(), 'archiver-config.json')
  overrideDefaultConfig(file)
  // Set crypto hash keys from config
  const hashKey = config.ARCHIVER_HASH_KEY
  Crypto.setCryptoHashKey(hashKey)

  // validate appReceiptData
  let result = validateReceiptData(receipt)
  if (!result) {
    console.error('Invalid receipt data')
    return
  }

  // verifyAppReceiptData
  result = verifyAppReceiptData(receipt)
  if (!result) {
    console.error('Invalid app receipt data')
    return
  }

  // verifyAccountHash
  result = verifyAccountHash(receipt)
  if (!result) {
    console.error('Invalid accounts data')
    return
  }
}

// validating appReceiptData
const validateReceiptData = (receipt: Receipt.ArchiverReceipt): boolean => {
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
    console.error('Invalid receipt data', err)
    return false
  }
  err = Utils.validateTypes(receipt.tx, {
    originalTxData: 'o',
    txId: 's',
    timestamp: 'n',
  })
  if (err) {
    console.error('Invalid receipt tx data', err)
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
      console.error('Invalid receipt beforeStateAccounts data', err)
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
      console.error('Invalid receipt accounts data', err)
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
    console.error('Invalid receipt appliedReceipt data', err)
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
    console.error('Invalid receipt appliedReceipt appliedVote data', err)
    return false
  }
  err = Utils.validateTypes(receipt.appliedReceipt.appliedVote.sign, {
    owner: 's',
    sig: 's',
  })
  if (err) {
    console.error('Invalid receipt appliedReceipt appliedVote signature data', err)
    return false
  }
  err = Utils.validateTypes(receipt.appliedReceipt.confirmOrChallenge, {
    message: 's',
    nodeId: 's',
    appliedVote: 'o',
    sign: 'o',
  })
  if (err) {
    console.error('Invalid receipt appliedReceipt confirmOrChallenge data', err)
    return false
  }
  err = Utils.validateTypes(receipt.appliedReceipt.confirmOrChallenge.sign, {
    owner: 's',
    sig: 's',
  })
  if (err) {
    console.error('Invalid receipt appliedReceipt confirmOrChallenge signature data', err)
    return false
  }
  err = Utils.validateTypes(receipt.appliedReceipt.signatures[0], {
    owner: 's',
    sig: 's',
  })
  if (err) {
    console.error('Invalid receipt appliedReceipt signatures data', err)
    return false
  }

  const { appliedVote, confirmOrChallenge } = receipt.appliedReceipt

  if (!Crypto.verify(appliedVote)) {
    console.error('Invalid receipt appliedReceipt appliedVote signature verification failed')
    return false
  }

  if (!Crypto.verify(confirmOrChallenge)) {
    console.error('Invalid receipt appliedReceipt confirmOrChallenge signature verification failed')
    return false
  }

  return true
}

// Converting the correct appReceipt data format to get the correct hash
const calculateAppReceiptDataHash = (appReceiptData: any): string => {
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
    const hash = Crypto.hashObj(appReceiptData)
    return hash
  } catch (err) {
    console.error(`calculateAppReceiptDataHash error: ${err}`)
    return ''
  }
}

export const verifyAppReceiptData = (receipt: Receipt.ArchiverReceipt): boolean => {
  const { appReceiptData, globalModification } = receipt
  const newShardeumReceipt = appReceiptData.data as ShardeumReceipt
  if (!newShardeumReceipt.amountSpent || !newShardeumReceipt.readableReceipt) {
    console.error(`appReceiptData missing amountSpent or readableReceipt`)
    return false
  }
  if (
    newShardeumReceipt.amountSpent === '0x0' &&
    newShardeumReceipt.readableReceipt.status === 0 &&
    receipt.accounts.length > 0
  ) {
    console.error(
      `The receipt has 0 amountSpent and status 0 but has state updated accounts!`,
      receipt.tx.txId,
      receipt.cycle,
      receipt.tx.timestamp
    )
  }
  if (globalModification && config.skipGlobalTxReceiptVerification) return true
  // Finally verify appReceiptData hash
  const appReceiptDataCopy = { ...appReceiptData }
  const calculatedAppReceiptDataHash = calculateAppReceiptDataHash(appReceiptDataCopy)
  if (calculatedAppReceiptDataHash !== receipt.appliedReceipt.app_data_hash) {
    console.error(
      `appReceiptData hash mismatch: ${Crypto.hashObj(appReceiptData)} != ${
        receipt.appliedReceipt.app_data_hash
      }`
    )
    return false
  }
  return true
}

// Verify account hash
export const verifyAccountHash = (receipt: Receipt.ArchiverReceipt): boolean => {
  try {
    if (receipt.globalModification && config.skipGlobalTxReceiptVerification) return true // return true if global modification
    for (const account of receipt.accounts) {
      const calculatedAccountHash = accountSpecificHash(account.data)
      const indexOfAccount = receipt.appliedReceipt.appliedVote.account_id.indexOf(account.accountId)
      if (indexOfAccount === -1) {
        console.error(
          'Account not found',
          account.accountId,
          receipt.tx.txId,
          receipt.cycle,
          receipt.tx.timestamp
        )
        return false
      }
      const expectedAccountHash = receipt.appliedReceipt.appliedVote.account_state_hash_after[indexOfAccount]
      if (calculatedAccountHash !== expectedAccountHash) {
        console.error(
          'Account hash does not match',
          account.accountId,
          receipt.tx.txId,
          receipt.cycle,
          receipt.tx.timestamp
        )
        return false
      }
    }
    return true
  } catch (e) {
    console.error('Error in verifyAccountHash', e)
    return false
  }
}

runProgram()
