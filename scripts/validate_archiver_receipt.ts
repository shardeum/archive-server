import { join } from 'path'
import { overrideDefaultConfig, config } from '../src/Config'
import * as Crypto from '../src/Crypto'
import * as Utils from '../src/Utils'
import * as Receipt from '../src/dbstore/receipts'

const receipt: any = {}

const runProgram = async (): Promise<void> => {
  // Override default config params from config file, env vars, and cli args
  const file = join(process.cwd(), 'archiver-config.json')
  overrideDefaultConfig(file)
  // Set crypto hash keys from config
  const hashKey = config.ARCHIVER_HASH_KEY
  Crypto.setCryptoHashKey(hashKey)
  validateReceiptData(receipt)
}

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

runProgram()
