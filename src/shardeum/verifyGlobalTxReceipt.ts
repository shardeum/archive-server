import { P2P } from '@shardus/types'
import { ArchiverReceipt } from '../dbstore/receipts'
import { accountSpecificHash } from './calculateAccountHash'

// Refer to https://github.com/shardeum/shardeum/blob/89db23e1d4ffb86b4353b8f37fb360ea3cd93c5b/src/shardeum/shardeumTypes.ts#L242
export interface SetGlobalTxValue {
  isInternalTx: boolean
  internalTXType: InternalTXType
  timestamp: number
  from: string
  change: {
    cycle: number
    change: object
  }
}

// Refer to https://github.com/shardeum/shardeum/blob/89db23e1d4ffb86b4353b8f37fb360ea3cd93c5b/src/shardeum/shardeumTypes.ts#L87-L88
export enum InternalTXType {
  SetGlobalCodeBytes = 0, //Deprecated
  InitNetwork = 1,
  NodeReward = 2, //Deprecated
  ChangeConfig = 3,
  ApplyChangeConfig = 4,
  SetCertTime = 5,
  Stake = 6,
  Unstake = 7,
  InitRewardTimes = 8,
  ClaimReward = 9,
  ChangeNetworkParam = 10,
  ApplyNetworkParam = 11,
  Penalty = 12,
}

export const verifyGlobalTxAccountChange = (
  receipt: ArchiverReceipt,
  failedReasons = [],
  nestedCounterMessages = []
): boolean => {
  try {
    const signedReceipt = receipt.signedReceipt as P2P.GlobalAccountsTypes.GlobalTxReceipt
    const internalTx = signedReceipt.tx.value as SetGlobalTxValue

    if (internalTx.internalTXType === InternalTXType.InitNetwork) {
      // Refer to https://github.com/shardeum/shardeum/blob/89db23e1d4ffb86b4353b8f37fb360ea3cd93c5b/src/index.ts#L2334
      // no need to do anything, as it is network account creation
      return true
    } else if (
      internalTx.internalTXType === InternalTXType.ApplyChangeConfig ||
      internalTx.internalTXType === InternalTXType.ApplyNetworkParam
    ) {
      if (signedReceipt.tx.addressHash !== '') {
        for (const account of receipt.beforeStates) {
          if (account.accountId !== signedReceipt.tx.address) {
            failedReasons.push(
              `Unexpected account found in before accounts ${receipt.tx.txId} , ${receipt.cycle} , ${receipt.tx.timestamp}`
            )
            nestedCounterMessages.push(`Unexpected account found in before accounts`)
            return false
          }
          const expectedAccountHash = signedReceipt.tx.addressHash
          const calculatedAccountHash = accountSpecificHash(account.data)
          if (expectedAccountHash !== calculatedAccountHash) {
            failedReasons.push(
              `Account hash before does not match in globalModification tx - ${account.accountId} , ${receipt.tx.txId} , ${receipt.cycle} , ${receipt.tx.timestamp}`
            )
            nestedCounterMessages.push(`Account hash before does not match in globalModification tx`)
            return false
          }
        }
      }
      for (const account of receipt.afterStates) {
        if (account.accountId !== signedReceipt.tx.address) {
          failedReasons.push(
            `Unexpected account found in accounts ${receipt.tx.txId} , ${receipt.cycle} , ${receipt.tx.timestamp}`
          )
          nestedCounterMessages.push(`Unexpected account found in accounts`)
          return false
        }
        const networkAccountBefore = receipt.beforeStates.find(
          (bAccount) => bAccount?.accountId === account.accountId
        )
        const networkAccountAfter = receipt.afterStates.find(
          (fAccount) => fAccount?.accountId === signedReceipt.tx.address
        )
        if (!networkAccountBefore || !networkAccountAfter) {
          failedReasons.push(
            `No network account found in accounts ${receipt.tx.txId} , ${receipt.cycle} , ${receipt.tx.timestamp}`
          )
          nestedCounterMessages.push(`No network account found in accounts`)
          return false
        }
        networkAccountBefore.data.listOfChanges?.push(internalTx.change)
        networkAccountBefore.data.timestamp = signedReceipt.tx.when
        const expectedAccountHash = networkAccountAfter.hash
        console.dir(networkAccountBefore, { depth: null })
        const calculatedAccountHash = accountSpecificHash(networkAccountBefore.data)
        if (expectedAccountHash !== calculatedAccountHash) {
          failedReasons.push(
            `Account hash does not match in globalModification tx - ${networkAccountAfter.accountId} , ${receipt.tx.txId} , ${receipt.cycle} , ${receipt.tx.timestamp}`
          )
          nestedCounterMessages.push(`Account hash does not match in globalModification tx`)
          return false
        }
      }
      return true
    } else {
      failedReasons.push(
        `Unexpected internal transaction type in the globalModification tx ${receipt.tx.txId} , ${receipt.cycle} , ${receipt.tx.timestamp}`
      )
      nestedCounterMessages.push(`Unexpected internal transaction type in the globalModification tx`)
      return false
    }
  } catch (error) {
    console.error(`verifyGlobalTxAccountChange error`, error)
    failedReasons.push(
      `Error while verifying global account change ${receipt.tx.txId} , ${receipt.cycle} , ${receipt.tx.timestamp}, ${error}`
    )
    nestedCounterMessages.push(`Error while verifying global account change`)
    return false
  }
}
