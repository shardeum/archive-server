import * as Account from '../dbstore/accounts'
import * as Transaction from '../dbstore/transactions'
import * as Cycle from '../dbstore/cycles'
import * as Crypto from '../Crypto'
import { socketServer } from './Data'

export const processReceiptData = async (receipts = []) => {
  if (receipts && receipts.length <= 0) return
  if (socketServer) {
    let signedDataToSend = Crypto.sign({
      receipts: receipts,
    })
    socketServer.emit('RECEIPT', signedDataToSend)
  }
  for (let i = 0; i < receipts.length; i++) {
    const { accounts, cycle, result, sign, tx } = receipts[i]
    for (let j = 0; j < accounts.length; j++) {
      const account = accounts[j]
      const accObj: Account.AccountCopy = {
        accountId: account.accountId,
        data: account.data,
        timestamp: account.timestamp,
        hash: account.hash,
        cycleNumber: cycle,
      }
      const accountExist = await Account.queryAccountByAccountId(
        account.accountId
      )
      if (accountExist) {
        if (accObj.timestamp > accountExist.timestamp)
          await Account.updateAccount(accObj.accountId, accObj)
      } else {
        await Account.insertAccount(accObj)
      }
    }
    const txObj: Transaction.Transaction = {
      txId: tx.txId,
      timestamp: tx.timestamp,
      cycleNumber: cycle,
      data: tx.data,
      keys: tx.keys,
      result: result,
      sign: sign,
    }
    await Transaction.insertTransaction(txObj)
  }
}

export const processCycleData = async (cycles = []) => {
  if (cycles && cycles.length <= 0) return
  for (let i = 0; i < cycles.length; i++) {
    const cycleRecord = cycles[i]
    const cycleObj: Cycle.Cycle = {
      counter: cycleRecord.counter,
      cycleMarker: cycleRecord.marker,
      cycleRecord,
    }
    if (socketServer) {
      let signedDataToSend = Crypto.sign({
        cycles: [cycleObj],
      })
      socketServer.emit('RECEIPT', signedDataToSend)
    }
    const cycleExist = await Cycle.queryCycleByMarker(cycleObj.cycleMarker)
    if (cycleExist) {
      if (JSON.stringify(cycleObj) !== JSON.stringify(cycleExist))
        await Cycle.updateCycle(cycleObj.cycleMarker, cycleObj)
    } else {
      await Cycle.insertCycle(cycleObj)
    }
  }
}
