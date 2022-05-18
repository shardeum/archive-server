import * as Account from "../db/accounts"
import * as Transaction from "../db/transactions"
import * as Cycle from "../db/cycles"

export const processReceiptData = async (receipts = []) => {
    if (receipts && receipts.length <= 0) return
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
            const accountExist = await Account.queryAccountByAccountId(account.accountId)
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
            sign: sign
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
            cycleRecord
        }
        const cycleExist = await Cycle.queryCycleByMarker(cycleObj.cycleMarker);
        if (cycleExist) {
            if (JSON.stringify(cycleObj) !== JSON.stringify(cycleExist))
                await Cycle.updateCycle(cycleObj.cycleMarker, cycleObj);
        } else {
            await Cycle.insertCycle(cycleObj);
        }
    }
}