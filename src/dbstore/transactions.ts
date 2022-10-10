import { Signature } from "shardus-crypto-types";
import * as db from './sqlite3storage';
import { extractValues, extractValuesFromArray } from './sqlite3storage';
import * as Logger from '../Logger'
import { config } from '../Config'

export interface Transaction {
    txId: string;
    accountId: string;
    timestamp: number;
    cycleNumber: number,
    data: any,
    keys: any,
    result: TxResult,
    originTxData: any,
    sign: Signature
}

export interface TxResult {
    txIdShort: string,
    txResult: string
}

export interface TxRaw {
    tx: {
        raw: string;
        timestamp: number
    }
}

export async function insertTransaction(transaction: Transaction) {
    try {
        const fields = Object.keys(transaction).join(', ');
        const placeholders = Object.keys(transaction).fill('?').join(', ');
        const values = extractValues(transaction);
        let sql =
            'INSERT OR REPLACE INTO transactions (' +
            fields +
            ') VALUES (' +
            placeholders +
            ')';
        await db.run(sql, values);
        if (config.VERBOSE) {
            Logger.mainLogger.debug(
                'Successfully inserted Transaction',
                transaction.txId,
            );
        }
    } catch (e) {
        Logger.mainLogger.error(e);
        Logger.mainLogger.error(
            'Unable to insert Transaction or it is already stored in to database',
            transaction.txId
        );
    }
}

export async function bulkInsertTransactions(transactions: Transaction[]) {
    try {
        const fields = Object.keys(transactions[0]).join(', ');
        const placeholders = Object.keys(transactions[0]).fill('?').join(', ');
        const values = extractValuesFromArray(transactions);
        let sql =
            'INSERT OR REPLACE INTO transactions (' +
            fields +
            ') VALUES (' +
            placeholders +
            ')';
        for (let i = 1; i < transactions.length; i++) {
            sql = sql + ', (' + placeholders +
                ')';
        }
        await db.run(sql, values);
        Logger.mainLogger.debug(
            'Successfully inserted Transactions', transactions.length
        );
    } catch (e) {
        Logger.mainLogger.error(e);
        Logger.mainLogger.error(
            'Unable to bulk insert Transactions', transactions.length
        );
    }
}

export async function queryTransactionByTxId(txId: string) {
    try {
        const sql = `SELECT * FROM transactions WHERE txId=?`;
        let transaction: any = await db.get(sql, [txId]);
        if (transaction) {
            if (transaction.data)
                transaction.data = JSON.parse(transaction.data);
            if (transaction.keys)
                transaction.keys = JSON.parse(transaction.keys);
            if (transaction.result)
                transaction.result = JSON.parse(transaction.result)
            if (transaction.originTxData)
                transaction.originTxData = JSON.parse(transaction.originTxData);
            if (transaction.sign)
                transaction.sign = JSON.parse(transaction.sign);
        }
        if (config.VERBOSE) {
            Logger.mainLogger.debug('Transaction txId', transaction);
        }
        return transaction;
    } catch (e) {
        Logger.mainLogger.error(e);
    }
}

export async function queryTransactionByAccountId(accountId: string) {
    try {
        const sql = `SELECT * FROM transactions WHERE accountId=?`;
        let transaction: any = await db.get(sql, [accountId]);
        if (transaction) {
            if (transaction.data)
                transaction.data = JSON.parse(transaction.data);
            if (transaction.keys)
                transaction.keys = JSON.parse(transaction.keys);
            if (transaction.result)
                transaction.result = JSON.parse(transaction.result)
            if (transaction.originTxData)
                transaction.originTxData = JSON.parse(transaction.originTxData);
            if (transaction.sign)
                transaction.sign = JSON.parse(transaction.sign);
        }
        if (config.VERBOSE) {
            Logger.mainLogger.debug('Transaction accountId', transaction);
        }
        return transaction;
    } catch (e) {
        Logger.mainLogger.error(e);
    }
}

export async function queryLatestTransactions(count) {
    try {
        const sql = `SELECT * FROM transactions ORDER BY cycleNumber DESC, timestamp DESC LIMIT ${count ? count : 100
            }`;
        const transactions: any = await db.all(sql);
        if (config.VERBOSE) {
            Logger.mainLogger.debug('Account latest', transactions);
        }
        return transactions;
    } catch (e) {
        Logger.mainLogger.error(e);
    }
}

export async function queryTransactions(skip = 0, limit = 10000) {
    let transactions
    try {
        const sql = `SELECT * FROM transactions ORDER BY cycleNumber ASC, timestamp ASC LIMIT ${limit} OFFSET ${skip}`
        transactions = await db.all(sql)
    } catch (e) {
        Logger.mainLogger.error(e)
    }
    if (config.VERBOSE) {
        Logger.mainLogger.debug('Transaction transactions', transactions ? transactions.length : transactions, 'skip', skip)
    }
    return transactions
}

export async function queryTransactionCount() {
    let transactions;
    try {
        const sql = `SELECT COUNT(*) FROM transactions`;
        transactions = await db.get(sql, []);
    } catch (e) {
        Logger.mainLogger.error(e);
    }
    if (config.VERBOSE) {
        Logger.mainLogger.debug('Transaction count', transactions);
    }
    if (transactions) transactions = transactions['COUNT(*)'];
    else transactions = 0;
    return transactions;
}