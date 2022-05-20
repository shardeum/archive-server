import { Signature } from "shardus-crypto-types";
import * as db from './sqlite3storage';
import { extractValues } from './sqlite3storage';
import * as Logger from '../Logger'
import { config } from '../Config'

export interface Transaction {
    txId: string;
    timestamp: number;
    cycleNumber: number,
    data: any,
    keys: any,
    result: TxResult,
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