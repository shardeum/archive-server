import { Signature } from "shardus-crypto-types";
import * as db from './sqlite3storage';
import { extractValues } from './sqlite3storage';

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
        console.log(
            'Successfully inserted Transaction',
            transaction.txId,
        );
    } catch (e) {
        console.log(e);
        console.log(
            'Unable to insert Transaction or it is already stored in to database',
            transaction.txId
        );
    }
}