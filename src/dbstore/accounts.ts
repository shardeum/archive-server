import * as db from './sqlite3storage';
import { extractValues } from './sqlite3storage';

export type AccountCopy = {
    accountId: string;
    data: any;
    timestamp: number;
    hash: string;
    cycleNumber: number;
    isGlobal?: boolean
};

export async function insertAccount(account: AccountCopy) {
    try {
        const fields = Object.keys(account).join(', ');
        const placeholders = Object.keys(account).fill('?').join(', ');
        const values = extractValues(account);
        let sql =
            'INSERT OR REPLACE INTO accounts (' +
            fields +
            ') VALUES (' +
            placeholders +
            ')';
        await db.run(sql, values);
        console.log(
            'Successfully inserted Account', account.accountId
        );
    } catch (e) {
        console.log(e);
        console.log(
            'Unable to insert Account or it is already stored in to database',
            account.accountId
        );
    }
}

export async function updateAccount(accountId: string, account: AccountCopy) {
    try {
        const sql = `UPDATE accounts SET cycleNumber = $cycleNumber, timestamp = $timestamp, data = $data, hash = $hash WHERE accountId = $accountId `;
        await db.run(sql, {
            $cycleNumber: account.cycleNumber,
            $timestamp: account.timestamp,
            $data: account.data && JSON.stringify(account.data),
            $hash: account.hash,
            $accountId: account.accountId,
        });
        console.log(
            'Successfully updated Account', account.accountId
        );
    } catch (e) {
        console.log(e);
        console.log('Unable to update Account', account);
    }
}

export async function queryAccountByAccountId(accountId: string) {
    try {
        const sql = `SELECT * FROM accounts WHERE accountId=?`;
        let account: any = await db.get(sql, [accountId]);
        if (account)
            if (account && account.data)
                account.data = JSON.parse(account.data);
        console.log('Account accountId', account);
        return account;
    } catch (e) {
        console.log(e);
    }
}