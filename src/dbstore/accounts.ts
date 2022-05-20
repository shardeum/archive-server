import * as db from './sqlite3storage';
import { extractValues } from './sqlite3storage';
import * as Logger from '../Logger'
import { config } from '../Config'

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
        if (config.VERBOSE) {
            Logger.mainLogger.debug(
                'Successfully inserted Account', account.accountId
            );
        }
    } catch (e) {
        Logger.mainLogger.error(e);
        Logger.mainLogger.error(
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
        if (config.VERBOSE) {
            Logger.mainLogger.debug(
                'Successfully updated Account', account.accountId
            );
        }
    } catch (e) {
        Logger.mainLogger.error(e);
        Logger.mainLogger.error('Unable to update Account', account);
    }
}

export async function queryAccountByAccountId(accountId: string) {
    try {
        const sql = `SELECT * FROM accounts WHERE accountId=?`;
        let account: any = await db.get(sql, [accountId]);
        if (account)
            if (account && account.data)
                account.data = JSON.parse(account.data);
        if (config.VERBOSE) {
            Logger.mainLogger.debug('Account accountId', account);
        }
        return account;
    } catch (e) {
        Logger.mainLogger.error(e);
    }
}

export async function queryLatestAccounts(count) {
    try {
        const sql = `SELECT * FROM accounts ORDER BY cycleNumber DESC, timestamp DESC LIMIT ${count ? count : 100
            }`;
        const accounts: any = await db.all(sql);
        if (config.VERBOSE) {
            Logger.mainLogger.debug('Account latest', accounts);
        }
        return accounts;
    } catch (e) {
        Logger.mainLogger.error(e);
    }
}

export async function queryAccounts(skip = 0, limit = 10000) {
    let accounts
    try {
        const sql = `SELECT * FROM accounts ORDER BY cycleNumber ASC, timestamp ASC LIMIT ${limit} OFFSET ${skip}`
        accounts = await db.all(sql)
    } catch (e) {
        Logger.mainLogger.error(e)
    }
    if (config.VERBOSE) {
        Logger.mainLogger.debug('Account accounts', accounts ? accounts.length : accounts, 'skip', skip)
    }
    return accounts
}

export async function queryAccountCount() {
    let accounts;
    try {
        const sql = `SELECT COUNT(*) FROM accounts`;
        accounts = await db.get(sql, []);
    } catch (e) {
        Logger.mainLogger.error(e);
    }
    if (config.VERBOSE) {
        Logger.mainLogger.debug('Account count', accounts);
    }
    if (accounts) accounts = accounts['COUNT(*)'];
    else accounts = 0;
    return accounts;
}