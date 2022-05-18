const sqlite3 = require('sqlite3').verbose();
let db: any;

export async function init() {
    db = new sqlite3.Database('db.sqlite3');
    // await run('PRAGMA journal_mode=WAL');
    console.log('Database initialized.');
}

export async function runCreate(createStatement) {
    await run(createStatement);
}

export async function run(sql, params = [] || {}) {
    return new Promise((resolve, reject) => {
        db.run(sql, params, function (err) {
            if (err) {
                console.log('Error running sql ' + sql);
                console.log(err);
                reject(err);
            } else {
                resolve({ id: this.lastID });
            }
        });
    });
}

export async function get(sql, params = []) {
    return new Promise((resolve, reject) => {
        db.get(sql, params, (err, result) => {
            if (err) {
                console.log('Error running sql: ' + sql);
                console.log(err);
                reject(err);
            } else {
                resolve(result);
            }
        });
    });
}

export async function all(sql, params = []) {
    return new Promise((resolve, reject) => {
        db.all(sql, params, (err, rows) => {
            if (err) {
                console.log('Error running sql: ' + sql);
                console.log(err);
                reject(err);
            } else {
                resolve(rows);
            }
        });
    });
}

export function extractValues(object: any): any {
    try {
        const inputs = [];
        for (const column of Object.keys(object)) {
            let value = object[column];
            if (typeof value === 'object') value = JSON.stringify(value);
            inputs.push(value);
        }
        return inputs;
    } catch (e) {
        console.log(e);
    }
}
