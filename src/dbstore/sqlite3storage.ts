import { SerializeToJsonString } from '../utils/serialization'
import { Database } from 'sqlite3'

export const createDB = async (dbPath: string, dbName: string): Promise<Database> => {
  console.log('dbName', dbName, 'dbPath', dbPath)
  const db = new Database(dbPath, (err) => {
    if (err) {
      console.log('Error opening database:', err)
      throw err
    }
  })
  await run(db, 'PRAGMA journal_mode=WAL')
  db.on('profile', (sql, time) => {
    if (time > 500 && time < 1000) {
      console.log('SLOW QUERY', sql, time)
    } else if (time > 1000) {
      console.log('VERY SLOW QUERY', sql, time)
    }
  })
  console.log(`Database ${dbName} Initialized!`)
  return db
}

/**
 * Close Database Connections Gracefully
 */
export async function close(db: Database, dbName: string): Promise<void> {
  try {
    console.log(`Terminating ${dbName} Database/Indexer Connections...`)
    await new Promise<void>((resolve, reject) => {
      db.close((err) => {
        if (err) {
          console.error(`Error closing ${dbName} 0Database Connection.`)
          reject(err)
        } else {
          console.log(`${dbName} Database connection closed.`)
          resolve()
        }
      })
    })
  } catch (err) {
    console.error(`Error thrown in ${dbName} db close() function: `)
    console.error(err)
  }
}

export async function runCreate(db: Database, createStatement: string): Promise<void> {
  await run(db, createStatement)
}

export async function run(db: Database, sql: string, params = [] || {}): Promise<unknown> {
  return new Promise((resolve, reject) => {
    db.run(sql, params, function (err) {
      if (err) {
        console.log('Error running sql ' + sql)
        console.log(err)
        reject(err)
      } else {
        resolve({ id: this.lastID })
      }
    })
  })
}

export async function get(db: Database, sql: string, params = []): Promise<unknown> {
  return new Promise((resolve, reject) => {
    db.get(sql, params, (err, result) => {
      if (err) {
        console.log('Error running sql: ' + sql)
        console.log(err)
        reject(err)
      } else {
        resolve(result)
      }
    })
  })
}

export async function all(db: Database, sql: string, params = []): Promise<unknown[]> {
  return new Promise((resolve, reject) => {
    db.all(sql, params, (err, rows) => {
      if (err) {
        console.log('Error running sql: ' + sql)
        console.log(err)
        reject(err)
      } else {
        resolve(rows)
      }
    })
  })
}

export function extractValues(object: object): unknown[] {
  try {
    const inputs = []
    for (const column of Object.keys(object)) {
      let value = object[column] // eslint-disable-line security/detect-object-injection
      if (typeof value === 'object') value = SerializeToJsonString(value)
      inputs.push(value)
    }
    return inputs
  } catch (e) {
    console.log(e)
    return null
  }
}

export function extractValuesFromArray(arr: object[]): unknown[] {
  try {
    const inputs = []
    for (const object of arr) {
      for (const column of Object.keys(object)) {
        let value = object[column] // eslint-disable-line security/detect-object-injection
        if (typeof value === 'object') value = SerializeToJsonString(value)
        inputs.push(value)
      }
    }
    return inputs
  } catch (e) {
    console.log(e)
    return null
  }
}
