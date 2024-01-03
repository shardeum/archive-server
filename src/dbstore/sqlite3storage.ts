import * as fs from 'fs'
import * as path from 'path'
import { Config } from '../Config'
import { SerializeToJsonString } from '../utils/serialization'
import { Database } from 'sqlite3'
// eslint-disable-next-line @typescript-eslint/no-var-requires
const sqlite3 = require('sqlite3').verbose()
let db: Database

export async function init(config: Config): Promise<void> {
  console.log(config.ARCHIVER_DB)
  createDirectories(config.ARCHIVER_DB)
  const dbName = `${config.ARCHIVER_DB}/archiverdb-${config.ARCHIVER_PORT}.sqlite3`
  // const dbName = config.ARCHIVER_DB
  db = new sqlite3.Database(dbName)
  await run('PRAGMA journal_mode=WAL')
  console.log('Database initialized.')
}

export async function runCreate(createStatement: string): Promise<void> {
  await run(createStatement)
}

export async function run(sql: string, params = [] || {}): Promise<unknown> {
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

export async function get(sql: string, params = []): Promise<unknown> {
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

export async function all(sql: string, params = []): Promise<unknown[]> {
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

function createDirectories(pathname: string): void {
  const __dirname = path.resolve()
  pathname = pathname.replace(/^\.*\/|\/?[^/]+\.[a-z]+|\/$/g, '') // Remove leading directory markers, and remove ending /file-name.extension
  fs.mkdirSync(path.resolve(__dirname, pathname), { recursive: true }) // eslint-disable-line security/detect-non-literal-fs-filename
}
