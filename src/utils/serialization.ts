import { stringify } from './stringify'
import { config } from '../Config'

export function SerializeToJsonString(obj: any): string {
  try {
    if (config.useSerialization) return stringify(obj, { bufferEncoding: 'base64' })
    else return JSON.stringify(obj)
  } catch (e) {
    console.log('Error serializing object', e)
    console.log(obj)
    throw e
  }
}

export function DeSerializeFromJsonString<T>(jsonString: string): T {
  try {
    if (config.useSerialization) return <T>JSON.parse(jsonString, base64BufferReviver)
    else return JSON.parse(jsonString)
  } catch (e) {
    console.log('Error deserializing object', e)
    console.log(jsonString)
    throw e
  }
}

function base64BufferReviver(key: string, value: any) {
  const originalObject = value
  if (
    isObject(originalObject) &&
    originalObject.hasOwnProperty('dataType') &&
    originalObject.dataType &&
    originalObject.dataType == 'bh'
  ) {
    return new Uint8Array(GetBufferFromField(originalObject, 'base64'))
    // } else if (value && isHexStringWithoutPrefix(value) && value.length !== 42 && value.length !== 64) {
    //   console.log('hex string', value)
    //   return BigInt('0x' + value)
  } else {
    return value
  }
}

export const isObject = (val) => {
  if (val === null) {
    return false
  }
  if (Array.isArray(val)) {
    return false
  }
  return typeof val === 'function' || typeof val === 'object'
}

export function GetBufferFromField(input: any, encoding?: 'base64' | 'hex'): Buffer {
  switch (encoding) {
    case 'base64':
      return Buffer.from(input.data, 'base64')
    default:
      return Buffer.from(input)
  }
}

export function isHexStringWithoutPrefix(value: string, length?: number): boolean {
  if (value && typeof value === 'string' && value.indexOf('0x') >= 0) return false // do not convert strings with 0x
  // prefix
  if (typeof value !== 'string' || !value.match(/^[0-9A-Fa-f]*$/)) return false

  if (typeof length !== 'undefined' && length > 0 && value.length !== 2 + 2 * length) return false

  return true
}
