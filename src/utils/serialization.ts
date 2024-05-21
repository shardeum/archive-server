import { Utils as StringUtils } from '@shardus/types'
import { config } from '../Config'

export function SerializeToJsonString(obj: object): string {
  try {
    if (config.useSerialization) return StringUtils.safeStringify(obj, { bufferEncoding: 'base64' })
    else return StringUtils.safeStringify(obj)
  } catch (e) {
    console.log('Error serializing object', e)
    console.log(obj)
    throw e
  }
}

export function DeSerializeFromJsonString<T>(jsonString: string): T {
  try {
    if (config.useSerialization) return <T>StringUtils.safeJsonParse(jsonString)
    else return StringUtils.safeJsonParse(jsonString)
  } catch (e) {
    console.log('Error deserializing object', e)
    console.log(jsonString)
    throw e
  }
}
