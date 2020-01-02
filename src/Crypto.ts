import core = require('shardus-crypto-utils')
import * as cryptoTypes from './shardus-crypto-types'
import * as State from './State'

// Crypto initialization fns

export function setCryptoHashKey(hashkey: string) {
  core(hashkey)
}

// Asymmetric Encyption Sign/Verify API
export type SignedMessage = cryptoTypes.SignedObject

export function sign<T>(obj: T): T & cryptoTypes.SignedObject {
  const objCopy = JSON.parse(core.stringify(obj))
  core.signObj(objCopy, State.getSecretKey(), State.getNodeInfo().publicKey)
  return objCopy
}

export function verify(obj: cryptoTypes.SignedObject): boolean {
  return core.verifyObj(obj)
}

// HMAC Tag/Authenticate API

export interface TaggedMessage extends cryptoTypes.TaggedObject {
  publicKey: cryptoTypes.publicKey
}

const curvePublicKeys: Map<
  cryptoTypes.publicKey,
  cryptoTypes.curvePublicKey
> = new Map()
const sharedKeys: Map<cryptoTypes.publicKey, cryptoTypes.sharedKey> = new Map()

function getOrCreateCurvePk(
  pk: cryptoTypes.publicKey
): cryptoTypes.curvePublicKey {
  let curvePk = curvePublicKeys.get(pk)
  if (!curvePk) {
    curvePk = core.convertPkToCurve(pk)
    curvePublicKeys.set(pk, curvePk)
  }
  return curvePk
}

function getOrCreateSharedKey(
  pk: cryptoTypes.publicKey
): cryptoTypes.sharedKey {
  let sharedK = sharedKeys.get(pk)
  if (!sharedK) {
    const ourCurveSk = State.getCurveSk()
    const theirCurvePk = getOrCreateCurvePk(pk)
    sharedK = core.generateSharedKey(ourCurveSk, theirCurvePk)
  }
  return sharedK
}

export function tag<T>(
  obj: T,
  recipientPk: cryptoTypes.publicKey
): T & TaggedMessage {
  const sharedKey = getOrCreateSharedKey(recipientPk)
  const objCopy = JSON.parse(core.stringify(obj))
  objCopy.publicKey = State.getNodeInfo().publicKey
  core.tagObj(objCopy, sharedKey)
  return objCopy
}

export function authenticate(msg: TaggedMessage): boolean {
  const sharedKey = getOrCreateSharedKey(msg.publicKey)
  return core.authenticateObj(msg, sharedKey)
}

export { core, cryptoTypes as types }
