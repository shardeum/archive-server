import cryptoInit from 'shardus-crypto-utils'
import * as core from 'shardus-crypto-utils'
import * as State from './State'

// Crypto initialization fns

export function setCryptoHashKey(hashkey: string) {
  cryptoInit(hashkey)
}

// Asymmetric Encyption Sign/Verify API

export function sign(obj: core.LooseObject): void {
  core.signObj(obj, State.getSecretKey(), State.getNodeInfo().publicKey)
}

export function verify(obj: core.SignedObject): boolean {
  return core.verifyObj(obj)
}

// HMAC Tag/Authenticate API

export interface TaggedMessage extends core.TaggedObject {
  publicKey: core.publicKey
}

const curvePublicKeys: Map<core.publicKey, core.curvePublicKey> = new Map()
const sharedKeys: Map<core.publicKey, core.sharedKey> = new Map()

function getOrCreateCurvePk(pk: core.publicKey): core.curvePublicKey {
  let curvePk = curvePublicKeys.get(pk)
  if (!curvePk) {
    curvePk = core.convertPkToCurve(pk)
    curvePublicKeys.set(pk, curvePk)
  }
  return curvePk
}

function getOrCreateSharedKey(pk: core.publicKey): core.sharedKey {
  let sharedK = sharedKeys.get(pk)
  if (!sharedK) {
    const ourCurveSk = State.getCurveSk()
    const theirCurvePk = getOrCreateCurvePk(pk)
    sharedK = core.generateSharedKey(ourCurveSk, theirCurvePk)
  }
  return sharedK
}

export function tag(
  obj: core.LooseObject,
  recipientPk: core.publicKey
): TaggedMessage {
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

export { core }
