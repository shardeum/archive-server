import { config } from './Config'
import * as Crypto from './Crypto'

let lastCounter = 0

export function isDebugMode(): boolean {
  return !!(config && config.ARCHIVER_MODE && config.ARCHIVER_MODE === 'debug')
}

function getDevPublicKey(): string {
  if (config && config.DevPublicKey) {
    return config.DevPublicKey
  }
  return ''
}

export const isDebugMiddleware = (_req, res): void => {
  const isDebug = isDebugMode()
  if (!isDebug) {
    try {
      //auth my by checking a signature
      if (_req.query.sig != null && _req.query.sig_counter != null) {
        const ownerPk = getDevPublicKey()
        const requestSig = _req.query.sig
        //check if counter is valid
        const sigObj = {
          route: _req.routerPath,
          count: _req.query.sig_counter,
          sign: { owner: ownerPk, sig: requestSig },
        }
        const currentCounter = parseInt(sigObj.count)
        //reguire a larger counter than before.
        if (currentCounter < lastCounter) {
          const verified = Crypto.verify(sigObj)
          if (!verified) {
            throw new Error('FORBIDDEN. signature authentication is failed.')
          }
        } else {
          throw new Error('FORBIDDEN. signature counter is failed.')
        }
        lastCounter = currentCounter //update counter so we can't use it again
        return
      }
      throw new Error('FORBIDDEN. Endpoint is only available in debug mode.')
    } catch (error) {
      // console.log(error)
      // throw new Error('FORBIDDEN. Endpoint is only available in debug mode.')
      res.code(401).send(error)
    }
  }
}
