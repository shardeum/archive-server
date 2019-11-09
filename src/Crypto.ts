import crypto = require('shardus-crypto-utils');

export function setCryptoHashKey(hashkey: string) {
  crypto(hashkey);
}

export { crypto };
