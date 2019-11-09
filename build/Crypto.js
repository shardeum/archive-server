"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
const crypto = require("shardus-crypto-utils");
exports.crypto = crypto;
function setCryptoHashKey(hashkey) {
    crypto(hashkey);
}
exports.setCryptoHashKey = setCryptoHashKey;
//# sourceMappingURL=Crypto.js.map