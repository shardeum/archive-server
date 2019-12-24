"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
const shardus_crypto_utils_1 = require("shardus-crypto-utils");
const core = require("shardus-crypto-utils");
exports.core = core;
const State = require("./State");
// Crypto initialization fns
function setCryptoHashKey(hashkey) {
    shardus_crypto_utils_1.default(hashkey);
}
exports.setCryptoHashKey = setCryptoHashKey;
// Asymmetric Encyption Sign/Verify API
function sign(obj) {
    core.signObj(obj, State.getSecretKey(), State.getNodeInfo().publicKey);
}
exports.sign = sign;
function verify(obj) {
    return core.verifyObj(obj);
}
exports.verify = verify;
const curvePublicKeys = new Map();
const sharedKeys = new Map();
function getOrCreateCurvePk(pk) {
    let curvePk = curvePublicKeys.get(pk);
    if (!curvePk) {
        curvePk = core.convertPkToCurve(pk);
        curvePublicKeys.set(pk, curvePk);
    }
    return curvePk;
}
function getOrCreateSharedKey(pk) {
    let sharedK = sharedKeys.get(pk);
    if (!sharedK) {
        const ourCurveSk = State.getCurveSk();
        const theirCurvePk = getOrCreateCurvePk(pk);
        sharedK = core.generateSharedKey(ourCurveSk, theirCurvePk);
    }
    return sharedK;
}
function tag(obj, recipientPk) {
    const sharedKey = getOrCreateSharedKey(recipientPk);
    const objCopy = JSON.parse(core.stringify(obj));
    objCopy.publicKey = State.getNodeInfo().publicKey;
    core.tagObj(objCopy, sharedKey);
    return objCopy;
}
exports.tag = tag;
function authenticate(msg) {
    const sharedKey = getOrCreateSharedKey(msg.publicKey);
    return core.authenticateObj(msg, sharedKey);
}
exports.authenticate = authenticate;
//# sourceMappingURL=Crypto.js.map