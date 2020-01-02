"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
const core = require("shardus-crypto-utils");
exports.core = core;
const cryptoTypes = require("./shardus-crypto-types");
exports.types = cryptoTypes;
const State = require("./State");
// Crypto initialization fns
function setCryptoHashKey(hashkey) {
    core(hashkey);
}
exports.setCryptoHashKey = setCryptoHashKey;
function sign(obj) {
    const objCopy = JSON.parse(core.stringify(obj));
    core.signObj(objCopy, State.getSecretKey(), State.getNodeInfo().publicKey);
    return objCopy;
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