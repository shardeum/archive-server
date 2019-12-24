"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
const Crypto_1 = require("./Crypto");
const nodeState = {
    ip: '',
    port: -1,
    publicKey: '',
    secretKey: '',
    curvePk: '',
    curveSk: '',
};
exports.existingArchivers = [];
exports.isFirst = false;
exports.dbFile = '';
function initFromConfig(config) {
    // Get own nodeInfo from config
    nodeState.ip = config.ARCHIVER_IP;
    nodeState.port = config.ARCHIVER_PORT;
    nodeState.publicKey = config.ARCHIVER_PUBLIC_KEY;
    nodeState.secretKey = config.ARCHIVER_SECRET_KEY;
    nodeState.curvePk = Crypto_1.core.convertPkToCurve(nodeState.publicKey);
    nodeState.curveSk = Crypto_1.core.convertSkToCurve(nodeState.secretKey);
    // Parse existing archivers list
    try {
        exports.existingArchivers = JSON.parse(config.ARCHIVER_EXISTING);
    }
    catch (e) {
        console.warn('Failed to parse ARCHIVER_EXISTING array:', config.ARCHIVER_EXISTING);
    }
    // You're first, unless existing archiver info is given
    exports.isFirst = exports.existingArchivers.length <= 0;
    // Get db file location from config
    exports.dbFile = config.ARCHIVER_DB;
}
exports.initFromConfig = initFromConfig;
function getNodeInfo() {
    const sanitizedNodeInfo = Object.assign({}, nodeState);
    delete sanitizedNodeInfo.secretKey;
    delete sanitizedNodeInfo.curveSk;
    return sanitizedNodeInfo;
}
exports.getNodeInfo = getNodeInfo;
function getSecretKey() {
    return nodeState.secretKey;
}
exports.getSecretKey = getSecretKey;
function getCurveSk() {
    return nodeState.curveSk;
}
exports.getCurveSk = getCurveSk;
//# sourceMappingURL=State.js.map