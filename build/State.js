"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
const nodeInfo = {
    ip: '',
    port: -1,
    publicKey: '',
    secretKey: '',
};
exports.existingArchivers = [];
exports.isFirst = false;
exports.dbFile = '';
function initFromConfig(config) {
    // Get own nodeInfo from config
    nodeInfo.ip = config.ARCHIVER_IP;
    nodeInfo.port = config.ARCHIVER_PORT;
    nodeInfo.publicKey = config.ARCHIVER_PUBLIC_KEY;
    nodeInfo.secretKey = config.ARCHIVER_SECRET_KEY;
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
    const sanitizedNodeInfo = Object.assign({}, nodeInfo);
    delete sanitizedNodeInfo.secretKey;
    return sanitizedNodeInfo;
}
exports.getNodeInfo = getNodeInfo;
function getSecretKey() {
    return nodeInfo.secretKey;
}
exports.getSecretKey = getSecretKey;
//# sourceMappingURL=State.js.map