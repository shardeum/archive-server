"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
const state = {
    nodeInfo: {
        ip: '',
        port: -1,
        publicKey: '',
        secretKey: '',
    },
    existingArchivers: [],
    isFirst: false,
    dbFile: '',
    cycleSender: {
        ip: '',
        port: -1,
        publicKey: '',
    },
};
exports.state = state;
function initStateFromConfig(config) {
    // Get own nodeInfo from config
    state.nodeInfo.ip = config.ARCHIVER_IP;
    state.nodeInfo.port = config.ARCHIVER_PORT;
    state.nodeInfo.publicKey = config.ARCHIVER_PUBLIC_KEY;
    state.nodeInfo.secretKey = config.ARCHIVER_SECRET_KEY;
    // Parse existing archivers list
    try {
        state.existingArchivers = JSON.parse(config.ARCHIVER_EXISTING);
    }
    catch (e) {
        console.warn('Failed to parse ARCHIVER_EXISTING array:', config.ARCHIVER_EXISTING);
    }
    // You're first, unless existing archiver info is given
    state.isFirst = state.existingArchivers.length <= 0;
    // Get db file location from config
    state.dbFile = config.ARCHIVER_DB;
}
exports.initStateFromConfig = initStateFromConfig;
//# sourceMappingURL=State.js.map