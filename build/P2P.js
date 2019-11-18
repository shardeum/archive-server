"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
const State_1 = require("./State");
const Crypto_1 = require("./Crypto");
function createJoinRequest() {
    const nodeInfo = Object.assign({}, State_1.state.nodeInfo);
    delete nodeInfo.secretKey;
    const joinRequest = {
        nodeInfo,
    };
    Crypto_1.crypto.signObj(joinRequest, State_1.state.nodeInfo.secretKey, State_1.state.nodeInfo.publicKey);
    return joinRequest;
}
exports.createJoinRequest = createJoinRequest;
//# sourceMappingURL=P2P.js.map