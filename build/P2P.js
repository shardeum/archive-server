"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
const State = require("./State");
const Crypto_1 = require("./Crypto");
require("node-fetch");
const node_fetch_1 = require("node-fetch");
function createJoinRequest() {
    const nodeInfo = State.getNodeInfo();
    const joinRequest = {
        nodeInfo,
    };
    Crypto_1.crypto.signObj(joinRequest, State.getSecretKey(), State.getNodeInfo().publicKey);
    return joinRequest;
}
exports.createJoinRequest = createJoinRequest;
async function postJson(url, body) {
    try {
        const res = await node_fetch_1.default(url, {
            method: 'post',
            body: JSON.stringify(body),
            headers: { 'Content-Type': 'application/json' },
        });
        if (res.ok) {
            return res.json();
        }
        else {
            console.warn('postJson failed: got bad response');
            console.warn(res.headers);
            console.warn(res.statusText);
            console.warn(res.text());
            return null;
        }
    }
    catch (err) {
        console.warn('postJson failed: could not reach host');
        console.warn(err);
        return null;
    }
}
async function addToCycleRecipients(node) {
    const url = `http://${node.ip}:${node.port}/addtocyclerecipients`;
    const res = await postJson(url, State.getNodeInfo());
}
exports.addToCycleRecipients = addToCycleRecipients;
function removeFromCycleRecipients(node) { }
exports.removeFromCycleRecipients = removeFromCycleRecipients;
//# sourceMappingURL=P2P.js.map