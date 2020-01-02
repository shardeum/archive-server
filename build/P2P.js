"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
const State = require("./State");
const Crypto = require("./Crypto");
require("node-fetch");
const node_fetch_1 = require("node-fetch");
function createArchiverJoinRequest() {
    const joinRequest = {
        nodeInfo: State.getNodeInfo(),
    };
    return Crypto.sign(joinRequest);
}
exports.createArchiverJoinRequest = createArchiverJoinRequest;
async function postJson(url, body) {
    try {
        const res = await node_fetch_1.default(url, {
            method: 'post',
            body: JSON.stringify(body),
            headers: { 'Content-Type': 'application/json' },
        });
        if (res.ok) {
            return await res.json();
        }
        else {
            console.warn('postJson failed: got bad response');
            console.warn(res.headers);
            console.warn(res.statusText);
            console.warn(await res.text());
            return null;
        }
    }
    catch (err) {
        console.warn('postJson failed: could not reach host');
        console.warn(err);
        return null;
    }
}
exports.postJson = postJson;
//# sourceMappingURL=P2P.js.map