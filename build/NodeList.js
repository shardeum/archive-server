"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
// TYPES
var Statuses;
(function (Statuses) {
    Statuses["ACTIVE"] = "active";
    Statuses["SYNCING"] = "syncing";
})(Statuses = exports.Statuses || (exports.Statuses = {}));
// STATE
const list = [];
const syncingList = new Map();
const activeList = new Map();
const byPublicKey = {};
const byIpPort = {};
// METHODS
function getIpPort({ ip, port }) {
    return ip + ':' + port;
}
function isEmpty() {
    return list.length <= 0;
}
exports.isEmpty = isEmpty;
function addNodes(status, ...nodes) {
    for (const node of nodes) {
        if (byPublicKey[node.publicKey] !== undefined) {
            console.warn(`addNodes failed: publicKey ${node.publicKey} already in nodelist`);
            return;
        }
        const ipPort = getIpPort(node);
        if (byIpPort[ipPort] !== undefined) {
            console.warn(`addNodes failed: ipPort ${ipPort} already in nodelist`);
            return;
        }
        list.push(node);
        if (status === Statuses.SYNCING) {
            syncingList.set(node.publicKey, node);
        }
        else if (status === Statuses.ACTIVE) {
            activeList.set(node.publicKey, node);
        }
        byPublicKey[node.publicKey] = node;
        byIpPort[ipPort] = node;
    }
}
exports.addNodes = addNodes;
function removeNodes(...publicKeys) {
    // Efficiently remove nodes from nodelist
    const keysToDelete = new Map();
    for (const key of publicKeys) {
        if (byPublicKey[key] === undefined) {
            console.warn(`removeNodes: publicKey ${key} not in nodelist`);
            continue;
        }
        keysToDelete.set(key, true);
        delete byIpPort[getIpPort(byPublicKey[key])];
        delete byPublicKey[key];
    }
    if (keysToDelete.size > 0) {
        let key;
        for (let i = list.length - 1; i > -1; i--) {
            key = list[i].publicKey;
            if (keysToDelete.has(key)) {
                list.splice(i, 1);
                if (syncingList.has(key))
                    syncingList.delete(key);
                else if (activeList.has(key))
                    activeList.delete(key);
            }
        }
    }
    return [...keysToDelete.keys()];
}
exports.removeNodes = removeNodes;
function setStatus(status, ...publicKeys) {
    for (const key of publicKeys) {
        const node = byPublicKey[key];
        if (node === undefined) {
            console.warn(`setStatus: publicKey ${key} not in nodelist`);
            continue;
        }
        if (status === Statuses.SYNCING) {
            if (activeList.has(key))
                activeList.delete(key);
            if (syncingList.has(key))
                continue;
            syncingList.set(key, node);
        }
        else if (status === Statuses.ACTIVE) {
            if (syncingList.has(key))
                syncingList.delete(key);
            if (activeList.has(key))
                continue;
            activeList.set(key, node);
        }
    }
}
exports.setStatus = setStatus;
function getList() {
    return list;
}
exports.getList = getList;
function getActiveList() {
    return [...activeList.values()];
}
exports.getActiveList = getActiveList;
function getSyncingList() {
    return [...syncingList.values()];
}
exports.getSyncingList = getSyncingList;
function getNodeInfo(node) {
    // Prefer publicKey
    if (node.publicKey) {
        return byPublicKey[node.publicKey];
    }
    // Then, ipPort
    else if (node.ip && node.port) {
        return byIpPort[getIpPort(node)];
    }
    // If nothing found, return undefined
    return undefined;
}
exports.getNodeInfo = getNodeInfo;
//# sourceMappingURL=NodeList.js.map