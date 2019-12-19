"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
const Storage = require("./Storage");
const NodeList = require("./NodeList");
const Utils_1 = require("./Utils");
const cycleSenders = new Map();
function processNewCycle(cycle) {
    // Save the cycle to db
    Storage.storeCycle(cycle);
    // Update NodeList from cycle info
    updateNodeList(cycle);
    // Update cycleSenders
    console.log(`Processed cycle ${cycle.counter}`);
}
exports.processNewCycle = processNewCycle;
function updateNodeList(cycle) {
    //   Add joined nodes
    const joinedConsensors = Utils_1.safeParse([], cycle.joinedConsensors, `Error processing cycle ${cycle.counter}: failed to parse joinedConsensors`);
    NodeList.addNodes(...joinedConsensors);
    //   Remove removed nodes
    const removed = Utils_1.safeParse([], cycle.removed, `Error processing cycle ${cycle.counter}: failed to parse removed`);
    NodeList.removeNodes(...removed);
}
function addCycleSenders(...nodes) {
    for (const node of nodes) {
        if (cycleSenders.has(node.publicKey) === false) {
            cycleSenders.set(node.publicKey, node);
        }
    }
}
exports.addCycleSenders = addCycleSenders;
function removeCycleSenders(...publicKeys) {
    for (const key of publicKeys) {
        if (cycleSenders.has(key) === true) {
            cycleSenders.delete(key);
        }
    }
}
exports.removeCycleSenders = removeCycleSenders;
//# sourceMappingURL=Data.js.map