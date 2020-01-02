"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
const Storage = require("../Storage");
const NodeList = require("../NodeList");
const Utils_1 = require("../Utils");
exports.currentCycleDuration = 0;
exports.currentCycleCounter = 0;
function processCycles(cycles) {
    for (const cycle of cycles) {
        // Skip if already processed [TODO] make this check more secure
        if (exports.currentCycleCounter > 0 && cycle.counter <= exports.currentCycleCounter) {
            continue;
        }
        // Save the cycle to db
        Storage.storeCycle(cycle);
        // Update NodeList from cycle info
        updateNodeList(cycle);
        // Update currentCycle state
        exports.currentCycleDuration = cycle.duration * 1000;
        exports.currentCycleCounter = cycle.counter;
        console.log(`Processed cycle ${cycle.counter}`);
    }
}
exports.processCycles = processCycles;
function updateNodeList(cycle) {
    // Add joined nodes
    const joinedConsensors = Utils_1.safeParse([], cycle.joinedConsensors, `Error processing cycle ${cycle.counter}: failed to parse joinedConsensors`);
    NodeList.addNodes(NodeList.Statuses.SYNCING, cycle.marker, ...joinedConsensors);
    // Update activated nodes
    const activatedPublicKeys = Utils_1.safeParse([], cycle.activatedPublicKeys, `Error processing cycle ${cycle.counter}: failed to parse activated`);
    NodeList.setStatus(NodeList.Statuses.ACTIVE, ...activatedPublicKeys);
    NodeList.addNodeId(...activatedPublicKeys);
    // Remove removed nodes
    const removed = Utils_1.safeParse([], cycle.removed, `Error processing cycle ${cycle.counter}: failed to parse removed`);
    NodeList.removeNodes(...removed.map(id => NodeList.getNodeInfoById(id).publicKey));
    // Remove lost nodes
    const lost = Utils_1.safeParse([], cycle.lost, `Error processing cycle ${cycle.counter}: failed to parse lost`);
    NodeList.removeNodes(...lost.map(id => NodeList.getNodeInfoById(id).publicKey));
    // Remove apoptosized nodes
    const apoptosized = Utils_1.safeParse([], cycle.lost, `Error processing cycle ${cycle.counter}: failed to parse apoptosized`);
    NodeList.removeNodes(...apoptosized.map(id => NodeList.getNodeInfoById(id).publicKey));
}
//# sourceMappingURL=Cycles.js.map