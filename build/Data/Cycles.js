"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
const Storage = require("../Storage");
const NodeList = require("../NodeList");
const Utils_1 = require("../Utils");
exports.currentCycleDuration = 0;
exports.currentCycleCounter = 0;
function processCycles(cycles) {
    // Process 10 cycles max on each call
    let cycle;
    for (let i = 0; i < 10; i++) {
        // Save the cycle to db
        cycle = cycles[i];
        Storage.storeCycle(cycle);
        // Update NodeList from cycle info
        updateNodeList(cycle);
        // Update currentCycle state
        exports.currentCycleDuration = cycle.duration;
        exports.currentCycleCounter = cycle.counter;
        console.log(`Processed cycle ${cycle.counter}`);
    }
}
exports.processCycles = processCycles;
function updateNodeList(cycle) {
    // Add joined nodes
    const joinedConsensors = Utils_1.safeParse([], cycle.joinedConsensors, `Error processing cycle ${cycle.counter}: failed to parse joinedConsensors`);
    NodeList.addNodes(NodeList.Statuses.SYNCING, ...joinedConsensors);
    // Update activated nodes
    const activated = Utils_1.safeParse([], cycle.activated, `Error processing cycle ${cycle.counter}: failed to parse activated`);
    NodeList.setStatus(NodeList.Statuses.ACTIVE, ...activated);
    // Remove removed nodes
    const removed = Utils_1.safeParse([], cycle.removed, `Error processing cycle ${cycle.counter}: failed to parse removed`);
    NodeList.removeNodes(...removed);
    // Remove lost nodes
    const lost = Utils_1.safeParse([], cycle.lost, `Error processing cycle ${cycle.counter}: failed to parse lost`);
    NodeList.removeNodes(...lost);
    // Remove apoptosized nodes
    const apoptosized = Utils_1.safeParse([], cycle.lost, `Error processing cycle ${cycle.counter}: failed to parse apoptosized`);
    NodeList.removeNodes(...apoptosized);
}
//# sourceMappingURL=Cycles.js.map