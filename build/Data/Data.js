"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
const events_1 = require("events");
const Crypto = require("../Crypto");
const NodeList = require("../NodeList");
const Cycles_1 = require("./Cycles");
var TypeNames;
(function (TypeNames) {
    TypeNames["CYCLE"] = "CYCLE";
    TypeNames["TRANSACTION"] = "TRANSACTION";
    TypeNames["PARTITION"] = "PARTITION";
})(TypeNames = exports.TypeNames || (exports.TypeNames = {}));
function createDataRequest(type, lastData, recipientPk) {
    return Crypto.tag({
        type,
        lastData,
    }, recipientPk);
}
exports.createDataRequest = createDataRequest;
const dataSenders = new Map();
const timeoutPadding = 1000;
exports.emitter = new events_1.EventEmitter();
function replaceDataSender(publicKey) {
    console.log(`replaceDataSender: replacing ${publicKey}`);
    // Remove old dataSender
    const removedSenders = removeDataSenders(publicKey);
    if (removedSenders.length < 1) {
        throw new Error('replaceDataSender failed: old sender not removed');
    }
    console.log(`replaceDataSender: removed old sender ${JSON.stringify(removedSenders, null, 2)}`);
    // Pick a new dataSender
    const newSenderInfo = selectNewDataSender();
    const newSender = {
        nodeInfo: newSenderInfo,
        type: TypeNames.CYCLE,
        contactTimeout: createContactTimeout(newSenderInfo.publicKey),
    };
    console.log(`replaceDataSender: selected new sender ${JSON.stringify(newSender.nodeInfo, null, 2)}`);
    // Add new dataSender to dataSenders
    addDataSenders(newSender);
    console.log(`replaceDataSender: added new sender ${newSenderInfo.publicKey} to dataSenders`);
    // Send dataRequest to new dataSender
    const dataRequest = {
        type: TypeNames.CYCLE,
        lastData: Cycles_1.currentCycleCounter,
    };
    sendDataRequest(newSender, dataRequest);
    console.log(`replaceDataSender: sent dataRequest to new sender: ${JSON.stringify(dataRequest, null, 2)}`);
}
/**
 * Sets timeout to current cycle duration + some padding
 * Removes sender from dataSenders on timeout
 * Select a new dataSender
 */
function createContactTimeout(publicKey) {
    const ms = Cycles_1.currentCycleDuration + timeoutPadding;
    const contactTimeout = setTimeout(replaceDataSender, ms, publicKey);
    console.log(`createContactTimeout: created timeout for ${publicKey} in ${ms} ms...`);
    return contactTimeout;
}
function addDataSenders(...senders) {
    for (const sender of senders) {
        dataSenders.set(sender.nodeInfo.publicKey, sender);
    }
}
exports.addDataSenders = addDataSenders;
function removeDataSenders(...publicKeys) {
    const removedSenders = [];
    for (const key of publicKeys) {
        const sender = dataSenders.get(key);
        if (sender) {
            // Clear contactTimeout associated with this sender
            if (sender.contactTimeout) {
                clearTimeout(sender.contactTimeout);
            }
            // Record which sender was removed
            removedSenders.push(sender);
            // Delete sender from dataSenders
            dataSenders.delete(key);
        }
    }
    return removedSenders;
}
function selectNewDataSender() {
    // Randomly pick an active node
    const activeList = NodeList.getActiveList();
    const newSender = activeList[Math.floor(Math.random() * activeList.length)];
    return newSender;
}
function sendDataRequest(sender, dataRequest) {
    const taggedDataRequest = Crypto.tag(dataRequest, sender.nodeInfo.publicKey);
    exports.emitter.emit('selectNewDataSender', sender.nodeInfo, taggedDataRequest);
}
function processData(newData) {
    // Get sender entry
    const sender = dataSenders.get(newData.publicKey);
    // If no sender entry, remove publicKey from senders, END
    if (!sender) {
        removeDataSenders(newData.publicKey);
        return;
    }
    // Clear senders contactTimeout, if it has one
    if (sender.contactTimeout) {
        clearTimeout(sender.contactTimeout);
    }
    // Process data depending on type
    switch (newData.type) {
        case TypeNames.CYCLE: {
            // Process cycles
            Cycles_1.processCycles(newData.data);
            break;
        }
        case TypeNames.TRANSACTION: {
            // [TODO] process transactions
            break;
        }
        case TypeNames.PARTITION: {
            // [TODO] process partitions
            break;
        }
        default: {
            // If data type not recognized, remove sender from dataSenders
            removeDataSenders(newData.publicKey);
        }
    }
    // Set new contactTimeout for sender
    if (Cycles_1.currentCycleDuration > 0) {
        sender.contactTimeout = createContactTimeout(sender.nodeInfo.publicKey);
    }
}
// Data endpoints
exports.routePostNewdata = {
    method: 'POST',
    url: '/newdata',
    // [TODO] Compile json-schemas from types and add for validation + performance
    handler: (request, reply) => {
        const newData = request.body;
        console.log('GOT NEWDATA', JSON.stringify(newData, null, 2));
        const resp = { keepAlive: true };
        // If publicKey is not in dataSenders, dont keepAlive, END
        const sender = dataSenders.get(newData.publicKey);
        if (!sender) {
            resp.keepAlive = false;
            Crypto.tag(resp, newData.publicKey);
            reply.send(resp);
            return;
        }
        // If unexpected data type from sender, dont keepAlive, END
        if (sender.type !== newData.type) {
            resp.keepAlive = false;
            Crypto.tag(resp, newData.publicKey);
            reply.send(resp);
            return;
        }
        // If tag is invalid, dont keepAlive, END
        if (Crypto.authenticate(newData) === false) {
            resp.keepAlive = false;
            Crypto.tag(resp, newData.publicKey);
            reply.send(resp);
            return;
        }
        // Reply with keepAlive and schedule data processing after I/O
        Crypto.tag(resp, newData.publicKey);
        reply.send(resp);
        setImmediate(processData, newData);
    },
};
//# sourceMappingURL=Data.js.map