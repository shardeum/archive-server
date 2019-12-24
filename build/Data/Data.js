"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
const Crypto = require("../Crypto");
const NodeList = require("../NodeList");
const P2P_1 = require("../P2P");
const Cycles = require("./Cycles");
const NewDataJson = require("./schemas/NewData.json");
const DataResponseJson = require("./schemas/DataResponse.json");
var DataTypes;
(function (DataTypes) {
    DataTypes["CYCLE"] = "cycle";
    DataTypes["TRANSACTION"] = "transaction";
    DataTypes["PARTITION"] = "partition";
    DataTypes["ALL"] = "all";
})(DataTypes = exports.DataTypes || (exports.DataTypes = {}));
const dataSenders = new Map();
const timeoutPadding = 5000;
/**
 * Sets timeout to current cycle duration + some padding
 * Removes sender from dataSenders on timeout
 * Select a new dataSender
 */
function removeOnTimeout(publicKey) {
    return setTimeout(() => {
        removeDataSenders(publicKey);
        selectNewDataSender();
    }, Cycles.currentCycleDuration + timeoutPadding);
}
function addDataSenders(...senders) {
    for (const sender of senders) {
        dataSenders.set(sender.nodeInfo.publicKey, sender);
    }
}
exports.addDataSenders = addDataSenders;
function removeDataSenders(...publicKeys) {
    for (const key of publicKeys) {
        dataSenders.delete(key);
    }
}
function selectNewDataSender() {
    // Randomly pick an active node
    const activeList = NodeList.getActiveList();
    const newSender = activeList[Math.floor(Math.random() * activeList.length)];
    // Add it to dataSenders
    addDataSenders({
        nodeInfo: newSender,
        type: DataTypes.CYCLE,
        contactTimeout: removeOnTimeout(newSender.publicKey),
    });
    //  Send it a DataRequest
    const request = {
        type: DataTypes.CYCLE,
        lastData: Cycles.currentCycleCounter,
    };
    Crypto.tag(request, newSender.publicKey);
    P2P_1.postJson(`http://${newSender.ip}:${newSender.port}/requestdata`, request);
}
// Data endpoints
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
        case DataTypes.CYCLE: {
            // Process cycles
            Cycles.processCycles(newData.data);
            break;
        }
        case DataTypes.TRANSACTION: {
            // [TODO] process transactions
            break;
        }
        case DataTypes.PARTITION: {
            // [TODO] process partitions
            break;
        }
        default: {
            // If data type not recognized, remove sender from dataSenders
            removeDataSenders(newData.publicKey);
        }
    }
    // Set new contactTimeout for sender
    sender.contactTimeout = removeOnTimeout(sender.nodeInfo.publicKey);
}
exports.routePostNewdata = {
    method: 'POST',
    url: '/newdata',
    // Compile json-schemas from types and add for validation + performance
    schema: {
        body: NewDataJson,
        response: DataResponseJson,
    },
    handler: (request, reply) => {
        const newData = request.body;
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
        if (sender.type !== DataTypes.ALL) {
            if (sender.type !== newData.type) {
                resp.keepAlive = false;
                Crypto.tag(resp, newData.publicKey);
                reply.send(resp);
                return;
            }
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