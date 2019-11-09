"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
const Config_1 = require("./Config");
const Crypto_1 = require("./Crypto");
const list = [];
function isFirst() {
    return list.length <= 0;
}
exports.isFirst = isFirst;
function addNode(node) {
    list.push(node);
}
exports.addNode = addNode;
function getSignedList() {
    const signedList = {
        nodeList: list,
    };
    Crypto_1.crypto.signObj(signedList, Config_1.config.ARCHIVER_SECRET_KEY, Config_1.config.ARCHIVER_PUBLIC_KEY);
    return signedList;
}
exports.getSignedList = getSignedList;
//# sourceMappingURL=NodeList.js.map