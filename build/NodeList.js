"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
const list = [];
function isEmpty() {
    return list.length <= 0;
}
exports.isEmpty = isEmpty;
function addNodes(...nodes) {
    list.push(...nodes);
}
exports.addNodes = addNodes;
function removeNode(partial) {
    let node;
    for (let i = 0; i < list.length; i++) {
        node = list[i];
        // publicKey takes precedence
        if (partial.publicKey && partial.publicKey === node.publicKey) {
            list.splice(i, 1);
        }
        // next, ip && port
        else if (partial.ip &&
            partial.port &&
            partial.ip === node.ip &&
            partial.port === node.port) {
            list.splice(i, 1);
        }
    }
}
exports.removeNode = removeNode;
function getList() {
    return list;
}
exports.getList = getList;
//# sourceMappingURL=NodeList.js.map