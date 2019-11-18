"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
const list = [];
function isEmpty() {
    return list.length <= 0;
}
exports.isEmpty = isEmpty;
function addNode(node) {
    list.push(node);
}
exports.addNode = addNode;
function getList() {
    return list;
}
exports.getList = getList;
//# sourceMappingURL=NodeList.js.map