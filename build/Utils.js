"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
function safeParse(fallback, json, msg) {
    try {
        return JSON.parse(json);
    }
    catch (err) {
        console.warn(msg ? msg : err);
        return fallback;
    }
}
exports.safeParse = safeParse;
//# sourceMappingURL=Utils.js.map