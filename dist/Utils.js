"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.calculatePatch = exports.objectEquals = exports.arrayEquals = exports.deepClone = void 0;
function normalizeJsonKey(key) {
    return key
        .replace(new RegExp("~", 'g'), "~0")
        .replace(new RegExp("/", 'g'), "~1");
}
/**
 * Simple deep clone handling just primitive types, objects and arrays
 *
 * @param input
 */
function deepClone(input) {
    if (input != null && typeof (input) === "object") {
        if (Array.isArray(input)) {
            return input.map(deepClone);
        }
        else {
            const output = {};
            for (let key in input) {
                output[key] = deepClone(input[key]);
            }
            return output;
        }
    }
    else {
        return input;
    }
}
exports.deepClone = deepClone;
/**
 * Checks equality between two arrays, considers just primitive types, objects and arrays
 *
 * @param oldArray
 * @param newArray
 */
function arrayEquals(oldArray, newArray) {
    if (oldArray.length !== newArray.length)
        return false;
    return oldArray.every((value, index) => objectEquals(newArray[index], value));
}
exports.arrayEquals = arrayEquals;
/**
 * Checks equality between two objects, considers just primitive types, objects and arrays
 *
 * @remarks `null` and `undefined` values are equivalent
 *
 * @param oldObject
 * @param newObject
 */
function objectEquals(oldObject, newObject) {
    oldObject ??= null;
    newObject ??= null;
    if (oldObject == null || newObject == null)
        return oldObject === newObject;
    if (typeof (oldObject) === "object" && typeof (newObject) === "object") {
        if (Array.isArray(oldObject) && Array.isArray(newObject)) {
            return arrayEquals(oldObject, newObject);
        }
        else if (!Array.isArray(oldObject) && !Array.isArray(newObject)) {
            for (let key in newObject) {
                if (!objectEquals(newObject[key], oldObject[key])) {
                    return false;
                }
            }
            for (let key in oldObject) {
                if (!objectEquals(newObject[key], oldObject[key])) {
                    return false;
                }
            }
            return true;
        }
        else {
            return false;
        }
    }
    else if (typeof (oldObject) !== "object" && typeof (newObject) !== "object") {
        return oldObject === newObject;
    }
    else {
        return false;
    }
}
exports.objectEquals = objectEquals;
/**
 * Calculates the CosmosDB patch between an old and a new object
 *
 * @param oldObject
 * @param newObject
 * @param maxLevel Maximum patch depth
 * @param rootPath
 * @param level
 */
function calculatePatch(oldObject, newObject, maxLevel = Infinity, rootPath = "/", level = 0) {
    oldObject ??= null;
    newObject ??= null;
    if (oldObject == null && newObject != null) {
        return [{
                op: "add",
                value: newObject,
                path: rootPath
            }];
    }
    if (oldObject != null && newObject == null) {
        return [{
                op: "add",
                value: newObject,
                path: rootPath
            }];
    }
    if (oldObject == null && newObject == null) {
        return [];
    }
    if (typeof (oldObject) === "object" && typeof (newObject) === "object") {
        if (Array.isArray(oldObject) && Array.isArray(newObject)) {
            // Replace arrays verbatim
            if (!arrayEquals(oldObject, newObject))
                return [{
                        op: "add",
                        value: newObject,
                        path: rootPath
                    }];
            return [];
        }
        else if (!Array.isArray(oldObject) && !Array.isArray(newObject)) {
            if (level === maxLevel) {
                if (!objectEquals(oldObject, newObject))
                    return [{
                            op: "add",
                            value: newObject,
                            path: rootPath
                        }];
                return [];
            }
            const ops = [];
            for (let key in newObject) {
                const newValue = newObject[key];
                const oldValue = oldObject[key];
                const patches = calculatePatch(oldValue, newValue, maxLevel, (rootPath === "/" ? rootPath : rootPath + "/") + normalizeJsonKey(key), level + 1);
                ops.push(...patches);
            }
            for (let key in oldObject) {
                if (Object.prototype.hasOwnProperty.call(newObject, key))
                    continue;
                const newValue = newObject[key];
                const oldValue = oldObject[key];
                const patches = calculatePatch(oldValue, newValue, maxLevel, (rootPath === "/" ? rootPath : rootPath + "/") + normalizeJsonKey(key), level + 1);
                ops.push(...patches);
            }
            return ops;
        }
        else {
            return [{
                    op: "add",
                    value: newObject,
                    path: rootPath
                }];
        }
    }
    else if (typeof (oldObject) !== "object" && typeof (newObject) !== "object") {
        if (oldObject !== newObject)
            return [{
                    op: "add",
                    value: newObject,
                    path: rootPath
                }];
        return [];
    }
    else {
        return [{
                op: "add",
                value: newObject,
                path: rootPath
            }];
    }
}
exports.calculatePatch = calculatePatch;
// console.log(calculatePatch({hello: "world"}, {hello: "world1"}));
// console.log(calculatePatch({hello: {world: [1, 2, 3]}}, {hello: {world: [1, 2, 3]}}));
// console.log(calculatePatch({hello: {world: [1, 2, 3]}}, {hello: {world: [2, 2, 3]}}));
// console.log(calculatePatch({hello: [1, 2, 3]}, {hello: {world: [2, 2, 3]}}));
// console.log(calculatePatch({hello: {world: undefined}}, {hello: {world: null}}))
// console.log(calculatePatch({hello: {world: undefined}}, {hello: {world: undefined}}));
// console.log(calculatePatch({hello: {world: null}}, {hello: {world: null}}));
// console.log(calculatePatch({hello: {world: null}}, {hello: {world: undefined}}));
