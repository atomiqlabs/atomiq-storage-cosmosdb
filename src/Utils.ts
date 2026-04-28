import {PatchOperation} from "@azure/cosmos";

function normalizeJsonKey(key: string): string {
    return key
        .replace(new RegExp("~", 'g'), "~0")
        .replace(new RegExp("/", 'g'), "~1");
}

/**
 * Simple deep clone handling just primitive types, objects and arrays
 *
 * @param input
 */
export function deepClone<T>(input: T): T {
    if(input!=null && typeof(input)==="object") {
        if(Array.isArray(input)) {
            return input.map(deepClone) as T;
        } else {
            const output: any = {};
            for(let key in input) {
                output[key] = deepClone(input[key]);
            }
            return output as T;
        }
    } else {
        return input;
    }
}

/**
 * Checks equality between two arrays, considers just primitive types, objects and arrays
 *
 * @param oldArray
 * @param newArray
 */
export function arrayEquals(oldArray: any[], newArray: any[]): boolean {
    if(oldArray.length !== newArray.length) return false;
    return oldArray.every((value, index) => objectEquals(newArray[index], value));
}

/**
 * Checks equality between two objects, considers just primitive types, objects and arrays
 *
 * @remarks `null` and `undefined` values are equivalent
 *
 * @param oldObject
 * @param newObject
 */
export function objectEquals(oldObject: any, newObject: any): boolean {
    oldObject ??= null;
    newObject ??= null;
    if(oldObject==null || newObject==null) return oldObject===newObject;

    if(typeof(oldObject)==="object" && typeof(newObject)==="object") {
        if(Array.isArray(oldObject) && Array.isArray(newObject)) {
            return arrayEquals(oldObject, newObject);
        } else if(!Array.isArray(oldObject) && !Array.isArray(newObject)) {
            for(let key in newObject) {
                if(!objectEquals(newObject[key], oldObject[key])) {
                    return false;
                }
            }
            for(let key in oldObject) {
                if(!objectEquals(newObject[key], oldObject[key])) {
                    return false;
                }
            }
            return true;
        } else {
            return false;
        }
    } else if(typeof(oldObject)!=="object" && typeof(newObject)!=="object") {
        return oldObject === newObject;
    } else {
        return false;
    }
}

/**
 * Calculates the CosmosDB patch between an old and a new object
 *
 * @param oldObject
 * @param newObject
 * @param maxLevel Maximum patch depth
 * @param rootPath
 * @param level
 */
export function calculatePatch(oldObject: any, newObject: any, maxLevel: number = Infinity, rootPath: string = "/", level: number = 0): PatchOperation[] {
    oldObject ??= null;
    newObject ??= null;

    if(oldObject==null && newObject!=null) {
        return [{
            op: "add",
            value: newObject,
            path: rootPath
        }];
    }

    if(oldObject!=null && newObject==null) {
        return [{
            op: "add",
            value: newObject,
            path: rootPath
        }];
    }

    if(oldObject==null && newObject==null) {
        return [];
    }

    if(typeof(oldObject)==="object" && typeof(newObject)==="object") {
        if(Array.isArray(oldObject) && Array.isArray(newObject)) {
            // Replace arrays verbatim
            if(!arrayEquals(oldObject, newObject)) return [{
                op: "add",
                value: newObject,
                path: rootPath
            }];
            return [];
        } else if(!Array.isArray(oldObject) && !Array.isArray(newObject)) {
            if(level===maxLevel) {
                if(!objectEquals(oldObject, newObject)) return [{
                    op: "add",
                    value: newObject,
                    path: rootPath
                }];
                return [];
            }

            const ops: PatchOperation[] = [];
            for(let key in newObject) {
                const newValue = newObject[key];
                const oldValue = oldObject[key];

                const patches = calculatePatch(oldValue, newValue, maxLevel, (rootPath==="/" ? rootPath : rootPath+"/")+normalizeJsonKey(key), level+1);
                ops.push(...patches);
            }
            for(let key in oldObject) {
                if(Object.prototype.hasOwnProperty.call(newObject, key)) continue;

                const newValue = newObject[key];
                const oldValue = oldObject[key];
                const patches = calculatePatch(oldValue, newValue, maxLevel, (rootPath==="/" ? rootPath : rootPath+"/")+normalizeJsonKey(key), level+1);
                ops.push(...patches);
            }
            return ops;
        } else {
            return [{
                op: "add",
                value: newObject,
                path: rootPath
            }];
        }
    } else if(typeof(oldObject)!=="object" && typeof(newObject)!=="object") {
        if(oldObject!==newObject) return [{
            op: "add",
            value: newObject,
            path: rootPath
        }];
        return [];
    } else {
        return [{
            op: "add",
            value: newObject,
            path: rootPath
        }];
    }
}

// console.log(calculatePatch({hello: "world"}, {hello: "world1"}));
// console.log(calculatePatch({hello: {world: [1, 2, 3]}}, {hello: {world: [1, 2, 3]}}));
// console.log(calculatePatch({hello: {world: [1, 2, 3]}}, {hello: {world: [2, 2, 3]}}));
// console.log(calculatePatch({hello: [1, 2, 3]}, {hello: {world: [2, 2, 3]}}));
// console.log(calculatePatch({hello: {world: undefined}}, {hello: {world: null}}))
// console.log(calculatePatch({hello: {world: undefined}}, {hello: {world: undefined}}));
// console.log(calculatePatch({hello: {world: null}}, {hello: {world: null}}));
// console.log(calculatePatch({hello: {world: null}}, {hello: {world: undefined}}));
