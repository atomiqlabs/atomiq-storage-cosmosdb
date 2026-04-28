import { PatchOperation } from "@azure/cosmos";
/**
 * Simple deep clone handling just primitive types, objects and arrays
 *
 * @param input
 */
export declare function deepClone<T>(input: T): T;
/**
 * Checks equality between two arrays, considers just primitive types, objects and arrays
 *
 * @param oldArray
 * @param newArray
 */
export declare function arrayEquals(oldArray: any[], newArray: any[]): boolean;
/**
 * Checks equality between two objects, considers just primitive types, objects and arrays
 *
 * @remarks `null` and `undefined` values are equivalent
 *
 * @param oldObject
 * @param newObject
 */
export declare function objectEquals(oldObject: any, newObject: any): boolean;
/**
 * Calculates the CosmosDB patch between an old and a new object
 *
 * @param oldObject
 * @param newObject
 * @param maxLevel Maximum patch depth
 * @param rootPath
 * @param level
 */
export declare function calculatePatch(oldObject: any, newObject: any, maxLevel?: number, rootPath?: string, level?: number): PatchOperation[];
