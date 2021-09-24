export const makeShortHash = (x: any, n = 4) => {
    if (!x) {
        return x
    }
    if (x.length > 63) {
        if (x.length === 64) {
            return x.slice(0, n) + 'x' + x.slice(63 - n)
        } else if (x.length === 128) {
            return x.slice(0, n) + 'xx' + x.slice(127 - n)
        } else if (x.length === 192) {
            return x.slice(0, n) + 'xx' + x.slice(191 - n)
        }
    }
    return x
}

const objToString = Object.prototype.toString
const objKeys =
    ((obj: any) => {
        const keys = []
        // tslint:disable-next-line: forin
        for (const name in obj) {
            keys.push(name)
        }
        return keys
    }) || Object.keys

export const stringifyReduce = (val: any, isArrayProp?: boolean): any => {
    let i, max, str, keys, key, propVal, toStr
    if (val === true) {
        return 'true'
    }
    if (val === false) {
        return 'false'
    }
    switch (typeof val) {
        case 'object':
            if (val === null) {
                return null
            } else if (val.toJSON && typeof val.toJSON === 'function') {
                return stringifyReduce(val.toJSON(), isArrayProp)
            } else if (val instanceof Map) {
                // let mapContainer = {stringifyReduce_map_2_array:[...val.entries()]}
                // return stringifyReduce(mapContainer)
                let mapContainer = {
                    dataType: 'stringifyReduce_map_2_array',
                    value: Array.from(val.entries()), // or with spread: value: [...originalObject]
                }
                return stringifyReduce(mapContainer)
            } else {
                toStr = objToString.call(val)
                if (toStr === '[object Array]') {
                    str = '['
                    max = val.length - 1
                    for (i = 0; i < max; i++) {
                        str += stringifyReduce(val[i], true) + ','
                    }
                    if (max > -1) {
                        str += stringifyReduce(val[i], true)
                    }
                    return str + ']'
                } else if (toStr === '[object Object]') {
                    // only object is left
                    keys = objKeys(val).sort()
                    max = keys.length
                    str = ''
                    i = 0
                    while (i < max) {
                        key = keys[i]
                        propVal = stringifyReduce(val[key], false)
                        if (propVal !== undefined) {
                            if (str) {
                                str += ','
                            }
                            str += JSON.stringify(key) + ':' + propVal
                        }
                        i++
                    }
                    return '{' + str + '}'
                } else {
                    return JSON.stringify(val)
                }
            }
        case 'function':
        case 'undefined':
            return isArrayProp ? null : undefined
        case 'string':
            const reduced = makeShortHash(val)
            return JSON.stringify(reduced)
        default:
            return isFinite(val) ? val : null
    }
}

export const replacer = (key: any, value: any) => {
    const originalObject = value // this[key]
    if (originalObject instanceof Map) {
        return {
            dataType: 'stringifyReduce_map_2_array',
            value: Array.from(originalObject.entries()), // or with spread: value: [...originalObject]
        }
    } else {
        return value
    }
}

export const reviver = (key: any, value: any) => {
    if (typeof value === 'object' && value !== null) {
        if (value.dataType === 'stringifyReduce_map_2_array') {
            return new Map(value.value)
        }
    }
    return value
}

export const reviverExpander = (key: any, value: any) => {
    if (typeof value === 'object' && value !== null) {
        if (value.dataType === 'stringifyReduce_map_2_array') {
            return new Map(value.value)
        }
    }
    if (typeof value === 'string' && value.length === 10 && value[4] === 'x') {
        let res = value.slice(0, 4) + '0'.repeat(55) + value.slice(5, 5 + 5)
        return res
    }
    return value
}
