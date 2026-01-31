/**
 * Parquet VARIANT binary encoder
 *
 * Implements the Variant encoding spec:
 * https://github.com/apache/parquet-format/blob/master/VariantEncoding.md
 *
 * A Variant consists of:
 * - metadata: binary field containing the string dictionary
 * - value: binary field containing the self-describing encoded value
 */

const encoder = new TextEncoder()

/**
 * Encode a JavaScript value as a Parquet VARIANT.
 *
 * @param {any} value - The value to encode (object, array, primitive)
 * @returns {{ metadata: Uint8Array, value: Uint8Array }}
 */
export function encodeVariant(value) {
  // Build string dictionary from all object keys
  /** @type {string[]} */
  const dictionary = []
  /** @type {Map<string, number>} */
  const dictIndex = new Map()
  collectStrings(value, dictionary, dictIndex)

  // Encode metadata (dictionary)
  const metadata = encodeMetadata(dictionary)

  // Encode value
  const encodedValue = encodeValue(value, dictIndex)

  return { metadata, value: encodedValue }
}

/**
 * Recursively collect all object keys into the dictionary.
 *
 * @param {any} value
 * @param {string[]} dictionary
 * @param {Map<string, number>} dictIndex
 */
function collectStrings(value, dictionary, dictIndex) {
  if (value === null || value === undefined) return

  if (Array.isArray(value)) {
    for (const item of value) {
      collectStrings(item, dictionary, dictIndex)
    }
  } else if (typeof value === 'object') {
    for (const key of Object.keys(value)) {
      if (!dictIndex.has(key)) {
        dictIndex.set(key, dictionary.length)
        dictionary.push(key)
      }
      collectStrings(value[key], dictionary, dictIndex)
    }
  }
}

/**
 * Encode the metadata (string dictionary).
 *
 * Format:
 * - header: 1 byte (version=1, sorted=0, offset_size)
 * - dictionary_size: offset_size bytes
 * - offsets: (dictionary_size + 1) * offset_size bytes
 * - strings: concatenated UTF-8 strings
 *
 * @param {string[]} dictionary
 * @returns {Uint8Array}
 */
function encodeMetadata(dictionary) {
  if (dictionary.length === 0) {
    // Empty dictionary: just header + size
    return new Uint8Array([0x01, 0x00]) // version=1, sorted=0, offset_size=1, dict_size=0
  }

  // Encode all strings
  const encodedStrings = dictionary.map(s => encoder.encode(s))
  const totalStringBytes = encodedStrings.reduce((sum, s) => sum + s.length, 0)

  // Determine offset size needed
  const offsetSize = totalStringBytes <= 255 ? 1 : totalStringBytes <= 65535 ? 2 : 4

  // Calculate total size
  const headerSize = 1
  const dictSizeSize = offsetSize
  const offsetsSize = (dictionary.length + 1) * offsetSize
  const totalSize = headerSize + dictSizeSize + offsetsSize + totalStringBytes

  const buffer = new Uint8Array(totalSize)
  let pos = 0

  // Header: version=1 (4 bits), sorted=0 (1 bit), offset_size_minus_one (2 bits)
  const header = 0x01 | ((offsetSize - 1) << 6)
  buffer[pos++] = header

  // Dictionary size
  writeUnsigned(buffer, pos, dictionary.length, offsetSize)
  pos += offsetSize

  // Offsets
  let stringOffset = 0
  for (let i = 0; i <= dictionary.length; i++) {
    writeUnsigned(buffer, pos, stringOffset, offsetSize)
    pos += offsetSize
    if (i < dictionary.length) {
      stringOffset += encodedStrings[i].length
    }
  }

  // Strings
  for (const encoded of encodedStrings) {
    buffer.set(encoded, pos)
    pos += encoded.length
  }

  return buffer
}

/**
 * Encode a value using the Variant binary format.
 *
 * @param {any} value
 * @param {Map<string, number>} dictIndex
 * @returns {Uint8Array}
 */
function encodeValue(value, dictIndex) {
  // Null
  if (value === null || value === undefined) {
    return new Uint8Array([0x00]) // basic_type=0 (primitive), type_id=0 (null)
  }

  // Boolean
  if (typeof value === 'boolean') {
    return new Uint8Array([value ? 0x04 : 0x08]) // type_id 1=true, 2=false
  }

  // Number
  if (typeof value === 'number') {
    if (Number.isInteger(value)) {
      return encodeInteger(value)
    } else {
      return encodeDouble(value)
    }
  }

  // BigInt
  if (typeof value === 'bigint') {
    return encodeBigInt(value)
  }

  // String
  if (typeof value === 'string') {
    return encodeString(value)
  }

  // Date
  if (value instanceof Date) {
    return encodeTimestamp(value)
  }

  // Array
  if (Array.isArray(value)) {
    return encodeArray(value, dictIndex)
  }

  // Object
  if (typeof value === 'object') {
    return encodeObject(value, dictIndex)
  }

  // Fallback: encode as string
  return encodeString(String(value))
}

/**
 * Encode an integer value.
 * Uses the smallest integer type that fits.
 *
 * @param {number} value
 * @returns {Uint8Array}
 */
function encodeInteger(value) {
  if (value >= -128 && value <= 127) {
    // INT8
    const buf = new Uint8Array(2)
    buf[0] = 0x0C // basic_type=0, type_id=3
    buf[1] = value & 0xFF
    return buf
  }
  if (value >= -32768 && value <= 32767) {
    // INT16
    const buf = new Uint8Array(3)
    buf[0] = 0x10 // basic_type=0, type_id=4
    buf[1] = value & 0xFF
    buf[2] = (value >> 8) & 0xFF
    return buf
  }
  if (value >= -2147483648 && value <= 2147483647) {
    // INT32
    const buf = new Uint8Array(5)
    buf[0] = 0x14 // basic_type=0, type_id=5
    const view = new DataView(buf.buffer)
    view.setInt32(1, value, true)
    return buf
  }
  // INT64
  return encodeBigInt(BigInt(value))
}

/**
 * Encode a BigInt as INT64.
 *
 * @param {bigint} value
 * @returns {Uint8Array}
 */
function encodeBigInt(value) {
  const buf = new Uint8Array(9)
  buf[0] = 0x18 // basic_type=0, type_id=6
  const view = new DataView(buf.buffer)
  view.setBigInt64(1, value, true)
  return buf
}

/**
 * Encode a double value.
 *
 * @param {number} value
 * @returns {Uint8Array}
 */
function encodeDouble(value) {
  const buf = new Uint8Array(9)
  buf[0] = 0x1C // basic_type=0, type_id=7
  const view = new DataView(buf.buffer)
  view.setFloat64(1, value, true)
  return buf
}

/**
 * Encode a string value.
 * Uses short string format if < 64 bytes, otherwise long string.
 *
 * @param {string} value
 * @returns {Uint8Array}
 */
function encodeString(value) {
  const encoded = encoder.encode(value)

  if (encoded.length < 64) {
    // Short string: basic_type=1, length in header
    const buf = new Uint8Array(1 + encoded.length)
    buf[0] = 0x01 | (encoded.length << 2) // basic_type=1, length in upper 6 bits
    buf.set(encoded, 1)
    return buf
  }

  // Long string: primitive type 16
  const buf = new Uint8Array(5 + encoded.length)
  buf[0] = 0x40 // basic_type=0, type_id=16
  const view = new DataView(buf.buffer)
  view.setUint32(1, encoded.length, true)
  buf.set(encoded, 5)
  return buf
}

/**
 * Encode a Date as timestamp_micros (UTC).
 *
 * @param {Date} value
 * @returns {Uint8Array}
 */
function encodeTimestamp(value) {
  const micros = BigInt(value.getTime()) * 1000n
  const buf = new Uint8Array(9)
  buf[0] = 0x30 // basic_type=0, type_id=12 (timestamp_micros)
  const view = new DataView(buf.buffer)
  view.setBigInt64(1, micros, true)
  return buf
}

/**
 * Encode an array.
 *
 * @param {any[]} value
 * @param {Map<string, number>} dictIndex
 * @returns {Uint8Array}
 */
function encodeArray(value, dictIndex) {
  const numElements = value.length
  const isLarge = numElements > 255

  // Encode all elements
  const encodedElements = value.map(v => encodeValue(v, dictIndex))
  const totalValueBytes = encodedElements.reduce((sum, e) => sum + e.length, 0)

  // Determine offset size needed
  const offsetSize = totalValueBytes <= 255 ? 1 : totalValueBytes <= 65535 ? 2 : 4

  // Calculate sizes
  const headerSize = 1
  const numElementsSize = isLarge ? 4 : 1
  const offsetsSize = (numElements + 1) * offsetSize
  const totalSize = headerSize + numElementsSize + offsetsSize + totalValueBytes

  const buf = new Uint8Array(totalSize)
  let pos = 0

  // Header: basic_type=3 (array), offset_size, is_large
  const header = 0x03 | ((offsetSize - 1) << 2) | (isLarge ? 0x10 : 0)
  buf[pos++] = header

  // Number of elements
  if (isLarge) {
    const view = new DataView(buf.buffer)
    view.setUint32(pos, numElements, true)
    pos += 4
  } else {
    buf[pos++] = numElements
  }

  // Offsets
  let offset = 0
  for (let i = 0; i <= numElements; i++) {
    writeUnsigned(buf, pos, offset, offsetSize)
    pos += offsetSize
    if (i < numElements) {
      offset += encodedElements[i].length
    }
  }

  // Values
  for (const encoded of encodedElements) {
    buf.set(encoded, pos)
    pos += encoded.length
  }

  return buf
}

/**
 * Encode an object.
 *
 * @param {Record<string, any>} value
 * @param {Map<string, number>} dictIndex
 * @returns {Uint8Array}
 */
function encodeObject(value, dictIndex) {
  // Get keys sorted by their dictionary index (for sorted field order)
  const keys = Object.keys(value)
  const sortedKeys = [...keys].sort((a, b) => {
    const aName = dictIndex.get(a) ?? 0
    const bName = dictIndex.get(b) ?? 0
    return aName - bName
  })

  const numElements = sortedKeys.length
  const isLarge = numElements > 255

  // Encode all values
  const encodedValues = sortedKeys.map(k => encodeValue(value[k], dictIndex))
  const totalValueBytes = encodedValues.reduce((sum, e) => sum + e.length, 0)

  // Determine sizes needed
  const maxFieldId = Math.max(...sortedKeys.map(k => dictIndex.get(k) ?? 0), 0)
  const idSize = maxFieldId <= 255 ? 1 : maxFieldId <= 65535 ? 2 : 4
  const offsetSize = totalValueBytes <= 255 ? 1 : totalValueBytes <= 65535 ? 2 : 4

  // Calculate total size
  const headerSize = 1
  const numElementsSize = isLarge ? 4 : 1
  const fieldIdsSize = numElements * idSize
  const offsetsSize = (numElements + 1) * offsetSize
  const totalSize = headerSize + numElementsSize + fieldIdsSize + offsetsSize + totalValueBytes

  const buf = new Uint8Array(totalSize)
  let pos = 0

  // Header: basic_type=2 (object), offset_size, id_size, is_large
  const header = 0x02 | ((offsetSize - 1) << 2) | ((idSize - 1) << 4) | (isLarge ? 0x40 : 0)
  buf[pos++] = header

  // Number of elements
  if (isLarge) {
    const view = new DataView(buf.buffer)
    view.setUint32(pos, numElements, true)
    pos += 4
  } else {
    buf[pos++] = numElements
  }

  // Field IDs
  for (const key of sortedKeys) {
    const id = dictIndex.get(key) ?? 0
    writeUnsigned(buf, pos, id, idSize)
    pos += idSize
  }

  // Offsets
  let offset = 0
  for (let i = 0; i <= numElements; i++) {
    writeUnsigned(buf, pos, offset, offsetSize)
    pos += offsetSize
    if (i < numElements) {
      offset += encodedValues[i].length
    }
  }

  // Values
  for (const encoded of encodedValues) {
    buf.set(encoded, pos)
    pos += encoded.length
  }

  return buf
}

/**
 * Write an unsigned integer in little-endian format.
 *
 * @param {Uint8Array} buf
 * @param {number} pos
 * @param {number} value
 * @param {number} byteWidth
 */
function writeUnsigned(buf, pos, value, byteWidth) {
  for (let i = 0; i < byteWidth; i++) {
    buf[pos + i] = (value >> (i * 8)) & 0xFF
  }
}
