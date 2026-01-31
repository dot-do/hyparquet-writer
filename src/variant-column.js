/**
 * VARIANT column helper for Parquet
 *
 * Creates the proper schema and column data for VARIANT type columns.
 * VARIANT stores semi-structured data (like JSON) in an efficient binary format.
 *
 * Usage:
 *   const { schema, columnData } = createVariantColumn('event', events)
 *   // Add to your other columns and write
 */

import { encodeVariant } from './variant.js'

/**
 * Create a VARIANT column from an array of JavaScript values.
 *
 * The VARIANT type in Parquet is a group with:
 * - metadata: binary (string dictionary)
 * - value: binary (self-describing encoded value)
 *
 * @param {string} name - Column name
 * @param {any[]} values - Array of JavaScript values (objects, arrays, primitives)
 * @param {object} [options]
 * @param {boolean} [options.nullable=true] - Whether the column can contain nulls
 * @returns {{ schema: import('hyparquet').SchemaElement[], data: { metadata: Uint8Array, value: Uint8Array }[] }}
 */
export function createVariantColumn(name, values, options = {}) {
  const { nullable = true } = options

  // Encode each value as VARIANT
  const encodedValues = values.map(v => {
    if (v === null || v === undefined) {
      // Null variant: empty metadata, null value byte
      return {
        metadata: new Uint8Array([0x01, 0x00]), // version=1, empty dict
        value: new Uint8Array([0x00]), // null primitive
      }
    }
    return encodeVariant(v)
  })

  // Create schema for VARIANT group
  /** @type {import('hyparquet').SchemaElement[]} */
  const schema = [
    /** @type {import('hyparquet').SchemaElement} */ ({
      name,
      repetition_type: nullable ? 'OPTIONAL' : 'REQUIRED',
      num_children: 2,
      logical_type: { type: 'VARIANT' },
    }),
    /** @type {import('hyparquet').SchemaElement} */ ({
      name: 'metadata',
      type: 'BYTE_ARRAY',
      repetition_type: 'REQUIRED',
    }),
    /** @type {import('hyparquet').SchemaElement} */ ({
      name: 'value',
      type: 'BYTE_ARRAY',
      repetition_type: 'OPTIONAL',
    }),
  ]

  return {
    schema,
    data: encodedValues,
  }
}

/**
 * Encode a batch of values as VARIANT and return flat arrays for metadata and value.
 * This is useful when you want to manually construct the column data.
 *
 * @param {any[]} values
 * @returns {{ metadata: Uint8Array[], value: Uint8Array[] }}
 */
export function encodeVariantBatch(values) {
  const metadata = []
  const value = []

  for (const v of values) {
    if (v === null || v === undefined) {
      metadata.push(new Uint8Array([0x01, 0x00]))
      value.push(new Uint8Array([0x00]))
    } else {
      const encoded = encodeVariant(v)
      metadata.push(encoded.metadata)
      value.push(encoded.value)
    }
  }

  return { metadata, value }
}

/**
 * Get the VARIANT schema elements for a column.
 * Use this when building a custom schema.
 *
 * @param {string} name - Column name
 * @param {boolean} [nullable=true]
 * @returns {import('hyparquet').SchemaElement[]}
 */
export function getVariantSchema(name, nullable = true) {
  return [
    {
      name,
      repetition_type: nullable ? 'OPTIONAL' : 'REQUIRED',
      num_children: 2,
      logical_type: { type: 'VARIANT' },
    },
    {
      name: 'metadata',
      type: 'BYTE_ARRAY',
      repetition_type: 'REQUIRED',
    },
    {
      name: 'value',
      type: 'BYTE_ARRAY',
      repetition_type: 'OPTIONAL',
    },
  ]
}
