/**
 * Parquet VARIANT Shredding Support
 *
 * Implements the Variant Shredding specification:
 * https://parquet.apache.org/docs/file-format/types/variantshredding/
 *
 * Shredding extracts specific fields from Variant values into typed Parquet
 * columns, enabling predicate pushdown and statistics-based row group skipping.
 *
 * Structure:
 *   optional group column_name (VARIANT) {
 *     required binary metadata;
 *     optional binary value;           // null when fully shredded
 *     optional group typed_value {
 *       optional group field_name {
 *         optional binary value;       // null for typed
 *         optional TYPE typed_value;   // statistics here!
 *       }
 *     }
 *   }
 */

import { encodeVariant } from './variant.js'

/**
 * Create a shredded VARIANT column schema and data.
 *
 * @param {string} name - Column name (e.g., '$index')
 * @param {any[]} values - Array of JavaScript objects
 * @param {string[]} shredFields - Fields to shred into typed columns
 * @param {object} [options]
 * @param {boolean} [options.nullable=true] - Whether the column can contain nulls
 * @param {Record<string, string>} [options.fieldTypes] - Override field types (default: auto-detect)
 * @returns {{
 *   schema: import('hyparquet').SchemaElement[],
 *   columnData: Map<string, any[]>,
 *   shredPaths: string[]
 * }}
 */
export function createShreddedVariantColumn(name, values, shredFields, options = {}) {
  const { nullable = true, fieldTypes = {} } = options

  // Detect types for shredded fields
  const detectedTypes = detectFieldTypes(values, shredFields, fieldTypes)

  // Build schema
  const schema = buildShreddedSchema(name, shredFields, detectedTypes, nullable)

  // Extract column data
  const columnData = extractColumnData(name, values, shredFields)

  // Return paths for statistics
  const shredPaths = shredFields.map(f => `${name}.typed_value.${f}.typed_value`)

  return { schema, columnData, shredPaths }
}

/**
 * Detect Parquet types for shredded fields based on values.
 *
 * @param {any[]} values
 * @param {string[]} fields
 * @param {Record<string, string>} overrides
 * @returns {Record<string, { parquetType: string, convertedType?: string }>}
 */
function detectFieldTypes(values, fields, overrides) {
  const types = {}

  for (const field of fields) {
    if (overrides[field]) {
      types[field] = parseTypeOverride(overrides[field])
      continue
    }

    // Sample values to detect type
    let detectedType = null
    for (const value of values) {
      if (value === null || value === undefined) continue
      const fieldValue = value[field]
      if (fieldValue === null || fieldValue === undefined) continue

      const valueType = detectValueType(fieldValue)
      if (detectedType === null) {
        detectedType = valueType
      } else if (detectedType.parquetType !== valueType.parquetType) {
        // Mixed types - fall back to BYTE_ARRAY/UTF8
        detectedType = { parquetType: 'BYTE_ARRAY', convertedType: 'UTF8' }
        break
      }
    }

    types[field] = detectedType || { parquetType: 'BYTE_ARRAY', convertedType: 'UTF8' }
  }

  return types
}

/**
 * Detect Parquet type for a single value.
 *
 * @param {any} value
 * @returns {{ parquetType: string, convertedType?: string }}
 */
function detectValueType(value) {
  if (typeof value === 'boolean') {
    return { parquetType: 'BOOLEAN' }
  }
  if (typeof value === 'number') {
    if (Number.isInteger(value)) {
      if (value >= -2147483648 && value <= 2147483647) {
        return { parquetType: 'INT32' }
      }
      return { parquetType: 'INT64' }
    }
    return { parquetType: 'DOUBLE' }
  }
  if (typeof value === 'bigint') {
    return { parquetType: 'INT64' }
  }
  if (typeof value === 'string') {
    return { parquetType: 'BYTE_ARRAY', convertedType: 'UTF8' }
  }
  if (value instanceof Date) {
    return { parquetType: 'INT64', convertedType: 'TIMESTAMP_MILLIS' }
  }
  // Default to string
  return { parquetType: 'BYTE_ARRAY', convertedType: 'UTF8' }
}

/**
 * Parse type override string.
 *
 * @param {string} typeStr - e.g., 'STRING', 'INT32', 'TIMESTAMP'
 * @returns {{ parquetType: string, convertedType?: string }}
 */
function parseTypeOverride(typeStr) {
  const upper = typeStr.toUpperCase()
  switch (upper) {
    case 'STRING':
    case 'UTF8':
      return { parquetType: 'BYTE_ARRAY', convertedType: 'UTF8' }
    case 'INT32':
    case 'INT':
      return { parquetType: 'INT32' }
    case 'INT64':
    case 'LONG':
      return { parquetType: 'INT64' }
    case 'FLOAT':
      return { parquetType: 'FLOAT' }
    case 'DOUBLE':
      return { parquetType: 'DOUBLE' }
    case 'BOOLEAN':
    case 'BOOL':
      return { parquetType: 'BOOLEAN' }
    case 'TIMESTAMP':
      return { parquetType: 'INT64', convertedType: 'TIMESTAMP_MILLIS' }
    default:
      return { parquetType: upper }
  }
}

/**
 * Build schema for shredded VARIANT column.
 *
 * @param {string} name
 * @param {string[]} shredFields
 * @param {Record<string, { parquetType: string, convertedType?: string }>} fieldTypes
 * @param {boolean} nullable
 * @returns {import('hyparquet').SchemaElement[]}
 */
function buildShreddedSchema(name, shredFields, fieldTypes, nullable) {
  const schema = []

  // Root VARIANT group
  // num_children = 3 (metadata, value, typed_value)
  schema.push({
    name,
    repetition_type: nullable ? 'OPTIONAL' : 'REQUIRED',
    num_children: 3,
    logical_type: { type: 'VARIANT' },
  })

  // metadata (required binary)
  schema.push({
    name: 'metadata',
    type: 'BYTE_ARRAY',
    repetition_type: 'REQUIRED',
  })

  // value (optional binary) - null when fully shredded
  schema.push({
    name: 'value',
    type: 'BYTE_ARRAY',
    repetition_type: 'OPTIONAL',
  })

  // typed_value group containing shredded fields
  schema.push({
    name: 'typed_value',
    repetition_type: 'OPTIONAL',
    num_children: shredFields.length,
  })

  // Each shredded field is a group with value + typed_value
  for (const field of shredFields) {
    const fieldType = fieldTypes[field]

    // Field group
    schema.push({
      name: field,
      repetition_type: 'OPTIONAL',
      num_children: 2,
    })

    // value (binary) - null for typed values
    schema.push({
      name: 'value',
      type: 'BYTE_ARRAY',
      repetition_type: 'OPTIONAL',
    })

    // typed_value - the actual typed column with statistics
    const typedValueSchema = {
      name: 'typed_value',
      type: fieldType.parquetType,
      repetition_type: 'OPTIONAL',
    }
    if (fieldType.convertedType) {
      typedValueSchema.converted_type = fieldType.convertedType
    }
    schema.push(typedValueSchema)
  }

  return schema
}

/**
 * Extract column data for shredded VARIANT.
 *
 * Returns a Map with paths to their data arrays:
 * - '{name}.metadata' -> Uint8Array[]
 * - '{name}.value' -> Uint8Array[] (nulls)
 * - '{name}.typed_value.{field}.value' -> Uint8Array[] (nulls)
 * - '{name}.typed_value.{field}.typed_value' -> typed values
 *
 * @param {string} name
 * @param {any[]} values
 * @param {string[]} shredFields
 * @returns {Map<string, any[]>}
 */
function extractColumnData(name, values, shredFields) {
  const columnData = new Map()

  const metadataPath = `${name}.metadata`
  const valuePath = `${name}.value`

  columnData.set(metadataPath, [])
  columnData.set(valuePath, [])

  // Initialize paths for each shredded field
  for (const field of shredFields) {
    columnData.set(`${name}.typed_value.${field}.value`, [])
    columnData.set(`${name}.typed_value.${field}.typed_value`, [])
  }

  // Process each value
  for (const value of values) {
    if (value === null || value === undefined) {
      // Null row
      columnData.get(metadataPath).push(new Uint8Array([0x01, 0x00]))
      columnData.get(valuePath).push(null)

      for (const field of shredFields) {
        columnData.get(`${name}.typed_value.${field}.value`).push(null)
        columnData.get(`${name}.typed_value.${field}.typed_value`).push(null)
      }
      continue
    }

    // Build object WITHOUT shredded fields for the value column
    const remaining = {}
    let hasRemaining = false
    for (const [k, v] of Object.entries(value)) {
      if (!shredFields.includes(k)) {
        remaining[k] = v
        hasRemaining = true
      }
    }

    // Encode metadata (full dictionary including shredded field names)
    const { metadata } = encodeVariant(value)
    columnData.get(metadataPath).push(metadata)

    // value column is null when ALL fields are shredded
    if (hasRemaining) {
      const { value: encodedValue } = encodeVariant(remaining)
      columnData.get(valuePath).push(encodedValue)
    } else {
      columnData.get(valuePath).push(null)
    }

    // Extract shredded field values
    for (const field of shredFields) {
      const fieldValue = value[field]

      // value subcolumn is always null (we use typed_value)
      columnData.get(`${name}.typed_value.${field}.value`).push(null)

      // typed_value has the actual value (enables statistics)
      if (fieldValue === null || fieldValue === undefined) {
        columnData.get(`${name}.typed_value.${field}.typed_value`).push(null)
      } else {
        // Convert to appropriate type
        columnData.get(`${name}.typed_value.${field}.typed_value`).push(
          convertValue(fieldValue)
        )
      }
    }
  }

  return columnData
}

/**
 * Convert value to appropriate type for Parquet.
 *
 * @param {any} value
 * @returns {any}
 */
function convertValue(value) {
  if (value instanceof Date) {
    return BigInt(value.getTime())
  }
  if (typeof value === 'object') {
    return JSON.stringify(value)
  }
  return value
}

/**
 * Get statistics paths for a shredded VARIANT column.
 * These are the column paths where min/max statistics are available.
 *
 * @param {string} name - Column name
 * @param {string[]} shredFields - Shredded field names
 * @returns {string[]}
 */
export function getStatisticsPaths(name, shredFields) {
  return shredFields.map(f => `${name}.typed_value.${f}.typed_value`)
}

/**
 * Map a user filter path to the Parquet statistics path.
 *
 * @param {string} filterPath - User filter path (e.g., '$index.titleType')
 * @param {string} columnName - VARIANT column name (e.g., '$index')
 * @param {string[]} shredFields - Shredded field names
 * @returns {string|null} - Parquet path or null if not shredded
 */
export function mapFilterPathToStats(filterPath, columnName, shredFields) {
  if (!filterPath.startsWith(columnName + '.')) return null

  const field = filterPath.slice(columnName.length + 1).split('.')[0]
  if (!shredFields.includes(field)) return null

  return `${columnName}.typed_value.${field}.typed_value`
}
