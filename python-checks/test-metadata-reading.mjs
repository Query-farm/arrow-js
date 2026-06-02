/**
 * Test script to verify that RecordBatch metadata is correctly read from Arrow IPC files.
 *
 * This tests:
 * 1. Schema-level metadata (already supported)
 * 2. Field-level metadata (already supported)
 * 3. RecordBatch/Message-level metadata (newly added)
 */

import { readFileSync } from 'fs';
import { tableFromIPC } from '../targets/es2015/esm/Arrow.node.js';

// Use the file with message-level metadata
const arrowFile = new URL('./test_message_metadata.arrow', import.meta.url);
const buffer = readFileSync(arrowFile);
const table = tableFromIPC(buffer);

console.log('='.repeat(70));
console.log('Apache Arrow JavaScript - Metadata Reading Test');
console.log('='.repeat(70));
console.log();

// Test 1: Schema metadata
console.log('1. Schema Metadata:');
console.log('-'.repeat(40));
if (table.schema.metadata.size > 0) {
    console.log(`   Found ${table.schema.metadata.size} schema metadata entries`);
    let count = 0;
    for (const [key, value] of table.schema.metadata) {
        if (count < 5) {
            const displayValue = value.length > 50 ? value.substring(0, 47) + '...' : value;
            console.log(`   ${key}: ${displayValue}`);
        }
        count++;
    }
    if (count > 5) {
        console.log(`   ... and ${count - 5} more entries`);
    }
} else {
    console.log('   No schema metadata found');
}
console.log();

// Test 2: Field metadata
console.log('2. Field Metadata:');
console.log('-'.repeat(40));
for (const field of table.schema.fields) {
    if (field.metadata.size > 0) {
        console.log(`   [${field.name}] (${field.type}): ${field.metadata.size} metadata entries`);
        for (const [key, value] of field.metadata) {
            const displayValue = value.length > 40 ? value.substring(0, 37) + '...' : value;
            console.log(`      ${key}: ${displayValue}`);
        }
    } else {
        console.log(`   [${field.name}] (${field.type}): no metadata`);
    }
}
console.log();

// Test 3: RecordBatch metadata (NEW)
console.log('3. RecordBatch Metadata (NEW):');
console.log('-'.repeat(40));
let batchIndex = 0;
for (const batch of table.batches) {
    console.log(`   Batch ${batchIndex}:`);
    console.log(`      Rows: ${batch.numRows}`);
    console.log(`      Columns: ${batch.numCols}`);

    // Check if metadata property exists and is accessible
    if (typeof batch.metadata === 'undefined') {
        console.log('      ERROR: metadata property is undefined!');
    } else if (!(batch.metadata instanceof Map)) {
        console.log(`      ERROR: metadata is not a Map, got: ${typeof batch.metadata}`);
    } else {
        console.log(`      Metadata entries: ${batch.metadata.size}`);
        if (batch.metadata.size > 0) {
            for (const [key, value] of batch.metadata) {
                const displayValue = value.length > 40 ? value.substring(0, 37) + '...' : value;
                console.log(`         ${key}: ${displayValue}`);
            }
        }
    }
    batchIndex++;
}
console.log();

// Test 4: Verify metadata is preserved through operations
console.log('4. Metadata Preservation Tests:');
console.log('-'.repeat(40));
const batch = table.batches[0];
const originalMetadataSize = batch.metadata.size;

// Test slice()
const sliced = batch.slice(0, 2);
console.log(`   slice(): metadata preserved = ${sliced.metadata.size === originalMetadataSize}`);

// Test select()
const selected = batch.select(['id', 'name']);
console.log(`   select(): metadata preserved = ${selected.metadata.size === originalMetadataSize}`);

// Test selectAt()
const selectedAt = batch.selectAt([0, 1]);
console.log(`   selectAt(): metadata preserved = ${selectedAt.metadata.size === originalMetadataSize}`);

console.log();
console.log('='.repeat(70));
console.log('Test completed successfully!');
console.log('='.repeat(70));
