// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

import '../../../jest-extensions.js';
import { readFileSync } from 'fs';
import { resolve } from 'path';
import {
    Bool,
    Field,
    Float64,
    Int32,
    makeData,
    MessageHeader,
    RecordBatch,
    RecordBatchStreamWriter,
    Schema,
    Struct,
    tableFromIPC,
    Utf8,
} from 'apache-arrow';

const pyarrowTestData = resolve(process.cwd(), 'test/data');

/**
 * Extract the bodyLength for each RecordBatch message from a raw IPC stream buffer.
 */
function extractRecordBatchBodyLengths(buf: Uint8Array): number[] {
    const view = new DataView(buf.buffer, buf.byteOffset, buf.byteLength);
    const bodyLengths: number[] = [];
    let offset = 0;

    while (offset + 8 <= buf.byteLength) {
        const continuation = view.getInt32(offset, true);
        const metadataLength = view.getInt32(offset + 4, true);

        if (continuation === -1 && metadataLength === 0) break;
        offset += 8;

        const fbBytes = buf.subarray(offset, offset + metadataLength);
        const fbView = new DataView(fbBytes.buffer, fbBytes.byteOffset, fbBytes.byteLength);

        const rootOff = fbView.getUint32(0, true);
        const vtableSoff = fbView.getInt32(rootOff, true);
        const vtableOff = rootOff - vtableSoff;
        const vtableLen = fbView.getUint16(vtableOff, true);
        const numFields = (vtableLen - 4) / 2;

        let headerType = 0;
        if (numFields > 1) {
            const fieldOff = fbView.getUint16(vtableOff + 4 + 1 * 2, true);
            if (fieldOff !== 0) {
                headerType = fbView.getUint8(rootOff + fieldOff);
            }
        }

        let bodyLength = 0;
        if (numFields > 3) {
            const fieldOff = fbView.getUint16(vtableOff + 4 + 3 * 2, true);
            if (fieldOff !== 0) {
                bodyLength = Number(fbView.getBigInt64(rootOff + fieldOff, true));
            }
        }

        if (headerType === MessageHeader.RecordBatch) {
            bodyLengths.push(bodyLength);
        }

        offset += metadataLength;
        if (bodyLength > 0) {
            offset += bodyLength;
        }
        offset = (offset + 7) & ~7;
    }

    return bodyLengths;
}

function createZeroRowBatch(): RecordBatch {
    const schema = new Schema([
        new Field('id', new Int32()),
        new Field('name', new Utf8()),
    ]);
    const idData = makeData({ type: new Int32(), data: new Int32Array(0) });
    const nameData = makeData({
        type: new Utf8(),
        data: new Uint8Array(0),
        valueOffsets: new Int32Array([0]),
    });
    const structData = makeData({
        type: new Struct(schema.fields),
        length: 0,
        nullCount: 0,
        children: [idData, nameData],
    });
    return new RecordBatch(schema, structData);
}

function createNonZeroRowBatch(): RecordBatch {
    const schema = new Schema([
        new Field('id', new Int32()),
        new Field('name', new Utf8()),
    ]);
    const idData = makeData({ type: new Int32(), data: new Int32Array([1, 2, 3]) });
    const nameData = makeData({
        type: new Utf8(),
        data: Buffer.from('foobarbaz'),
        valueOffsets: new Int32Array([0, 3, 6, 9]),
    });
    const structData = makeData({
        type: new Struct(schema.fields),
        length: 3,
        nullCount: 0,
        children: [idData, nameData],
    });
    return new RecordBatch(schema, structData);
}

describe('Zero-row RecordBatch IPC serialization', () => {

    describe('Arrow JS writer behavior', () => {

        test('zero-row batch should round-trip through stream writer', () => {
            const batch = createZeroRowBatch();
            expect(batch.numRows).toBe(0);

            const writer = new RecordBatchStreamWriter();
            writer.write(batch);
            writer.finish();
            const buffer = writer.toUint8Array(true);

            const table = tableFromIPC(buffer);
            expect(table.numRows).toBe(0);
            expect(table.numCols).toBe(2);
            expect(table.batches).toHaveLength(1);
            expect(table.schema.fields[0].name).toBe('id');
            expect(table.schema.fields[1].name).toBe('name');
        });

        test('zero-row batch writes non-zero bodyLength due to Utf8 offsets buffer', () => {
            const batch = createZeroRowBatch();

            const writer = new RecordBatchStreamWriter();
            writer.write(batch);
            writer.finish();
            const jsBuffer = writer.toUint8Array(true);

            const bodyLengths = extractRecordBatchBodyLengths(jsBuffer);
            expect(bodyLengths).toHaveLength(1);
            // Utf8 column has an offsets buffer of Int32Array([0]) = 4 bytes, padded to 8
            expect(bodyLengths[0]).toBe(8);
        });

        test('zero-row Int32-only batch writes zero bodyLength (no variable-length buffers)', () => {
            const schema = new Schema([new Field('x', new Int32())]);
            const data = makeData({ type: new Int32(), data: new Int32Array(0) });
            const structData = makeData({
                type: new Struct(schema.fields),
                length: 0,
                nullCount: 0,
                children: [data],
            });
            const batch = new RecordBatch(schema, structData);

            const writer = new RecordBatchStreamWriter();
            writer.write(batch);
            writer.finish();
            const jsBuffer = writer.toUint8Array(true);

            const bodyLengths = extractRecordBatchBodyLengths(jsBuffer);
            expect(bodyLengths).toHaveLength(1);
            // Int32 with 0 rows has 0-byte values buffer -> bodyLength = 0
            expect(bodyLengths[0]).toBe(0);
        });
    });

    describe('PyArrow interop - reading PyArrow-generated zero-row IPC streams', () => {

        test('should read PyArrow zero-row stream', () => {
            const pyarrowBuffer = readFileSync(resolve(pyarrowTestData, 'zero_row_stream.arrow'));
            const table = tableFromIPC(pyarrowBuffer);

            expect(table.numRows).toBe(0);
            expect(table.numCols).toBe(2);
            expect(table.batches).toHaveLength(1);
            expect(table.schema.fields[0].name).toBe('id');
            expect(table.schema.fields[0].type.toString()).toBe('Int32');
            expect(table.schema.fields[1].name).toBe('name');
            expect(table.schema.fields[1].type.toString()).toBe('Utf8');
        });

        test('PyArrow zero-row stream has bodyLength=8 (offsets buffer for Utf8)', () => {
            const pyarrowBuffer = readFileSync(resolve(pyarrowTestData, 'zero_row_stream.arrow'));

            const bodyLengths = extractRecordBatchBodyLengths(pyarrowBuffer);
            expect(bodyLengths).toHaveLength(1);
            expect(bodyLengths[0]).toBe(8);
        });

        test('should read PyArrow non-zero row stream', () => {
            const pyarrowBuffer = readFileSync(resolve(pyarrowTestData, 'nonzero_row_stream.arrow'));
            const table = tableFromIPC(pyarrowBuffer);

            expect(table.numRows).toBe(3);
            expect(table.numCols).toBe(2);
            expect(table.batches).toHaveLength(1);
            expect(table.toArray().map(row => ({
                id: row.id,
                name: row.name,
            }))).toEqual([
                { id: 1, name: 'foo' },
                { id: 2, name: 'bar' },
                { id: 3, name: 'baz' },
            ]);
        });

        test('should read PyArrow zero-row multi-type stream', () => {
            const pyarrowBuffer = readFileSync(resolve(pyarrowTestData, 'zero_row_multi_type_stream.arrow'));
            const table = tableFromIPC(pyarrowBuffer);

            expect(table.numRows).toBe(0);
            expect(table.numCols).toBe(4);
            expect(table.schema.fields[0].name).toBe('i32');
            expect(table.schema.fields[0].type.toString()).toBe('Int32');
            expect(table.schema.fields[1].name).toBe('f64');
            expect(table.schema.fields[1].type.toString()).toBe('Float64');
            expect(table.schema.fields[2].name).toBe('str');
            expect(table.schema.fields[2].type.toString()).toBe('Utf8');
            expect(table.schema.fields[3].name).toBe('bool');
            expect(table.schema.fields[3].type.toString()).toBe('Bool');
        });

        test('should read PyArrow mixed zero + non-zero row stream', () => {
            const pyarrowBuffer = readFileSync(resolve(pyarrowTestData, 'mixed_zero_nonzero_stream.arrow'));
            const table = tableFromIPC(pyarrowBuffer);

            expect(table.numRows).toBe(3);
            expect(table.numCols).toBe(2);
            expect(table.batches).toHaveLength(2);
            expect(table.batches[0].numRows).toBe(0);
            expect(table.batches[1].numRows).toBe(3);
        });
    });

    describe('Arrow JS vs PyArrow body size comparison', () => {

        test('Arrow JS and PyArrow produce the same bodyLength for zero-row batches with Utf8', () => {
            // Arrow JS
            const batch = createZeroRowBatch();
            const writer = new RecordBatchStreamWriter();
            writer.write(batch);
            writer.finish();
            const jsBuffer = writer.toUint8Array(true);

            // PyArrow
            const pyBuffer = readFileSync(resolve(pyarrowTestData, 'zero_row_stream.arrow'));

            const jsBodyLengths = extractRecordBatchBodyLengths(jsBuffer);
            const pyBodyLengths = extractRecordBatchBodyLengths(pyBuffer);

            // Both write 8 bytes: the Utf8 offsets buffer Int32Array([0]) padded to 8
            expect(jsBodyLengths[0]).toBe(pyBodyLengths[0]);
            expect(jsBodyLengths[0]).toBe(8);
        });

        test('both implementations produce readable tables with identical data for non-zero rows', () => {
            const batch = createNonZeroRowBatch();
            const writer = new RecordBatchStreamWriter();
            writer.write(batch);
            writer.finish();
            const jsBuffer = writer.toUint8Array(true);

            const pyBuffer = readFileSync(resolve(pyarrowTestData, 'nonzero_row_stream.arrow'));

            const jsTable = tableFromIPC(jsBuffer);
            const pyTable = tableFromIPC(pyBuffer);

            expect(jsTable.numRows).toBe(pyTable.numRows);
            expect(jsTable.numCols).toBe(pyTable.numCols);
            for (let i = 0; i < jsTable.numRows; i++) {
                expect(jsTable.get(i)!.id).toBe(pyTable.get(i)!.id);
                expect(jsTable.get(i)!.name).toBe(pyTable.get(i)!.name);
            }
        });
    });

    describe('Zero-row batch with various types', () => {

        test('zero-row Int32-only batch round-trips', () => {
            const schema = new Schema([new Field('x', new Int32())]);
            const data = makeData({ type: new Int32(), data: new Int32Array(0) });
            const structData = makeData({
                type: new Struct(schema.fields),
                length: 0,
                nullCount: 0,
                children: [data],
            });
            const batch = new RecordBatch(schema, structData);

            const writer = new RecordBatchStreamWriter();
            writer.write(batch);
            writer.finish();
            const buffer = writer.toUint8Array(true);

            const table = tableFromIPC(buffer);
            expect(table.numRows).toBe(0);
            expect(table.numCols).toBe(1);
            expect(table.schema.fields[0].type.toString()).toBe('Int32');
        });

        test('zero-row multi-type batch round-trips', () => {
            const schema = new Schema([
                new Field('i32', new Int32()),
                new Field('f64', new Float64()),
                new Field('str', new Utf8()),
                new Field('bool', new Bool()),
            ]);
            const i32Data = makeData({ type: new Int32(), data: new Int32Array(0) });
            const f64Data = makeData({ type: new Float64(), data: new Float64Array(0) });
            const strData = makeData({
                type: new Utf8(),
                data: new Uint8Array(0),
                valueOffsets: new Int32Array([0]),
            });
            const boolData = makeData({ type: new Bool(), data: new Uint8Array(0) });
            const structData = makeData({
                type: new Struct(schema.fields),
                length: 0,
                nullCount: 0,
                children: [i32Data, f64Data, strData, boolData],
            });
            const batch = new RecordBatch(schema, structData);

            const writer = new RecordBatchStreamWriter();
            writer.write(batch);
            writer.finish();
            const buffer = writer.toUint8Array(true);

            const table = tableFromIPC(buffer);
            expect(table.numRows).toBe(0);
            expect(table.numCols).toBe(4);
        });

        test('mixed zero-row and non-zero-row batches round-trip', () => {
            const zeroBatch = createZeroRowBatch();
            const nonzeroBatch = createNonZeroRowBatch();

            const writer = new RecordBatchStreamWriter();
            writer.write(zeroBatch);
            writer.write(nonzeroBatch);
            writer.finish();
            const buffer = writer.toUint8Array(true);

            const table = tableFromIPC(buffer);
            expect(table.numRows).toBe(3);
            expect(table.batches).toHaveLength(2);
            expect(table.batches[0].numRows).toBe(0);
            expect(table.batches[1].numRows).toBe(3);
        });
    });
});
