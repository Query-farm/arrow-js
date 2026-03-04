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
    makeData,
    RecordBatch,
    RecordBatchStreamWriter,
    Schema,
    Struct,
    tableFromIPC,
} from 'apache-arrow';

const testDataDir = resolve(process.cwd(), 'test/data');

describe('Zero-column RecordBatch numRows preservation', () => {

    describe('PyArrow interop', () => {

        test('should read PyArrow zero-column stream and preserve numRows', () => {
            const buffer = readFileSync(resolve(testDataDir, 'zero_column_batch.arrow'));
            const table = tableFromIPC(buffer);

            expect(table.numRows).toBe(100);
            expect(table.numCols).toBe(0);
            expect(table.batches).toHaveLength(1);
            expect(table.batches[0].numRows).toBe(100);
        });
    });

    describe('JS round-trip', () => {

        test('zero-column batch should round-trip through IPC stream writer', () => {
            const schema = new Schema([]);
            const data = makeData({
                type: new Struct([]),
                length: 100,
                nullCount: 0,
                children: [],
            });
            const batch = new RecordBatch(schema, data);
            expect(batch.numRows).toBe(100);
            expect(batch.numCols).toBe(0);

            const writer = new RecordBatchStreamWriter();
            writer.write(batch);
            writer.finish();
            const buffer = writer.toUint8Array(true);

            const table = tableFromIPC(buffer);
            expect(table.numRows).toBe(100);
            expect(table.numCols).toBe(0);
            expect(table.batches).toHaveLength(1);
            expect(table.batches[0].numRows).toBe(100);
        });
    });

    describe('Direct constructor', () => {

        test('RecordBatch constructor preserves length for zero-column data', () => {
            const schema = new Schema([]);
            const data = makeData({
                type: new Struct([]),
                length: 100,
                nullCount: 0,
                children: [],
            });
            const batch = new RecordBatch(schema, data);

            expect(batch.numRows).toBe(100);
            expect(batch.numCols).toBe(0);
        });
    });
});
