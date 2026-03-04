"""Generate a zero-column IPC stream with 100 rows for testing."""

import pyarrow as pa
import pyarrow.ipc as ipc

# Create a zero-column RecordBatch with 100 rows by creating a batch
# with a dummy column and then dropping it.
schema_with_col = pa.schema([("_dummy", pa.int32())])
batch_with_col = pa.RecordBatch.from_arrays(
    [pa.array([0] * 100, type=pa.int32())], schema=schema_with_col
)
batch = batch_with_col.drop_columns(["_dummy"])
assert batch.num_rows == 100
assert batch.num_columns == 0

with open("zero_column_batch.arrow", "wb") as f:
    writer = ipc.new_stream(f, batch.schema)
    writer.write_batch(batch)
    writer.close()
