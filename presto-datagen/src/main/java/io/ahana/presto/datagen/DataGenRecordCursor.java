/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.ahana.presto.datagen;

import com.facebook.presto.common.type.Type;
import com.facebook.presto.spi.RecordCursor;
import io.ahana.presto.datagen.generator.ValueCursor;
import io.ahana.presto.datagen.generator.ValueCursorFactory;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.common.type.BooleanType.BOOLEAN;
import static com.facebook.presto.common.type.DoubleType.DOUBLE;
import static com.facebook.presto.common.type.VarcharType.createUnboundedVarcharType;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkPositionIndex;
import static java.util.Objects.requireNonNull;

public class DataGenRecordCursor
        implements RecordCursor
{
    private final List<ValueCursor> recordCursor;
    private long rowCount;

    public DataGenRecordCursor(
            List<DataGenColumnHandle> columnHandles,
            DataGenTableStats splitSpec)
    {
        requireNonNull(columnHandles, "columnHandles is null");
        requireNonNull(splitSpec, "splitSpec is null");

        this.rowCount = splitSpec.getRowCount();

        this.recordCursor = new ArrayList<ValueCursor>();

        ValueCursorFactory valueCursorFactory = new ValueCursorFactory();
        for (DataGenColumnHandle columnHandle : columnHandles) {
            Type columnType = columnHandle.getColumnType();
            Optional<DataGenColumnStats> columnSpec = splitSpec.getColumnStats(columnHandle.getColumnName());

            recordCursor.add(valueCursorFactory.create(columnType, columnSpec));
        }
    }

    @Override
    public long getCompletedBytes()
    {
        return 0;
    }

    @Override
    public long getReadTimeNanos()
    {
        return 0;
    }

    @Override
    public Type getType(int colIndex)
    {
        checkPositionIndex(colIndex, recordCursor.size(), "Invalid column index");
        return recordCursor.get(colIndex).getValueType();
    }

    @Override
    public boolean advanceNextPosition()
    {
        if (rowCount > 0) {
            rowCount--;
            for (ValueCursor valueCursor : recordCursor) {
                valueCursor.advanceNextPosition();
            }

            return true;
        }

        return false;
    }

    @Override
    public boolean isNull(int colIndex)
    {
        checkPositionIndex(colIndex, recordCursor.size(), "Invalid column index");
        return recordCursor.get(colIndex).isNull();
    }

    @Override
    public Object getObject(int colIndex)
    {
        checkPositionIndex(colIndex, recordCursor.size(), "Invalid column index");
        return recordCursor.get(colIndex).getValue();
    }

    @Override
    public boolean getBoolean(int colIndex)
    {
        checkFieldType(colIndex, BOOLEAN);
        return (Boolean) getObject(colIndex);
    }

    @Override
    public long getLong(int colIndex)
    {
        checkFieldType(colIndex, BIGINT);
        return (Long) getObject(colIndex);
    }

    @Override
    public double getDouble(int colIndex)
    {
        checkFieldType(colIndex, DOUBLE);
        return (Double) getObject(colIndex);
    }

    @Override
    public Slice getSlice(int colIndex)
    {
        checkFieldType(colIndex, createUnboundedVarcharType());
        return Slices.utf8Slice(getObject(colIndex).toString());
    }

    private void checkFieldType(int colIndex, Type expected)
    {
        Type actual = getType(colIndex);
        checkArgument(actual.equals(expected), "Expected column %s to be type %s but is %s", colIndex, expected, actual);
    }

    @Override
    public void close()
    {
    }
}
