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

import com.facebook.presto.spi.RecordCursor;
import com.facebook.presto.spi.RecordSet;
import com.google.common.collect.ImmutableList;
import org.testng.annotations.Test;

import java.util.List;

import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.common.type.DoubleType.DOUBLE;
import static com.facebook.presto.common.type.IntegerType.INTEGER;
import static com.facebook.presto.common.type.VarcharType.VARCHAR;
import static io.ahana.presto.datagen.TestDataGenCatalog.TEST_CATALOG;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;

public class TestDataGenRecordSet
{
    @Test
    public void testGetColumnTypes()
    {
        DataGenTable table = TEST_CATALOG.getTable("first", "ta").get();
        RecordSet recordSet = new DataGenRecordSet(
                table.getSplitSpecs().get(0),
                columnHandles(table.getColumns()));

        assertEquals(recordSet.getColumnTypes(), ImmutableList.of(BIGINT, DOUBLE));

        recordSet = new DataGenRecordSet(
                table.getSplitSpecs().get(0),
                ImmutableList.of());
        assertEquals(recordSet.getColumnTypes(), ImmutableList.of());
    }

    @Test
    public void testCursorSimple()
    {
        DataGenTable table = TEST_CATALOG.getTable("first", "ta").get();
        RecordSet recordSet = new DataGenRecordSet(
                table.getSplitSpecs().get(1),
                columnHandles(table.getColumns()));

        RecordCursor cursor = recordSet.cursor();

        assertEquals(cursor.getType(0), BIGINT);
        assertEquals(cursor.getType(1), DOUBLE);

        ImmutableList.Builder<Long> vals0 = ImmutableList.builder();
        ImmutableList.Builder<Double> vals1 = ImmutableList.builder();

        while (cursor.advanceNextPosition()) {
            assertFalse(cursor.isNull(0));
            vals0.add(cursor.getLong(0));

            assertFalse(cursor.isNull(1));
            vals1.add(cursor.getDouble(1));
        }

        assertEquals(vals0.build(), ImmutableList.of(4L, 6L, 9L, 4L));
        assertEquals(vals1.build(), ImmutableList.of(0.0, 3.0, 6.0, 9.0));
    }

    @Test
    public void testCursorVarchar()
    {
        DataGenTable table = TEST_CATALOG.getTable("second", "tb").get();
        RecordSet recordSet = new DataGenRecordSet(
                table.getSplitSpecs().get(0),
                columnHandles(table.getColumns()));

        RecordCursor cursor = recordSet.cursor();

        assertEquals(cursor.getType(0), INTEGER);
        assertEquals(cursor.getType(1), DOUBLE);
        assertEquals(cursor.getType(2), VARCHAR);

        ImmutableList.Builder<Long> vals0 = ImmutableList.builder();
        ImmutableList.Builder<Double> vals1 = ImmutableList.builder();
        ImmutableList.Builder<String> vals2 = ImmutableList.builder();

        while (cursor.advanceNextPosition()) {
            assertFalse(cursor.isNull(0));
            vals0.add(cursor.getLong(0));

            assertFalse(cursor.isNull(1));
            vals1.add(cursor.getDouble(1));

            assertFalse(cursor.isNull(2));
            vals2.add(cursor.getSlice(2).toStringUtf8());
        }

        assertEquals(vals0.build(), ImmutableList.of(1L, 3L, 1L, 3L, 1L));
        assertEquals(vals1.build(), ImmutableList.of(5.5, 6.25, 7.0, 5.5, 6.25));
        assertEquals(vals2.build(), ImmutableList.of("BAX", "CAK", "CZX", "BAX", "CAK"));
    }

    private List<DataGenColumnHandle> columnHandles(List<DataGenColumn> columns)
    {
        int ordinalPosition = 0;
        ImmutableList.Builder<DataGenColumnHandle> handles = ImmutableList.builder();
        for (DataGenColumn column : columns) {
            handles.add(new DataGenColumnHandle(column.getName(), column.getType(), ordinalPosition));
            ordinalPosition++;
        }

        return handles.build();
    }
}
