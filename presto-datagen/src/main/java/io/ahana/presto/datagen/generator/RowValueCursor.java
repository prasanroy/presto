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
package io.ahana.presto.datagen.generator;

import com.facebook.presto.common.block.Block;
import com.facebook.presto.common.block.BlockBuilder;
import com.facebook.presto.common.block.RowBlockBuilder;
import com.facebook.presto.common.block.SingleRowBlockWriter;
import com.facebook.presto.common.type.RowType;
import com.google.common.collect.ImmutableList;
import io.ahana.presto.datagen.DataGenRowColumnStats;

import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

public final class RowValueCursor
        implements ValueCursor
{
    private final RowType rowType;

    private final List<ValueCursor> fieldCursors;

    private Block value;

    public RowValueCursor(RowType rowType, List<ValueCursor> fieldCursors)
    {
        this.rowType = requireNonNull(rowType, "rowType is null");
        this.fieldCursors = ImmutableList.copyOf(requireNonNull(fieldCursors, "fieldCursors is null"));

        int fieldCount = rowType.getFields().size();
        int cursorCount = fieldCursors.size();
        checkArgument(fieldCount == cursorCount, "Expected %d cursors, found %d", fieldCount, cursorCount);
    }

    @Override
    public RowType getValueType()
    {
        return rowType;
    }

    @Override
    public Block getValue()
    {
        return value;
    }

    @Override
    public boolean isNull()
    {
        return false;
    }

    @Override
    public void advanceNextPosition()
    {
        RowBlockBuilder rowBuilder = (RowBlockBuilder) rowType.createBlockBuilder(null, 1);
        SingleRowBlockWriter blockWriter = rowBuilder.beginBlockEntry();

        for (ValueCursor fieldCursor : fieldCursors) {
            fieldCursor.advanceNextPosition();
            fieldCursor.writeValue(blockWriter);
        }

        rowBuilder.closeEntry();

        value = (Block) rowType.getObject(rowBuilder, 0);
    }

    @Override
    public void writeValue(BlockBuilder builder)
    {
        rowType.writeObject(builder, getValue());
    }

    public static RowValueCursor create(
            RowType rowType,
            DataGenRowColumnStats columnSpec,
            ValueCursorFactory valueCursorFactory)
    {
        requireNonNull(rowType, "rowType is null");
        requireNonNull(columnSpec, "columnSpec is null");

        ImmutableList.Builder<ValueCursor> fieldCursorListBuilder = ImmutableList.builder();
        for (RowType.Field field : rowType.getFields()) {
            String fieldName = field.getName().orElseThrow(() ->
                    new IllegalArgumentException("Anonymous fields are not supported"));
            ValueCursor fieldCursor = valueCursorFactory.create(field.getType(), columnSpec.getFieldStats(fieldName));
            fieldCursorListBuilder.add(fieldCursor);
        }

        return new RowValueCursor(rowType, fieldCursorListBuilder.build());
    }
}
