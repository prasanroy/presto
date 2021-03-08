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

import com.facebook.presto.common.type.ArrayType;
import io.ahana.presto.datagen.DataGenArrayColumnStats;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

public final class ArrayValueCursor
        implements ValueCursor
{
    private final ArrayType arrayType;

    private final ValueCursor elementCursor;
    private final int elementCount;

    private Object[] value;
    private Object[] nextValue;

    public ArrayValueCursor(
            ArrayType arrayType, ValueCursor elementCursor, int elementCount)
    {
        this.arrayType = requireNonNull(arrayType, "arrayType is null");
        this.elementCursor = requireNonNull(elementCursor, "elementCursor is null");

        checkArgument(elementCount >= 0, "elementCount is negative");
        this.elementCount = elementCount;
    }

    @Override
    public ArrayType getValueType()
    {
        return arrayType;
    }

    @Override
    public Object[] getValue()
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
        /* TODO */
    }

    public static ArrayValueCursor create(
            ArrayType columnType,
            DataGenArrayColumnStats columnSpec,
            ValueCursorFactory valueCursorFactory)
    {
        requireNonNull(columnType, "value type is null");
        requireNonNull(columnSpec, "columnSpec is null");

        ValueCursor elementCursor = valueCursorFactory.create(columnType.getElementType(), columnSpec.getElementStats());

        return new ArrayValueCursor(columnType, elementCursor, columnSpec.getElementCount());
    }
}
