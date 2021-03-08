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
import com.facebook.presto.common.type.Type;
import com.google.common.collect.ImmutableList;
import io.ahana.presto.datagen.DataGenArrayColumnStats;
import io.ahana.presto.datagen.DataGenBaseColumnStats;
import io.ahana.presto.datagen.DataGenColumnStats;

import java.util.Optional;

import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.common.type.BooleanType.BOOLEAN;
import static com.facebook.presto.common.type.DoubleType.DOUBLE;
import static com.facebook.presto.common.type.IntegerType.INTEGER;
import static com.facebook.presto.common.type.VarcharType.VARCHAR;
import static com.google.common.base.Preconditions.checkArgument;

public final class ValueCursorFactory
{
    public ValueCursor create(Type columnType, Optional<DataGenColumnStats> columnSpecOpt)
    {
        if (!columnSpecOpt.isPresent()) {
            return new NullCursor(columnType);
        }

        return create(columnType, columnSpecOpt.get());
    }

    public ValueCursor create(Type columnType, DataGenColumnStats columnSpec)
    {
        if (ImmutableList.of(INTEGER, BIGINT).contains(columnType)) {
            checkArgument(columnSpec instanceof DataGenBaseColumnStats, "incompatible column statistics specification");
            return LongValueCursor.create(columnType, (DataGenBaseColumnStats) columnSpec);
        }
        else if (ImmutableList.of(DOUBLE).contains(columnType)) {
            checkArgument(columnSpec instanceof DataGenBaseColumnStats, "incompatible column statistics specification");
            return DoubleValueCursor.create((DataGenBaseColumnStats) columnSpec);
        }
        else if (ImmutableList.of(BOOLEAN).contains(columnType)) {
            checkArgument(columnSpec instanceof DataGenBaseColumnStats, "incompatible column statistics specification");
            return BooleanValueCursor.create((DataGenBaseColumnStats) columnSpec);
        }
        else if (ImmutableList.of(VARCHAR).contains(columnType)) {
            checkArgument(columnSpec instanceof DataGenBaseColumnStats, "incompatible column statistics specification");
            return StringValueCursor.create((DataGenBaseColumnStats) columnSpec);
        }
        else if (columnType instanceof ArrayType) {
            checkArgument(columnSpec instanceof DataGenArrayColumnStats, "incompatible column statistics specification");
            return ArrayValueCursor.create((ArrayType) columnType, (DataGenArrayColumnStats) columnSpec, this);
        }

        throw new UnsupportedOperationException();
    }
}
