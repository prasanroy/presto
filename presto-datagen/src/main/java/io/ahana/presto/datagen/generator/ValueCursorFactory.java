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
import com.facebook.presto.common.type.Type;
import com.google.common.collect.ImmutableList;
import io.ahana.presto.datagen.DataGenColumnStats;

import java.util.Optional;

import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.common.type.BooleanType.BOOLEAN;
import static com.facebook.presto.common.type.DoubleType.DOUBLE;
import static com.facebook.presto.common.type.IntegerType.INTEGER;
import static com.facebook.presto.common.type.VarcharType.VARCHAR;

public class ValueCursorFactory
{
    public static final long MAX_DISTINCTVALSCOUNT = 100;

    public ValueCursor create(Type columnType, Optional<DataGenColumnStats> columnSpecOpt)
    {
        if (!columnSpecOpt.isPresent()) {
            return new NullCursor(columnType);
        }

        DataGenColumnStats columnSpec = columnSpecOpt.get();

        if (ImmutableList.of(INTEGER, BIGINT).contains(columnType)) {
            long min = ((Number) columnSpec.getMin().orElse(0L)).longValue();
            long max = ((Number) columnSpec.getMax().orElse(Long.MAX_VALUE)).longValue();
            long distinctValsCount = columnSpec.getDistinctValsCount().orElse(Math.min(max - min + 1, MAX_DISTINCTVALSCOUNT));

            return new LongValueCursor(columnType, min, max, distinctValsCount);
        }
        else if (ImmutableList.of(DOUBLE).contains(columnType)) {
            double min = ((Double) columnSpec.getMin().orElse(0)).doubleValue();
            double max = ((Double) columnSpec.getMax().orElse(Double.MAX_VALUE)).doubleValue();
            long distinctValsCount = columnSpec.getDistinctValsCount().orElse((long) Math.min(max - min + 1, MAX_DISTINCTVALSCOUNT));

            return new DoubleValueCursor(columnType, min, max, distinctValsCount);
        }
        else if (ImmutableList.of(BOOLEAN).contains(columnType)) {
            boolean min = ((Boolean) columnSpec.getMin().orElse(false)).booleanValue();
            boolean max = ((Boolean) columnSpec.getMax().orElse(true)).booleanValue();
            long distinctValsCount = columnSpec.getDistinctValsCount().orElse(2L);
            return new BooleanValueCursor(columnType, min, max, distinctValsCount);
        }
        else if (ImmutableList.of(VARCHAR).contains(columnType)) {
            throw new UnsupportedOperationException("VARCHAR not supported");
            // return new SliceValueCursor(columnType, columnSpec);
        }
        else if (columnType.getJavaType() == Block.class) {
            throw new UnsupportedOperationException("BLOCK types not supported");
            // return new BlockValueCursor(columnType, columnSpec);
        }
        else {
            throw new UnsupportedOperationException();
        }
    }
}
