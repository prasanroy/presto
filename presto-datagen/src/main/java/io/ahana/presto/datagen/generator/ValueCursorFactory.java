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
import io.ahana.presto.datagen.DataGenColumnStats;
import io.airlift.slice.Slice;

import java.util.Optional;

public class ValueCursorFactory
{
    public ValueCursor create(Type columnType, Optional<DataGenColumnStats> columnSpecOpt)
    {
        if (!columnSpecOpt.isPresent()) {
            return new NullCursor(columnType);
        }

        DataGenColumnStats columnSpec = columnSpecOpt.get();

        if (columnType.getJavaType() == long.class) {
            return new LongValueCursor(columnType, columnSpec);
        }
        else if (columnType.getJavaType() == double.class) {
            return new DoubleValueCursor(columnType, columnSpec);
        }
        else if (columnType.getJavaType() == boolean.class) {
            return new BooleanValueCursor(columnType, columnSpec);
        }
        else if (columnType.getJavaType() == Slice.class) {
            throw new UnsupportedOperationException();
            // return new SliceValueCursor(columnType, columnSpec);
        }
        else if (columnType.getJavaType() == Block.class) {
            throw new UnsupportedOperationException();
            // return new BlockValueCursor(columnType, columnSpec);
        }
        else {
            throw new UnsupportedOperationException();
        }
    }
}
