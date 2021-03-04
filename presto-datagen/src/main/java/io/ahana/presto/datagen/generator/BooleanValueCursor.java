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

import com.facebook.presto.common.type.Type;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

public class BooleanValueCursor
        implements ValueCursor
{
    private final Type valueType;

    private final Boolean min;
    private final Boolean max;
    private final long distinctValsCount;

    private boolean value;
    private boolean nextValue;

    public BooleanValueCursor(
            Type valueType, boolean min, boolean max, long distinctValsCount)
    {
        this.valueType = requireNonNull(valueType, "value type is null");

        this.min = min;
        this.max = max;
        checkArgument(!this.min || this.max, "max is less than min");

        this.distinctValsCount = distinctValsCount;
        checkArgument(this.distinctValsCount == 1 || this.distinctValsCount == 2, "distinct values count must be either one or two");
        checkArgument(this.max == this.min || this.distinctValsCount == 2, String.format("distinct values count %d cannot be accomodated in the given min-max range [%s, %s]", this.distinctValsCount, this.min.toString(), this.max.toString()));

        this.value = false;
        this.nextValue = this.min;
    }

    @Override
    public Type getValueType()
    {
        return valueType;
    }

    @Override
    public Boolean getValue()
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
        value = nextValue;
        nextValue = this.max;
    }
}
