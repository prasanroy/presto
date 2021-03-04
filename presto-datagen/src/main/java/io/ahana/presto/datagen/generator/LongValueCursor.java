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

public class LongValueCursor
        implements ValueCursor
{
    private final Type valueType;

    private final long min;
    private final long max;
    private final long distinctValsCount;

    private long value;

    private double nextValue;
    private double increment;

    public LongValueCursor(
            Type valueType, long min, long max, long distinctValsCount)
    {
        this.valueType = requireNonNull(valueType, "value type is null");

        this.min = min;
        checkArgument(this.min >= 0, "min is negative, only positive values allowed");
        this.max = max;
        checkArgument(this.max >= 0, "max is negative, only positive values allowed");
        checkArgument(this.min <= this.max, "max is less than min");

        this.distinctValsCount = distinctValsCount;
        checkArgument(this.distinctValsCount >= 1, "distinct value count must be greater or equal to one");
        checkArgument(this.max - this.min >= this.distinctValsCount - 1, String.format("distinct values count %d cannot be accomodated in the given min-max range [%d, %d]", this.distinctValsCount, this.min, this.max));

        if (this.distinctValsCount == 1) {
            this.increment = 0.0;
        }
        else {
            this.increment = ((double) (this.max - this.min)) / (this.distinctValsCount - 1);
        }

        this.value = -1;
        this.nextValue = (double) this.min;
    }

    @Override
    public Type getValueType()
    {
        return valueType;
    }

    @Override
    public Long getValue()
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
        value = (long) nextValue;

        if (nextValue > max - increment) {
            if (nextValue < max) {
                nextValue = max;
            }
            else {
                nextValue = min;
            }
        }
        else {
            nextValue += increment;
        }
    }
}
