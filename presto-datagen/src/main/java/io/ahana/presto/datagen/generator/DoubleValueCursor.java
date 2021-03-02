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
import io.ahana.presto.datagen.DataGenColumnStats;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

public class DoubleValueCursor
        implements ValueCursor
{
    public static final long MAX_DISTINCTVALSCOUNT = 100;

    private final Type valueType;

    private final Double min;
    private final Double max;
    private final long distinctValsCount;

    private double value;

    private double nextValue;
    private double increment;

    public DoubleValueCursor(Type valueType, DataGenColumnStats spec)
    {
        this.valueType = requireNonNull(valueType, "value type is null");

        this.min = (Double) spec.getMin().orElse(0);
        checkArgument(this.min >= 0, "min is negative, only positive values allowed");
        this.max = (Double) spec.getMax().orElse(Double.MAX_VALUE);
        checkArgument(this.max >= 0, "max is negative, only positive values allowed");
        checkArgument(this.min <= this.max, "max is less than min");

        this.distinctValsCount = spec.getDistinctValsCount().orElse((long) Math.min(this.max - this.min + 1, MAX_DISTINCTVALSCOUNT));
        checkArgument(this.distinctValsCount >= 1, "distinct value count must be greater or equal to one");
        checkArgument(this.max - this.min >= this.distinctValsCount - 1, "distinct value count cannot be accomodated in the given min-max range");

        if (this.distinctValsCount == 1) {
            this.increment = 0.0;
        }
        else {
            this.increment = (this.max - this.min) / (this.distinctValsCount - 1);
        }

        this.value = -1;
        this.nextValue = this.min;
    }

    @Override
    public Type getValueType()
    {
        return valueType;
    }

    @Override
    public Double getValue()
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