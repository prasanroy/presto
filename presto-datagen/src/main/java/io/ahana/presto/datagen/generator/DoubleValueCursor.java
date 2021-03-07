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

import com.facebook.presto.common.type.DoubleType;
import io.ahana.presto.datagen.DataGenBaseColumnStats;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

public class DoubleValueCursor
        implements ValueCursor
{
    public static final long MAX_DISTINCTVALSCOUNT = 100;

    private final double min;
    private final double max;
    private final long distinctValsCount;

    private double value;

    private double nextValue;
    private double increment;

    public DoubleValueCursor(double min, double max, long distinctValsCount)
    {
        this.min = min;
        checkArgument(this.min >= 0, "min is negative, only positive values allowed");
        this.max = max;
        checkArgument(this.max >= 0, "max is negative, only positive values allowed");
        checkArgument(this.min <= this.max, "max is less than min");

        this.distinctValsCount = distinctValsCount;
        checkArgument(this.distinctValsCount >= 1, "distinct value count must be greater or equal to one");

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
    public DoubleType getValueType()
    {
        return DoubleType.DOUBLE;
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

    public static DoubleValueCursor create(DataGenBaseColumnStats columnSpec)
    {
        requireNonNull(columnSpec, "columnSpec is null");

        double min = ((Double) columnSpec.getMin().orElse(0)).doubleValue();
        double max = ((Double) columnSpec.getMax().orElse(Double.MAX_VALUE)).doubleValue();
        long distinctValsCount = columnSpec.getDistinctValsCount().orElse((long) Math.min(max - min + 1, MAX_DISTINCTVALSCOUNT));

        return new DoubleValueCursor(min, max, distinctValsCount);
    }
}
