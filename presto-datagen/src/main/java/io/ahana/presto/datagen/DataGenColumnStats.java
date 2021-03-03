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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Optional;

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Strings.isNullOrEmpty;
import static java.util.Objects.requireNonNull;

public final class DataGenColumnStats
{
    private final String name;

    private final Optional<Object> min;
    private final Optional<Object> max;
    private final Optional<Long> distinctValsCount;
    private final long nullsCount;
    private final Optional<Long> dataSize;

    public DataGenColumnStats(
            String name, Object min,
            Object max, long distinctValsCount)
    {
        this(name, Optional.of(min), Optional.of(max),
                Optional.of(Long.valueOf(distinctValsCount)),
                Optional.empty(), Optional.empty());
    }

    @JsonCreator
    public DataGenColumnStats(
            @JsonProperty("name") String name,
            @JsonProperty("min") Optional<Object> min,
            @JsonProperty("max") Optional<Object> max,
            @JsonProperty("distinctValsCount") Optional<Long> distinctValsCount,
            @JsonProperty("nullsCount") Optional<Long> nullsCount,
            @JsonProperty("dataSize") Optional<Long> dataSize)
    {
        checkArgument(!isNullOrEmpty(name), "name is null or is empty");
        this.name = name;

        this.min = requireNonNull(min, "min is null");
        this.max = requireNonNull(max, "max is null");

        requireNonNull(distinctValsCount, "distinctValsCount is null");
        checkArgument(distinctValsCount.isPresent() && distinctValsCount.get() > 0, "distinctValsCount is non-positive");
        this.distinctValsCount = distinctValsCount;

        requireNonNull(nullsCount, "nullsCount is null");
        checkArgument(!nullsCount.isPresent() || nullsCount.get() >= 0, "nullsCount is negative");
        this.nullsCount = nullsCount.orElse(0L);

        requireNonNull(dataSize, "dataSize is null");
        checkArgument(!dataSize.isPresent() || dataSize.get() >= 0, "dataSize is negative");
        this.dataSize = dataSize;
    }

    @JsonProperty
    public String getName()
    {
        return name;
    }

    @JsonProperty
    public Optional<Object> getMin()
    {
        return min;
    }

    @JsonProperty
    public Optional<Object> getMax()
    {
        return max;
    }

    @JsonProperty
    public Optional<Long> getDistinctValsCount()
    {
        return distinctValsCount;
    }

    @JsonProperty
    public long getNullsCount()
    {
        return nullsCount;
    }

    @JsonProperty
    public Optional<Long> getDataSize()
    {
        return dataSize;
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("min", min)
                .add("max", max)
                .add("distinctValsCount", distinctValsCount)
                .add("nullsCount", nullsCount)
                .add("dataSize", dataSize)
                .toString();
    }
}
