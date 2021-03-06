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

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

public final class DataGenArrayColumnStats
        extends DataGenColumnStats
{
    private final long elementCount;
    private final DataGenColumnStats elementStats;

    @JsonCreator
    public DataGenArrayColumnStats(
            @JsonProperty("elementCount") long elementCount,
            @JsonProperty("elementStats") DataGenColumnStats elementStats)
    {
        checkArgument(elementCount >= 0, "elementCount is negative");
        this.elementCount = elementCount;

        requireNonNull(elementStats, "elementStats is null");
        this.elementStats = elementStats;
    }

    @JsonProperty
    public long getElementCount()
    {
        return elementCount;
    }

    @JsonProperty
    public DataGenColumnStats getElementStats()
    {
        return elementStats;
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("elementCount", elementCount)
                .add("elementStats", elementStats)
                .toString();
    }
}
