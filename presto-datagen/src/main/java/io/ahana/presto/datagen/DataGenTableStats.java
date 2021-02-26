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
import com.google.common.base.MoreObjects.ToStringHelper;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.Maps.uniqueIndex;
import static java.util.Objects.requireNonNull;

public final class DataGenTableStats
{
    private final long rowCount;
    private final Map<String, DataGenColumnStats> columnStats;

    @JsonCreator
    public DataGenTableStats(
            @JsonProperty("rowCount") long rowCount,
            @JsonProperty("columnStats") List<DataGenColumnStats> columnStats)
    {
        checkArgument(rowCount >= 0, "rowCount is negative");
        this.rowCount = rowCount;

        requireNonNull(columnStats, "columnStats is null");
        this.columnStats = uniqueIndex(columnStats, DataGenColumnStats::getName);
    }

    @JsonProperty
    public long getRowCount()
    {
        return rowCount;
    }

    @JsonProperty
    public Map<String, DataGenColumnStats> getColumnStats()
    {
        return columnStats;
    }

    public Optional<DataGenColumnStats> getColumnStats(String columnName)
    {
        return Optional.ofNullable(columnStats.get(columnName));
    }

    @Override
    public String toString()
    {
        ToStringHelper columnStatsString = toStringHelper(columnStats);
        for (Map.Entry<String, DataGenColumnStats> e : columnStats.entrySet()) {
            columnStatsString.add(e.getKey(), e.getValue());
        }

        return toStringHelper(this)
                .add("rowCount", rowCount)
                .add("columnStats", columnStatsString)
                .toString();
    }
}
