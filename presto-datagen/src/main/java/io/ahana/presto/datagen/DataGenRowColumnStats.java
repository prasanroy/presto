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
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

public final class DataGenRowColumnStats
        extends DataGenColumnStats
{
    private final Map<String, DataGenColumnStats> fieldStats;

    @JsonCreator
    public DataGenRowColumnStats(
            @JsonProperty("fieldStats") List<DataGenNamedColumnStats> fieldStats)
    {
        requireNonNull(fieldStats, "fieldStats is null");

        ImmutableMap.Builder<String, DataGenColumnStats> statsMapBuilder = ImmutableMap.builder();
        for (DataGenNamedColumnStats s : fieldStats) {
            statsMapBuilder.put(s.getName(), s.getStats());
        }

        this.fieldStats = statsMapBuilder.build();
    }

    @JsonProperty
    public List<DataGenNamedColumnStats> getFieldStats()
    {
        ImmutableList.Builder<DataGenNamedColumnStats> statsListBuilder = ImmutableList.builder();

        for (Map.Entry<String, DataGenColumnStats> e : fieldStats.entrySet()) {
            statsListBuilder.add(new DataGenNamedColumnStats(e.getKey(), e.getValue()));
        }

        return statsListBuilder.build();
    }

    public Optional<DataGenColumnStats> getFieldStats(String fieldName)
    {
        return Optional.ofNullable(fieldStats.get(fieldName));
    }

    @Override
    public String toString()
    {
        ToStringHelper fieldStatsString = toStringHelper(fieldStats);
        for (Map.Entry<String, DataGenColumnStats> e : fieldStats.entrySet()) {
            fieldStatsString.add(e.getKey(), e.getValue());
        }

        return toStringHelper(this)
                .add("fieldStats", fieldStatsString)
                .toString();
    }
}
