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
import com.google.common.collect.ImmutableSet;

import java.util.List;
import java.util.Map;
import java.util.Set;

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Strings.isNullOrEmpty;
import static com.google.common.collect.Maps.uniqueIndex;
import static java.util.Objects.requireNonNull;

public class DataGenSchema
{
    private final String name;
    private final Map<String, DataGenTable> tables;

    @JsonCreator
    public DataGenSchema(
            @JsonProperty("name") String name,
            @JsonProperty("tables") List<DataGenTable> tables)
    {
        checkArgument(!isNullOrEmpty(name), "name is null or is empty");
        this.name = name;

        requireNonNull(tables, "tables is null");
        this.tables = uniqueIndex(tables, DataGenTable::getName);
    }

    @JsonProperty
    public String getName()
    {
        return name;
    }

    @JsonProperty
    public List<DataGenTable> getTables()
    {
        return ImmutableList.copyOf(tables.values());
    }

    public Set<String> getTableNames()
    {
        return ImmutableSet.copyOf(tables.keySet());
    }

    public DataGenTable getTable(String tableName)
    {
        checkArgument(!isNullOrEmpty(tableName), "tableName is null or is empty");
        return tables.get(tableName);
    }

    @Override
    public String toString()
    {
        ToStringHelper tablesString = toStringHelper(tables);
        for (Map.Entry<String, DataGenTable> e : tables.entrySet()) {
            tablesString.add(e.getKey(), e.getValue());
        }

        return toStringHelper(this)
                .add("name", name)
                .add("tables", tablesString)
                .toString();
    }
}
