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

import com.google.common.base.MoreObjects.ToStringHelper;
import com.google.common.collect.ImmutableSet;

import javax.inject.Inject;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Strings.isNullOrEmpty;
import static com.google.common.collect.Maps.uniqueIndex;
import static java.util.Objects.requireNonNull;

public class DataGenCatalog
{
    private final Map<String, DataGenSchema> schemas;

    @Inject
    public DataGenCatalog(DataGenCatalogParser parser, DataGenConfig config)
    {
        this(parser.parseCatalogJson(config.getCatalogJson()));
    }

    public DataGenCatalog(List<DataGenSchema> schemaList)
    {
        requireNonNull(schemaList, "schemaList is null");
        this.schemas = uniqueIndex(schemaList, DataGenSchema::getName);
    }

    public Set<String> getSchemaNames()
    {
        return ImmutableSet.copyOf(schemas.keySet());
    }

    public Optional<DataGenSchema> getSchema(String schemaName)
    {
        checkArgument(!isNullOrEmpty(schemaName), "schemaName is null or is empty");
        return Optional.ofNullable(schemas.get(schemaName));
    }

    public Set<String> getTableNames(String schemaName)
    {
        return getSchema(schemaName)
            .map(schema -> schema.getTableNames())
            .orElse(ImmutableSet.of());
    }

    public Optional<DataGenTable> getTable(String schemaName, String tableName)
    {
        return getSchema(schemaName).map(schema -> schema.getTable(tableName));
    }

    @Override
    public String toString()
    {
        ToStringHelper schemasString = toStringHelper(schemas);
        for (Map.Entry<String, DataGenSchema> e : schemas.entrySet()) {
            schemasString.add(e.getKey(), e.getValue());
        }

        return toStringHelper(this)
                .add("schemas", schemasString)
                .toString();
    }
}
