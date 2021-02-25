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

import com.facebook.airlift.json.JsonCodec;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.io.Resources;

import javax.inject.Inject;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.URL;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static com.google.common.collect.Maps.transformValues;
import static com.google.common.collect.Maps.uniqueIndex;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Objects.requireNonNull;

public class DataGen
{
    /**
     * SchemaName -> (TableName -> TableMetadata)
     */
    private final Map<String, Map<String, DataGenTable>> schemas;

    @Inject
    public DataGen(
            DataGenConfig config,
            JsonCodec<Map<String, List<DataGenTable>>> catalogCodec)
    {
        requireNonNull(config, "config is null");
        requireNonNull(catalogCodec, "catalogCodec is null");

        try {
            schemas = readSchemas(config.getMetadataUrl(), catalogCodec);
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    public Set<String> getSchemaNames()
    {
        return schemas.keySet();
    }

    public Set<String> getTableNames(String schema)
    {
        requireNonNull(schema, "schema is null");

        Map<String, DataGenTable> tables = schemas.get(schema);
        if (tables == null) {
            return ImmutableSet.of();
        }
        return tables.keySet();
    }

    public DataGenTable getTable(String schema, String tableName)
    {
        requireNonNull(schema, "schema is null");
        requireNonNull(tableName, "tableName is null");

        Map<String, DataGenTable> tables = schemas.get(schema);
        if (tables == null) {
            return null;
        }
        return tables.get(tableName);
    }

    private static Map<String, Map<String, DataGenTable>> readSchemas(
            URL metadataURL,
            JsonCodec<Map<String, List<DataGenTable>>> catalogCodec) throws IOException
    {
        String json = Resources.toString(metadataURL, UTF_8);
        Map<String, List<DataGenTable>> catalog = catalogCodec.fromJson(json);

        Map<String, Map<String, DataGenTable>> schema = transformValues(
                catalog,
                tables -> ImmutableMap.copyOf(uniqueIndex(tables, DataGenTable::getName)));

        return ImmutableMap.copyOf(schema);
    }
}
