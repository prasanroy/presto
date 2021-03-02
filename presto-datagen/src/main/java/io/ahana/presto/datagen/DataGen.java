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
import com.google.common.collect.ImmutableSet;
import com.google.common.io.Resources;

import javax.inject.Inject;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.URL;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Strings.isNullOrEmpty;
import static com.google.common.collect.Maps.uniqueIndex;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Objects.requireNonNull;

public class DataGen
{
    private final Map<String, DataGenSchema> schemas;

    @Inject
    public DataGen(
            DataGenConfig config,
            JsonCodec<List<DataGenSchema>> jsonCodec)
    {
        requireNonNull(config, "config is null");
        requireNonNull(jsonCodec, "jsonCodec is null");

        String catalogJson = getCatalogJson(config.getCatalogFileName());
        requireNonNull(catalogJson, "catalogJson is null");

        List<DataGenSchema> schemaList = jsonCodec.fromJson(catalogJson);
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

    private static String getCatalogJson(String catalogFileName)
    {
        String catalogJson;

        requireNonNull(catalogFileName, "catalogFileName is not set");
        try {
            URL catalogJsonUrl = Resources.getResource(catalogFileName);
            catalogJson = Resources.toString(catalogJsonUrl, UTF_8);
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }

        return catalogJson;
    }
}
