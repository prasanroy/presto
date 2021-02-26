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

import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ColumnMetadata;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.ConnectorTableHandle;
import com.facebook.presto.spi.ConnectorTableLayout;
import com.facebook.presto.spi.ConnectorTableLayoutHandle;
import com.facebook.presto.spi.ConnectorTableLayoutResult;
import com.facebook.presto.spi.ConnectorTableMetadata;
import com.facebook.presto.spi.Constraint;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.SchemaTablePrefix;
import com.facebook.presto.spi.TableNotFoundException;
import com.facebook.presto.spi.connector.ConnectorMetadata;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

import javax.inject.Inject;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static java.util.Objects.requireNonNull;

public class DataGenMetadata
        implements ConnectorMetadata
{
    private final DataGen datagen;

    @Inject
    public DataGenMetadata(DataGen datagen)
    {
        this.datagen = requireNonNull(datagen, "client is null");
    }

    @Override
    public List<String> listSchemaNames(ConnectorSession session)
    {
        return ImmutableList.copyOf(datagen.getSchemaNames());
    }

    @Override
    public DataGenTableHandle getTableHandle(
            ConnectorSession session,
            SchemaTableName table)
    {
        String schemaName = table.getSchemaName();
        String tableName = table.getTableName();

        if (datagen.getTable(schemaName, tableName).isPresent()) {
            return new DataGenTableHandle(schemaName, tableName);
        }

        return null;
    }

    @Override
    public List<ConnectorTableLayoutResult> getTableLayouts(
            ConnectorSession session,
            ConnectorTableHandle table,
            Constraint<ColumnHandle> constraint,
            Optional<Set<ColumnHandle>> desiredColumns)
    {
        DataGenTableHandle tableHandle = (DataGenTableHandle) table;
        ConnectorTableLayout layout = new ConnectorTableLayout(new DataGenTableLayoutHandle(tableHandle));
        return ImmutableList.of(new ConnectorTableLayoutResult(layout, constraint.getSummary()));
    }

    @Override
    public ConnectorTableLayout getTableLayout(
            ConnectorSession session,
            ConnectorTableLayoutHandle handle)
    {
        return new ConnectorTableLayout(handle);
    }

    @Override
    public ConnectorTableMetadata getTableMetadata(
            ConnectorSession session,
            ConnectorTableHandle table)
    {
        DataGenTableHandle tableHandle = (DataGenTableHandle) table;
        SchemaTableName tableName = new SchemaTableName(tableHandle.getSchemaName(), tableHandle.getTableName());

        return getTableMetadata(tableName).orElse(null);
    }

    @Override
    public List<SchemaTableName> listTables(
            ConnectorSession session,
            String schemaNameOrNull)
    {
        Set<String> schemaNames;
        if (schemaNameOrNull == null) {
            schemaNames = datagen.getSchemaNames();
        }
        else {
            schemaNames = ImmutableSet.of(schemaNameOrNull);
        }

        ImmutableList.Builder<SchemaTableName> builder = ImmutableList.builder();
        for (String schemaName : schemaNames) {
            for (String tableName : datagen.getTableNames(schemaName)) {
                builder.add(new SchemaTableName(schemaName, tableName));
            }
        }

        return builder.build();
    }

    @Override
    public Map<String, ColumnHandle> getColumnHandles(
            ConnectorSession session,
            ConnectorTableHandle handle)
    {
        DataGenTableHandle tableHandle = (DataGenTableHandle) handle;

        String schemaName = tableHandle.getSchemaName();
        String tableName = tableHandle.getTableName();
        DataGenTable table = datagen.getTable(schemaName, tableName).orElseThrow(() -> new TableNotFoundException(tableHandle.toSchemaTableName()));

        ImmutableMap.Builder<String, ColumnHandle> columnHandles = ImmutableMap.builder();
        int index = 0;
        for (ColumnMetadata column : table.getColumnsMetadata()) {
            columnHandles.put(column.getName(), new DataGenColumnHandle(column.getName(), column.getType(), index));
            index++;
        }

        return columnHandles.build();
    }

    @Override
    public Map<SchemaTableName, List<ColumnMetadata>> listTableColumns(
            ConnectorSession session,
            SchemaTablePrefix prefix)
    {
        requireNonNull(prefix, "prefix is null");
        ImmutableMap.Builder<SchemaTableName, List<ColumnMetadata>> columns = ImmutableMap.builder();
        for (SchemaTableName tableName : listTables(session, prefix)) {
            getTableMetadata(tableName).ifPresent(tableMetadata ->
                    columns.put(tableName, tableMetadata.getColumns()));
        }
        return columns.build();
    }

    private Optional<ConnectorTableMetadata> getTableMetadata(
            SchemaTableName schemaTable)
    {
        String schemaName = schemaTable.getSchemaName();
        String tableName = schemaTable.getTableName();

        return datagen.getTable(schemaName, tableName).map(table ->
            new ConnectorTableMetadata(schemaTable, table.getColumnsMetadata()));
    }

    private List<SchemaTableName> listTables(
            ConnectorSession session,
            SchemaTablePrefix prefix)
    {
        String schemaName = prefix.getSchemaName();

        if (prefix.getSchemaName() == null) {
            return listTables(session, schemaName);
        }

        return ImmutableList.of(new SchemaTableName(schemaName, prefix.getTableName()));
    }

    @Override
    public ColumnMetadata getColumnMetadata(
            ConnectorSession session,
            ConnectorTableHandle tableHandle,
            ColumnHandle columnHandle)
    {
        return ((DataGenColumnHandle) columnHandle).getColumnMetadata();
    }
}
