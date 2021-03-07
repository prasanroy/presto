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

import org.testng.annotations.Test;

import java.util.List;
import java.util.Optional;

import static io.ahana.presto.datagen.TestDataGenCatalog.TEST_CATALOG;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

public class TestDataGenCatalogParser
{
    @Test
    public void testJsonDeserialization()
    {
        List<DataGenSchema> parsedSchemaList = MetadataUtil.SCHEMALIST_CODEC.fromJson(TEST_SCHEMALIST_JSON);

        DataGenCatalog parsedCatalog = new DataGenCatalog(parsedSchemaList);

        for (String schemaName : TEST_CATALOG.getSchemaNames()) {
            for (String tableName : TEST_CATALOG.getTableNames(schemaName)) {
                Optional<DataGenTable> parsedTable = parsedCatalog.getTable(schemaName, tableName);
                assertTrue(parsedTable.isPresent(), String.format("Table %s.%s not present in catalog %s", schemaName, tableName, parsedCatalog));

                Optional<DataGenTable> table = TEST_CATALOG.getTable(schemaName, tableName);
                assertTrue(table.isPresent());

                List<DataGenColumn> columns = table.get().getColumns();
                assertEquals(parsedTable.get().getColumns(), columns);

                List<DataGenTableStats> parsedSplitSpecs = parsedTable.get().getSplitSpecs();
                List<DataGenTableStats> splitSpecs = table.get().getSplitSpecs();
                assertEquals(parsedSplitSpecs.size(), splitSpecs.size());

                for (int splitIndex = 0; splitIndex < splitSpecs.size(); splitIndex++) {
                    DataGenTableStats parsedSplitSpec = parsedSplitSpecs.get(splitIndex);
                    DataGenTableStats splitSpec = parsedSplitSpecs.get(splitIndex);
                    for (DataGenColumn column : columns) {
                        String columnName = column.getName();

                        Optional<DataGenColumnStats> parsedColumnSpec = parsedSplitSpec.getColumnStats(columnName);
                        assertTrue(parsedColumnSpec.isPresent());

                        Optional<DataGenColumnStats> columnSpec = splitSpec.getColumnStats(columnName);
                        assertTrue(columnSpec.isPresent());

                        assertEquals(parsedColumnSpec.get(), columnSpec.get());
                    }
                }
            }
        }
    }

    public static final String TEST_SCHEMALIST_JSON = String.join("\n",
            "[",
            "    {",
            "        \"name\": \"first\",",
            "        \"tables\": [",
            "            {",
            "                \"name\": \"ta\",",
            "                \"columns\": [",
            "                    {",
            "                        \"name\": \"u\",",
            "                        \"type\": \"BIGINT\"",
            "                    }",
            "                ],",
            "                \"splitSpecs\": [",
            "                    {",
            "                        \"rowCount\": 10,",
            "                        \"columnStats\": [",
            "                            {",
            "                                \"name\": \"u\",",
            "                                \"stats\": {",
            "                                    \"type\": \"base\",",
            "                                    \"min\": 1,",
            "                                    \"max\": 3,",
            "                                    \"distinctValsCount\": 2",
            "                                 }",
            "                            }",
            "                        ]",
            "                    },",
            "                    {",
            "                        \"rowCount\": 5,",
            "                        \"columnStats\": [",
            "                            {",
            "                                \"name\": \"u\",",
            "                                \"stats\": {",
            "                                    \"type\": \"base\",",
            "                                    \"min\": 4,",
            "                                    \"max\": 9,",
            "                                    \"distinctValsCount\": 3",
            "                                 }",
            "                            }",
            "                        ]",
            "                    }",
            "                ]",
            "            },",
            "            {",
            "                \"name\": \"tb\",",
            "                \"columns\": [",
            "                    {",
            "                        \"name\": \"u\",",
            "                        \"type\": \"INTEGER\"",
            "                    }",
            "                ],",
            "                \"splitSpecs\": [",
            "                    {",
            "                        \"rowCount\": 10,",
            "                        \"columnStats\": [",
            "                            {",
            "                                \"name\": \"u\",",
            "                                \"stats\": {",
            "                                    \"type\": \"base\",",
            "                                    \"min\": 1,",
            "                                    \"max\": 3,",
            "                                    \"distinctValsCount\": 2",
            "                                 }",
            "                            }",
            "                        ]",
            "                    },",
            "                    {",
            "                        \"rowCount\": 5,",
            "                        \"columnStats\": [",
            "                            {",
            "                                \"name\": \"u\",",
            "                                \"stats\": {",
            "                                    \"type\": \"base\",",
            "                                    \"min\": 4,",
            "                                    \"max\": 9,",
            "                                    \"distinctValsCount\": 3",
            "                                 }",
            "                            }",
            "                        ]",
            "                    }",
            "                ]",
            "            }",
            "        ]",
            "    },",
            "    {",
            "        \"name\": \"second\",",
            "        \"tables\": [",
            "            {",
            "                \"name\": \"t\",",
            "                \"columns\": [",
            "                    {",
            "                        \"name\": \"u\",",
            "                        \"type\": \"INTEGER\"",
            "                    },",
            "                    {",
            "                        \"name\": \"v\",",
            "                        \"type\": \"DOUBLE\"",
            "                    },",
            "                    {",
            "                        \"name\": \"w\",",
            "                        \"type\": \"VARCHAR\"",
            "                    }",
            "                ],",
            "                \"splitSpecs\": [",
            "                    {",
            "                        \"rowCount\": 10,",
            "                        \"columnStats\": [",
            "                            {",
            "                                \"name\": \"u\",",
            "                                \"stats\": {",
            "                                    \"type\": \"base\",",
            "                                    \"min\": 1,",
            "                                    \"max\": 3,",
            "                                    \"distinctValsCount\": 2",
            "                                 }",
            "                            },",
            "                            {",
            "                                \"name\": \"v\",",
            "                                \"stats\": {",
            "                                    \"type\": \"base\",",
            "                                    \"min\": 5.5,",
            "                                    \"max\": 7.0,",
            "                                    \"distinctValsCount\": 3",
            "                                 }",
            "                            },",
            "                            {",
            "                                \"name\": \"w\",",
            "                                \"stats\": {",
            "                                    \"type\": \"base\",",
            "                                    \"min\": \"BAX\",",
            "                                    \"max\": \"CZX\",",
            "                                    \"distinctValsCount\": 3",
            "                                 }",
            "                            }",
            "                        ]",
            "                    }",
            "                ]",
            "            }",
            "        ]",
            "    }",
            "]");
}
