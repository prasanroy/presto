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

import com.facebook.presto.common.type.ArrayType;
import com.facebook.presto.common.type.RowType;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import org.testng.annotations.Test;

import java.util.Optional;

import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.common.type.DoubleType.DOUBLE;
import static com.facebook.presto.common.type.IntegerType.INTEGER;
import static com.facebook.presto.common.type.VarcharType.VARCHAR;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

public class TestDataGenCatalog
{
    @Test
    public void testMetadata()
            throws Exception
    {
        assertEquals(TEST_CATALOG.getSchemaNames(), ImmutableSet.of("first", "second"));
        assertEquals(TEST_CATALOG.getTableNames("first"), ImmutableSet.of("ta", "tb"));
        assertEquals(TEST_CATALOG.getTableNames("second"), ImmutableSet.of("t"));

        Optional<DataGenTable> tableOpt = TEST_CATALOG.getTable("second", "t");
        assertTrue(tableOpt.isPresent(), "Table 't' is not present");

        DataGenTable table = tableOpt.get();
        assertEquals(table.getName(), "t");
        assertEquals(table.getColumns(), ImmutableList.of(new DataGenColumn("u", INTEGER), new DataGenColumn("v", DOUBLE), new DataGenColumn("w", VARCHAR)));
    }

    public static final DataGenCatalog TEST_CATALOG = new DataGenCatalog(
            ImmutableList.of(
                new DataGenSchema(
                    "first",
                    ImmutableList.of(
                        new DataGenTable(
                            "ta",
                            ImmutableList.of(
                                new DataGenColumn("u", RowType.from(ImmutableList.of(RowType.field("x", BIGINT), RowType.field("y", VARCHAR))))),
                            ImmutableList.of(
                                new DataGenTableStats(
                                    10,
                                    ImmutableList.of(
                                        new DataGenNamedColumnStats(
                                            "u",
                                            new DataGenRowColumnStats(
                                                ImmutableList.of(
                                                    new DataGenNamedColumnStats("x", new DataGenBaseColumnStats(1, 3, 2)),
                                                    new DataGenNamedColumnStats("y", new DataGenBaseColumnStats("BILL", "JEFF", 7))))))))),
                        new DataGenTable(
                            "tb",
                            ImmutableList.of(
                                new DataGenColumn("v", new ArrayType(RowType.from(ImmutableList.of(RowType.field("x", BIGINT), RowType.field("y", VARCHAR)))))),
                            ImmutableList.of(
                                new DataGenTableStats(
                                    10,
                                    ImmutableList.of(
                                        new DataGenNamedColumnStats(
                                            "v",
                                            new DataGenArrayColumnStats(
                                                3,
                                                new DataGenRowColumnStats(
                                                    ImmutableList.of(
                                                        new DataGenNamedColumnStats("x", new DataGenBaseColumnStats(1, 3, 2)),
                                                        new DataGenNamedColumnStats("y", new DataGenBaseColumnStats("BEZOS", "GATES", 7)))))))))))),
                new DataGenSchema(
                    "second",
                    ImmutableList.of(
                        new DataGenTable(
                            "t",
                            ImmutableList.of(
                                new DataGenColumn("u", INTEGER),
                                new DataGenColumn("v", DOUBLE),
                                new DataGenColumn("w", VARCHAR)),
                            ImmutableList.of(
                                new DataGenTableStats(
                                    5,
                                    ImmutableList.of(
                                        new DataGenNamedColumnStats("u", new DataGenBaseColumnStats(1, 3, 2)),
                                        new DataGenNamedColumnStats("v", new DataGenBaseColumnStats(5.5, 7.0, 3)),
                                        new DataGenNamedColumnStats("w", new DataGenBaseColumnStats("BAX", "CZX", 3))))))))));
}
