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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import org.testng.annotations.Test;

import java.util.List;
import java.util.Optional;

import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.common.type.DoubleType.DOUBLE;
import static com.facebook.presto.common.type.IntegerType.INTEGER;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

public class TestDataGenCatalog
{
    @Test
    public void testMetadata()
            throws Exception
    {
        DataGenCatalog dataGenCatalog = new DataGenCatalog(TEST_CATALOG);
        assertEquals(dataGenCatalog.getSchemaNames(), ImmutableSet.of("first", "second"));
        assertEquals(dataGenCatalog.getTableNames("first"), ImmutableSet.of("ta", "tb"));
        assertEquals(dataGenCatalog.getTableNames("second"), ImmutableSet.of("tb"));

        Optional<DataGenTable> tableOpt = dataGenCatalog.getTable("first", "ta");
        assertTrue(tableOpt.isPresent(), "Table 'ta' is not present");

        DataGenTable table = tableOpt.get();
        assertEquals(table.getName(), "ta");
        assertEquals(table.getColumns(), ImmutableList.of(new DataGenColumn("x", BIGINT), new DataGenColumn("y", DOUBLE)));
    }

    public static final List<DataGenSchema> TEST_CATALOG = ImmutableList.of(
            new DataGenSchema(
                "first",
                ImmutableList.of(
                    new DataGenTable(
                        "ta",
                        ImmutableList.of(
                            new DataGenColumn("x", BIGINT),
                            new DataGenColumn("y", DOUBLE)),
                        ImmutableList.of(
                            new DataGenTableStats(
                                10,
                                ImmutableList.of(
                                    new DataGenColumnStats("x", 1, 3, 2),
                                    new DataGenColumnStats("y", 5.5, 7.0, 3))),
                            new DataGenTableStats(
                                4,
                                ImmutableList.of(
                                    new DataGenColumnStats("x", 4, 9, 3),
                                    new DataGenColumnStats("y", 0.0, 9.0, 5))))),
                    new DataGenTable(
                        "tb",
                        ImmutableList.of(
                            new DataGenColumn("u", INTEGER)),
                        ImmutableList.of(
                            new DataGenTableStats(
                                10,
                                ImmutableList.of(
                                    new DataGenColumnStats("u", 1, 3, 2))),
                            new DataGenTableStats(
                                5,
                                ImmutableList.of(
                                    new DataGenColumnStats("u", 4, 9, 3))))))),
            new DataGenSchema(
                "second",
                ImmutableList.of(
                    new DataGenTable(
                        "tb",
                        ImmutableList.of(
                            new DataGenColumn("u", INTEGER),
                            new DataGenColumn("v", DOUBLE)),
                        ImmutableList.of(
                            new DataGenTableStats(
                                10,
                                ImmutableList.of(
                                    new DataGenColumnStats("u", 1, 3, 2),
                                    new DataGenColumnStats("v", 5.5, 7.0, 3))))))));
}
