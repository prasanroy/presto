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
        assertEquals(dataGenCatalog.getSchemaNames(), ImmutableSet.of("FIRST", "SECOND"));
        assertEquals(dataGenCatalog.getTableNames("FIRST"), ImmutableSet.of("TA", "TB"));
        assertEquals(dataGenCatalog.getTableNames("SECOND"), ImmutableSet.of("TB"));

        Optional<DataGenTable> tableOpt = dataGenCatalog.getTable("FIRST", "TA");
        assertTrue(tableOpt.isPresent(), "TA is not present");

        DataGenTable table = tableOpt.get();
        assertEquals(table.getName(), "TA");
        assertEquals(table.getColumns(), ImmutableList.of(new DataGenColumn("X", BIGINT), new DataGenColumn("Y", DOUBLE)));
    }

    public static final List<DataGenSchema> TEST_CATALOG = ImmutableList.of(
            new DataGenSchema(
                "FIRST",
                ImmutableList.of(
                    new DataGenTable(
                        "TA",
                        ImmutableList.of(
                            new DataGenColumn("X", BIGINT),
                            new DataGenColumn("Y", DOUBLE)),
                        ImmutableList.of(
                            new DataGenTableStats(
                                10,
                                ImmutableList.of(
                                    new DataGenColumnStats("X", 1, 3, 2),
                                    new DataGenColumnStats("Y", 5.5, 7.0, 3))),
                            new DataGenTableStats(
                                4,
                                ImmutableList.of(
                                    new DataGenColumnStats("X", 4, 9, 3),
                                    new DataGenColumnStats("Y", 0.0, 9.0, 5))))),
                    new DataGenTable(
                        "TB",
                        ImmutableList.of(
                            new DataGenColumn("U", INTEGER)),
                        ImmutableList.of(
                            new DataGenTableStats(
                                10,
                                ImmutableList.of(
                                    new DataGenColumnStats("U", 1, 3, 2))),
                            new DataGenTableStats(
                                5,
                                ImmutableList.of(
                                    new DataGenColumnStats("U", 4, 9, 3))))))),
            new DataGenSchema(
                "SECOND",
                ImmutableList.of(
                    new DataGenTable(
                        "TB",
                        ImmutableList.of(
                            new DataGenColumn("U", INTEGER),
                            new DataGenColumn("V", DOUBLE)),
                        ImmutableList.of(
                            new DataGenTableStats(
                                10,
                                ImmutableList.of(
                                    new DataGenColumnStats("U", 1, 3, 2),
                                    new DataGenColumnStats("V", 5.5, 7.0, 3))))))));
}
