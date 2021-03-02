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

import java.util.Optional;

import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.common.type.DoubleType.DOUBLE;
import static io.ahana.presto.datagen.MetadataUtil.CATALOG_CODEC;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

public class TestDataGen
{
    public static final String TEST_CATALOG_FILENAME = "test.json";

    @Test
    public void testMetadata()
            throws Exception
    {
        DataGen dataGen = new DataGen(new DataGenConfig().setCatalogFileName(TEST_CATALOG_FILENAME), CATALOG_CODEC);
        assertEquals(dataGen.getSchemaNames(), ImmutableSet.of("FIRST", "SECOND"));
        assertEquals(dataGen.getTableNames("FIRST"), ImmutableSet.of("TA", "TB"));
        assertEquals(dataGen.getTableNames("SECOND"), ImmutableSet.of("TB"));

        Optional<DataGenTable> tableOpt = dataGen.getTable("FIRST", "TA");
        assertTrue(tableOpt.isPresent(), "TA is not present");

        DataGenTable table = tableOpt.get();
        assertEquals(table.getName(), "TA");
        assertEquals(table.getColumns(), ImmutableList.of(new DataGenColumn("X", BIGINT), new DataGenColumn("Y", DOUBLE)));
    }
}
