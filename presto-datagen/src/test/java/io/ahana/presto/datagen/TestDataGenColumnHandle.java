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

import com.facebook.airlift.testing.EquivalenceTester;
import org.testng.annotations.Test;

import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.common.type.DoubleType.DOUBLE;
import static com.facebook.presto.common.type.IntegerType.INTEGER;
import static com.facebook.presto.common.type.VarcharType.createUnboundedVarcharType;
import static io.ahana.presto.datagen.MetadataUtil.COLUMN_CODEC;
import static org.testng.Assert.assertEquals;

public class TestDataGenColumnHandle
{
    @Test
    public void testJsonRoundTrip()
    {
        DataGenColumnHandle columnHandle = new DataGenColumnHandle("columnName", createUnboundedVarcharType(), 0);

        String json = COLUMN_CODEC.toJson(columnHandle);
        DataGenColumnHandle copy = COLUMN_CODEC.fromJson(json);
        assertEquals(copy, columnHandle);
    }

    @Test
    public void testEquivalence()
    {
        EquivalenceTester.equivalenceTester()
                .addEquivalentGroup(
                        new DataGenColumnHandle("columnName", createUnboundedVarcharType(), 0),
                        new DataGenColumnHandle("columnName", INTEGER, 0),
                        new DataGenColumnHandle("columnName", BIGINT, 0),
                        new DataGenColumnHandle("columnName", DOUBLE, 1))
                .check();
    }
}
