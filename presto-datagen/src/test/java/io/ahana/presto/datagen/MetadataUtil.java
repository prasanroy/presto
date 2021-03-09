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
import com.facebook.airlift.json.JsonCodecFactory;
import com.facebook.airlift.json.ObjectMapperProvider;
import com.facebook.presto.common.type.ArrayType;
import com.facebook.presto.common.type.RowType;
import com.facebook.presto.common.type.StandardTypes;
import com.facebook.presto.common.type.Type;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.deser.std.FromStringDeserializer;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import java.util.List;
import java.util.Map;

import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.common.type.BooleanType.BOOLEAN;
import static com.facebook.presto.common.type.DoubleType.DOUBLE;
import static com.facebook.presto.common.type.IntegerType.INTEGER;
import static com.facebook.presto.common.type.VarcharType.VARCHAR;
import static java.util.Locale.ENGLISH;

public final class MetadataUtil
{
    private MetadataUtil()
    {
    }

    public static final JsonCodec<List<DataGenSchema>> SCHEMALIST_CODEC;
    public static final JsonCodec<DataGenTable> TABLE_CODEC;
    public static final JsonCodec<DataGenColumnHandle> COLUMN_CODEC;
    public static final JsonCodec<DataGenColumnStats> COLUMNSTATS_CODEC;
    public static final JsonCodec<DataGenTableStats> TABLESTATS_CODEC;

    static {
        ObjectMapperProvider objectMapperProvider = new ObjectMapperProvider();
        objectMapperProvider.setJsonDeserializers(ImmutableMap.of(Type.class, new TestingTypeDeserializer()));
        JsonCodecFactory codecFactory = new JsonCodecFactory(objectMapperProvider);
        SCHEMALIST_CODEC = codecFactory.listJsonCodec(DataGenSchema.class);
        TABLE_CODEC = codecFactory.jsonCodec(DataGenTable.class);
        COLUMN_CODEC = codecFactory.jsonCodec(DataGenColumnHandle.class);
        COLUMNSTATS_CODEC = codecFactory.jsonCodec(DataGenColumnStats.class);
        TABLESTATS_CODEC = codecFactory.jsonCodec(DataGenTableStats.class);
    }

    public static final class TestingTypeDeserializer
            extends FromStringDeserializer<Type>
    {
        private final Map<String, Type> types;

        public TestingTypeDeserializer()
        {
            super(Type.class);

            ImmutableMap.Builder<String, Type> typeMapBuilder = ImmutableMap.builder();
            typeMapBuilder.put(StandardTypes.BOOLEAN, BOOLEAN);
            typeMapBuilder.put(StandardTypes.BIGINT, BIGINT);
            typeMapBuilder.put(StandardTypes.INTEGER, INTEGER);
            typeMapBuilder.put(StandardTypes.DOUBLE, DOUBLE);
            typeMapBuilder.put(StandardTypes.VARCHAR, VARCHAR);
            typeMapBuilder.put("row(x bigint, y varchar)", RowType.from(ImmutableList.of(RowType.field("x", BIGINT), RowType.field("y", VARCHAR))));
            typeMapBuilder.put("array(row(x bigint, y varchar))", new ArrayType(RowType.from(ImmutableList.of(RowType.field("x", BIGINT), RowType.field("y", VARCHAR)))));

            this.types = typeMapBuilder.build();
        }

        @Override
        protected Type _deserialize(String value, DeserializationContext context)
        {
            Type type = types.get(value.toLowerCase(ENGLISH));
            if (type == null) {
                throw new IllegalArgumentException(String.valueOf("Unknown type " + value));
            }
            return type;
        }
    }
}
