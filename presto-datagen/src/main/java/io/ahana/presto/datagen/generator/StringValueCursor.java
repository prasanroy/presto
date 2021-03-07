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
package io.ahana.presto.datagen.generator;

import com.facebook.presto.common.type.VarcharType;
import io.ahana.presto.datagen.DataGenBaseColumnStats;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;

import java.util.Optional;

import static com.facebook.presto.common.type.BigintType.BIGINT;
import static java.util.Objects.requireNonNull;

public class StringValueCursor
        implements ValueCursor
{
    private final LongValueCursor longValueCursor;

    public StringValueCursor(
            long min, long max, Optional<Long> distinctValsCountOpt)
    {
        this.longValueCursor = new LongValueCursor(BIGINT, min, max, distinctValsCountOpt);
    }

    @Override
    public VarcharType getValueType()
    {
        return VarcharType.VARCHAR;
    }

    @Override
    public Slice getValue()
    {
        return Slices.utf8Slice(decode(longValueCursor.getValue()));
    }

    @Override
    public boolean isNull()
    {
        return false;
    }

    @Override
    public void advanceNextPosition()
    {
        longValueCursor.advanceNextPosition();
    }

    private static String decode(long v)
    {
        if (v <= 0) {
            return "";
        }

        long r = v % 26;
        String s = Character.toString((char) ('A' + r));

        long q = v / 26;
        if (q == 0) {
            return s;
        }

        return decode(q) + s;
    }

    private static long encode(String s)
    {
        if (s.isEmpty()) {
            return 0;
        }

        int endIndex = s.length() - 1;
        char lastChar = s.charAt(endIndex);

        long rem = encode(s.substring(0, endIndex));

        if (lastChar < 'A' || lastChar > 'Z') {
            return rem;
        }

        long v = 26 * rem + (lastChar - 'A');
        if (v < 0) { // overflow -- ignore lastChar
            return rem;
        }

        return v;
    }

    public static StringValueCursor create(DataGenBaseColumnStats columnSpec)
    {
        requireNonNull(columnSpec, "columnSpec is null");

        long min = encode(columnSpec.getMin().orElse("").toString());
        long max = columnSpec.getMax().map(s -> encode(s.toString())).orElse(Long.MAX_VALUE);

        return new StringValueCursor(min, max, columnSpec.getDistinctValsCount());
    }
}
