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

import com.facebook.presto.common.type.Type;

public interface ValueCursor
{
    /**
     * Get the Presto type of the values returned
     */
    Type getValueType();

    /**
     * Is the current value NULL?
     */
    boolean isNull();

    /**
     * Get the current value
     */
    Object getValue();

    /**
     * Advance cursor to the next position
     */
    void advanceNextPosition();
}
