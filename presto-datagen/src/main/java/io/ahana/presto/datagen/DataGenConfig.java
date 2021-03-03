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

import com.facebook.airlift.configuration.Config;
import com.google.common.io.Resources;

import javax.validation.constraints.NotNull;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.URL;

import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Objects.requireNonNull;

public class DataGenConfig
{
    private String catalogFileName;

    @Config("datagen.catalog.file.name")
    public DataGenConfig setCatalogFileName(String catalogFileName)
    {
        this.catalogFileName = requireNonNull(catalogFileName);
        return this;
    }

    @NotNull
    public String getCatalogFileName()
    {
        return catalogFileName;
    }

    @NotNull
    public String getCatalogJson()
    {
        String catalogJson;

        requireNonNull(catalogFileName, "catalogFileName is null");
        try {
            URL catalogJsonUrl = Resources.getResource(catalogFileName);
            catalogJson = Resources.toString(catalogJsonUrl, UTF_8);
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }

        return catalogJson;
    }
}
