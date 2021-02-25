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

import java.net.URL;

public class DataGenConfig
{
    private URL metadataUrl;

    @NotNull
    public URL getMetadataUrl()
    {
        return metadataUrl;
    }

    @Config("metadata")
    public DataGenConfig setMetadataFile(String metadataResourceName)
    {
        this.metadataUrl = Resources.getResource(metadataResourceName);
        return this;
    }
}
