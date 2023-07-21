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
package io.trino.plugin.openpolicyagent.schema;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonInclude;
import io.trino.spi.connector.CatalogSchemaName;

import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

// TODO (PABLO) Try to abstract to a generic class shared with TrinoTable
@JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.ANY)
public record TrinoSchema(String catalogName, String schemaName,
                          @JsonInclude(JsonInclude.Include.NON_ABSENT) Map<String, Object> properties)
{
    public static class Builder
    {
        public String catalogName;
        public String schemaName;
        public Map<String, Object> properties;

        public Builder catalogName(String catalogName)
        {
            this.catalogName = catalogName;
            return this;
        }

        public Builder schemaName(String schemaName)
        {
            this.schemaName = schemaName;
            return this;
        }

        public Builder properties(Map<String, Object> properties)
        {
            // https://openjdk.org/jeps/269
            // ImmutableMap along with other new collections does not support null
            // cast nulls to empty optionals
            this.properties = properties
                    .entrySet()
                    .stream()
                    .map((e) -> Map.entry(e.getKey(), Optional.ofNullable(e.getValue())))
                    .collect(Collectors.toUnmodifiableMap(Map.Entry::getKey, Map.Entry::getValue));
            return this;
        }

        public static Builder fromTrinoCatalogSchema(CatalogSchemaName catalogSchemaName)
        {
            return new Builder()
                    .catalogName(catalogSchemaName.getCatalogName())
                    .schemaName(catalogSchemaName.getSchemaName());
        }

        public TrinoSchema build()
        {
            return new TrinoSchema(this);
        }
    }

    public static TrinoSchema fromTrinoCatalogSchema(CatalogSchemaName catalogSchemaName)
    {
        return Builder.fromTrinoCatalogSchema(catalogSchemaName).build();
    }

    public TrinoSchema(Builder builder)
    {
        this(builder.catalogName, builder.schemaName, builder.properties);
    }
}
