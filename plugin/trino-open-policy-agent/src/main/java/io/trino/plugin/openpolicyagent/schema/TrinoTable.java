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
import com.fasterxml.jackson.annotation.JsonUnwrapped;
import com.google.common.collect.ImmutableSet;
import io.trino.spi.connector.CatalogSchemaTableName;

import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

import static java.util.Objects.requireNonNullElse;

@JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.ANY)
public record TrinoTable(@JsonUnwrapped TrinoSchema catalogSchema, String tableName,
                         @JsonInclude(JsonInclude.Include.NON_ABSENT) Map<String, Object> properties,
                         Set<String> columns)
{
    public static class Builder
    {
        public String catalogName;
        public String schemaName;
        public String tableName;
        public Map<String, Object> properties;
        public Set<String> columns;

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

        private <T> Builder propertiesWithGetter(
                Map<String, T> properties,
                Function<T, Optional<Object>> optionalBuilder)
        {
            // https://openjdk.org/jeps/269
            // ImmutableMap along with other new collections does not support null
            // cast nulls to empty optionals
            this.properties = properties
                    .entrySet()
                    .stream()
                    .map((e) -> Map.entry(e.getKey(), optionalBuilder.apply(e.getValue())))
                    .collect(Collectors.toUnmodifiableMap(Map.Entry::getKey, Map.Entry::getValue));
            return this;
        }

        public Builder properties(Map<String, Object> properties)
        {
            return propertiesWithGetter(properties, Optional::ofNullable);
        }

        public Builder optionalProperties(Map<String, Optional<Object>> properties)
        {
            return propertiesWithGetter(properties, (i) -> requireNonNullElse(i, Optional.empty()));
        }

        public Builder tableName(String tableName)
        {
            this.tableName = tableName;
            return this;
        }

        public Builder columns(Set<String> columns)
        {
            this.columns = ImmutableSet.copyOf(columns);
            return this;
        }

        public static Builder fromTrinoTable(CatalogSchemaTableName table)
        {
            return new Builder()
                    .catalogName(table.getCatalogName())
                    .schemaName(table.getSchemaTableName().getSchemaName())
                    .tableName(table.getSchemaTableName().getTableName());
        }

        public TrinoTable build()
        {
            return new TrinoTable(this);
        }
    }

    public TrinoTable(Builder builder)
    {
        this(
                new TrinoSchema.Builder()
                        .catalogName(builder.catalogName)
                        .schemaName(builder.schemaName)
                        .build(),
                builder.tableName, builder.properties, builder.columns);
    }

    public static TrinoTable fromTrinoTable(CatalogSchemaTableName table)
    {
        return TrinoTable.Builder.fromTrinoTable(table).build();
    }
}
