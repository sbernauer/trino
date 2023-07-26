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

import com.fasterxml.jackson.annotation.JsonInclude;

import java.util.Set;

import static com.google.common.collect.ImmutableSet.toImmutableSet;

@JsonInclude(JsonInclude.Include.NON_NULL)
public record OpaQueryInputResource(
        TrinoUser user,
        NamedEntity systemSessionProperty,
        NamedEntity catalogSessionProperty,
        TrinoFunction function,
        NamedEntity catalog,
        TrinoSchema schema,
        TrinoTable table,
        NamedEntity role,
        Set<NamedEntity> roles)
{
    @JsonInclude
    public record NamedEntity(String name)
    { }

    public OpaQueryInputResource(OpaQueryInputResource.Builder builder)
    {
        this(
                builder.user,
                builder.systemSessionProperty,
                builder.catalogSessionProperty,
                builder.function,
                builder.catalog,
                builder.schema,
                builder.table,
                builder.role,
                builder.roles);
    }

    public static class Builder
    {
        private TrinoUser user;
        private NamedEntity systemSessionProperty;
        private NamedEntity catalogSessionProperty;
        private NamedEntity catalog;
        private TrinoSchema schema;
        private TrinoTable table;
        private NamedEntity role;
        private Set<NamedEntity> roles;
        private TrinoFunction function;

        public Builder user(TrinoUser user)
        {
            this.user = user;
            return this;
        }

        public Builder systemSessionProperty(String systemSessionProperty)
        {
            this.systemSessionProperty = new NamedEntity(systemSessionProperty);
            return this;
        }

        public Builder catalogSessionProperty(String catalogSessionProperty)
        {
            this.catalogSessionProperty = new NamedEntity(catalogSessionProperty);
            return this;
        }

        public Builder catalog(String catalog)
        {
            this.catalog = new NamedEntity(catalog);
            return this;
        }

        public Builder schema(TrinoSchema schema)
        {
            this.schema = schema;
            return this;
        }

        public Builder table(TrinoTable table)
        {
            this.table = table;
            return this;
        }

        public Builder role(String role)
        {
            this.role = new NamedEntity(role);
            return this;
        }

        public Builder roles(Set<String> roles)
        {
            this.roles = roles.stream().map(NamedEntity::new).collect(toImmutableSet());
            return this;
        }

        public Builder function(TrinoFunction function)
        {
            this.function = function;
            return this;
        }

        public Builder function(String functionName)
        {
            this.function = new TrinoFunction(functionName);
            return this;
        }

        public OpaQueryInputResource build()
        {
            return new OpaQueryInputResource(this);
        }
    }
}
