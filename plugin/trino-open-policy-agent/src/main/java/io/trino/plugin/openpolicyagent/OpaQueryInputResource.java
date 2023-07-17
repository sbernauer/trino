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
package io.trino.plugin.openpolicyagent;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonUnwrapped;
import io.trino.spi.connector.CatalogSchemaName;
import io.trino.spi.connector.CatalogSchemaTableName;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.function.FunctionKind;
import io.trino.spi.security.Identity;

import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

@JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.ANY)
public class OpaQueryInputResource
{
    public User user;
    public NamedEntity systemSessionProperty;
    public NamedEntity catalogSessionProperty;
    public Function function;
    public NamedEntity catalog;
    public CatalogSchema schema;
    public Table table;
    public NamedEntity role;
    public Set<NamedEntity> roles;

    @JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.ANY)
    public static class NamedEntity
    {
        public String name;

        public NamedEntity(String name)
        {
            this.name = name;
        }
    }

    @JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.ANY)
    public static class Function
    {
        public String name;
        public FunctionKind functionKind;

        public Function(String functionName)
        {
            this.name = functionName;
        }

        public Function(String functionName, FunctionKind functionKind)
        {
            this.name = functionName;
            this.functionKind = functionKind;
        }
    }

    @JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.ANY)
    public static class User
    {
        public String name;
        @JsonUnwrapped
        public OpaIdentity identity;

        public User(String name)
        {
            this.name = name;
        }

        public User(Identity identity)
        {
            this.identity = OpaIdentity.fromTrinoIdentity(identity);
            this.name = identity.getUser();
        }
    }

    @JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.ANY)
    public static class CatalogSchema
    {
        public String catalogName;
        public String schemaName;
        @JsonInclude(JsonInclude.Include.NON_ABSENT)
        public Map<String, Object> properties;

        public CatalogSchema(CatalogSchemaName schema)
        {
            this(schema.getCatalogName(), schema.getSchemaName());
        }

        public CatalogSchema(CatalogSchemaName schema, Map<String, Object> properties)
        {
            this(schema);
            this.properties = Map.copyOf(properties);
        }

        public CatalogSchema(String catalogName, String schemaName)
        {
            this.catalogName = catalogName;
            this.schemaName = schemaName;
        }
    }

    @JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.ANY)
    public static class Table
    {
        @JsonUnwrapped
        public CatalogSchema catalogSchema;
        public String tableName;
        @JsonInclude(JsonInclude.Include.NON_ABSENT)
        public Map<String, Object> properties;
        public Set<String> columns;

        public Table(CatalogSchemaTableName catalogSchemaTableName)
        {
            this.catalogSchema = new CatalogSchema(
                    catalogSchemaTableName.getCatalogName(),
                    catalogSchemaTableName.getSchemaTableName().getSchemaName());
            this.tableName = catalogSchemaTableName.getSchemaTableName().getTableName();
        }

        public Table(CatalogSchemaTableName catalogSchemaTableName, Map<String, Object> properties)
        {
            this(catalogSchemaTableName);
            this.properties = properties;
        }

        public Table(String catalogName, String schemaName, String tableName)
        {
            this.catalogSchema = new CatalogSchema(catalogName, schemaName);
            this.tableName = tableName;
        }

        public Table(String catalogName, SchemaTableName schemaTableName)
        {
            this(catalogName, schemaTableName.getSchemaName(), schemaTableName.getTableName());
        }

        public Table(CatalogSchemaTableName catalogSchemaTableName, Set<String> columns)
        {
            this(catalogSchemaTableName);
            this.columns = columns;
        }
    }

    public OpaQueryInputResource(OpaQueryInputResource.Builder builder)
    {
        this.user = builder.user;
        this.systemSessionProperty = builder.systemSessionProperty;
        this.catalog = builder.catalog;
        this.schema = builder.schema;
        this.table = builder.table;
        this.role = builder.role;
        this.catalogSessionProperty = builder.catalogSessionProperty;
        this.function = builder.function;
        this.roles = builder.roles;
    }

    public static class Builder
    {
        private User user;
        private NamedEntity systemSessionProperty;
        private NamedEntity catalogSessionProperty;
        private NamedEntity catalog;
        private CatalogSchema schema;
        private Table table;
        private NamedEntity role;
        private Set<NamedEntity> roles;
        private Function function;

        public Builder user(User user)
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

        public Builder schema(CatalogSchema schema)
        {
            this.schema = schema;
            return this;
        }

        public Builder table(Table table)
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
            this.roles = roles.stream().map(NamedEntity::new).collect(Collectors.toSet());
            return this;
        }

        public Builder function(String functionName)
        {
            this.function = new Function(functionName);
            return this;
        }

        public Builder function(Function function)
        {
            this.function = function;
            return this;
        }

        public OpaQueryInputResource build()
        {
            return new OpaQueryInputResource(this);
        }
    }
}
