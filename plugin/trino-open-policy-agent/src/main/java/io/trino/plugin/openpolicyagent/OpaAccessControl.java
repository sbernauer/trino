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

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module;
import com.google.inject.Inject;
import io.trino.spi.connector.CatalogSchemaName;
import io.trino.spi.connector.CatalogSchemaRoutineName;
import io.trino.spi.connector.CatalogSchemaTableName;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.eventlistener.EventListener;
import io.trino.spi.function.FunctionKind;
import io.trino.spi.security.Identity;
import io.trino.spi.security.Privilege;
import io.trino.spi.security.SystemAccessControl;
import io.trino.spi.security.SystemSecurityContext;
import io.trino.spi.security.TrinoPrincipal;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.security.Principal;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

public class OpaAccessControl
        implements SystemAccessControl
{
    private final HttpClient httpClient;
    private final ObjectMapper json;
    private final URI opaPolicyUri;

    @Inject
    public OpaAccessControl(OpaConfig config)
    {
        this(config, HttpClient.newHttpClient());
    }

    public OpaAccessControl(OpaConfig config, HttpClient httpClient)
    {
        this.opaPolicyUri = config.getOpaUri();
        this.json = new ObjectMapper();
        // do not include null values
        this.json.setSerializationInclusion(JsonInclude.Include.NON_NULL);
        // deal with Optional<T> values
        this.json.registerModule(new Jdk8Module());
        this.httpClient = httpClient;
    }

    protected <T> T tryGetResponseFromOpa(OpaQueryInput input, URI uri, Class<T> cls)
    {
        byte[] queryJson;
        OpaQuery query = new OpaQuery(input);

        try {
            queryJson = json.writeValueAsBytes(query);
        }
        catch (JsonProcessingException e) {
            throw new OpaQueryException.SerializeFailed(e);
        }
        HttpResponse<String> response;
        try {
            response = httpClient.send(HttpRequest.newBuilder(uri).header("Content-Type", "application/json").POST(HttpRequest.BodyPublishers.ofByteArray(queryJson)).build(), HttpResponse.BodyHandlers.ofString());
        }
        catch (Exception e) {
            throw new OpaQueryException.QueryFailed(e);
        }
        switch (response.statusCode()) {
            case 200:
                break;
            case 404:
                throw new OpaQueryException.PolicyNotFound(opaPolicyUri.toString());
            default:
                throw new OpaQueryException.OpaServerError(opaPolicyUri.toString(), response);
        }
        String body = response.body();
        try {
            return json.readValue(body, cls);
        }
        catch (Exception e) {
            throw new OpaQueryException.DeserializeFailed(e);
        }
    }

    protected boolean queryOpa(OpaQueryInput input)
    {
        OpaQueryResult result = tryGetResponseFromOpa(input, opaPolicyUri, OpaQueryResult.class);
        if (result.result == null) {
            return false;
        }
        return result.result;
    }

    protected boolean queryOpaWithSimpleAction(SystemSecurityContext context, String operation)
    {
        OpaQueryInputAction action = new OpaQueryInputAction.Builder().operation(operation).build();
        OpaQueryInput input = new OpaQueryInput(context, action);
        return queryOpa(input);
    }

    protected boolean queryOpaWithSimpleResource(SystemSecurityContext context, String operation, OpaQueryInputResource resource)
    {
        OpaQueryInputAction action = new OpaQueryInputAction.Builder().operation(operation).resource(resource).build();
        OpaQueryInput input = new OpaQueryInput(context, action);
        return queryOpa(input);
    }

    @Override
    public void checkCanImpersonateUser(SystemSecurityContext context, String userName)
    {
        OpaQueryInputResource resource = new OpaQueryInputResource.Builder().user(new OpaQueryInputResource.User(userName)).build();

        if (!queryOpaWithSimpleResource(context, "ImpersonateUser", resource)) {
            SystemAccessControl.super.checkCanImpersonateUser(context, userName);
        }
    }

    @Override
    public void checkCanSetUser(Optional<Principal> principal, String userName)
    {
        // This method is deprecated and is called for any identity, let's no-op
    }

    @Override
    public void checkCanExecuteQuery(SystemSecurityContext context)
    {
        if (!queryOpaWithSimpleAction(context, "ExecuteQuery")) {
            SystemAccessControl.super.checkCanExecuteQuery(context);
        }
    }

    @Override
    public void checkCanViewQueryOwnedBy(SystemSecurityContext context, Identity queryOwner)
    {
        OpaQueryInputResource resource = new OpaQueryInputResource.Builder().user(new OpaQueryInputResource.User(queryOwner)).build();
        if (!queryOpaWithSimpleResource(context, "ViewQueryOwnedBy", resource)) {
            SystemAccessControl.super.checkCanViewQueryOwnedBy(context, queryOwner);
        }
    }

    @Override
    public Collection<Identity> filterViewQueryOwnedBy(SystemSecurityContext context, Collection<Identity> queryOwners)
    {
        return queryOwners.parallelStream().filter(queryOwner -> queryOpaWithSimpleResource(context, "FilterViewQueryOwnedBy", new OpaQueryInputResource.Builder().user(new OpaQueryInputResource.User(queryOwner)).build())).collect(Collectors.toSet());
    }

    @Override
    public void checkCanKillQueryOwnedBy(SystemSecurityContext context, Identity queryOwner)
    {
        OpaQueryInputResource resource = new OpaQueryInputResource.Builder().user(new OpaQueryInputResource.User(queryOwner)).build();

        if (!queryOpaWithSimpleResource(context, "KillQueryOwnedBy", resource)) {
            SystemAccessControl.super.checkCanKillQueryOwnedBy(context, queryOwner);
        }
    }

    @Override
    public void checkCanReadSystemInformation(SystemSecurityContext context)
    {
        if (!queryOpaWithSimpleAction(context, "ReadSystemInformation")) {
            SystemAccessControl.super.checkCanReadSystemInformation(context);
        }
    }

    @Override
    public void checkCanWriteSystemInformation(SystemSecurityContext context)
    {
        if (!queryOpaWithSimpleAction(context, "WriteSystemInformation")) {
            SystemAccessControl.super.checkCanWriteSystemInformation(context);
        }
    }

    @Override
    public void checkCanSetSystemSessionProperty(SystemSecurityContext context, String propertyName)
    {
        OpaQueryInputResource resource = new OpaQueryInputResource.Builder().systemSessionProperty(propertyName).build();

        if (!queryOpaWithSimpleResource(context, "SetSystemSessionProperty", resource)) {
            SystemAccessControl.super.checkCanSetSystemSessionProperty(context, propertyName);
        }
    }

    @Override
    public void checkCanAccessCatalog(SystemSecurityContext context, String catalogName)
    {
        OpaQueryInputResource resource = new OpaQueryInputResource.Builder().catalog(catalogName).build();
        if (!queryOpaWithSimpleResource(context, "AccessCatalog", resource)) {
            SystemAccessControl.super.checkCanAccessCatalog(context, catalogName);
        }
    }

    @Override
    public Set<String> filterCatalogs(SystemSecurityContext context, Set<String> catalogs)
    {
        return catalogs.parallelStream().filter(catalog -> queryOpaWithSimpleResource(context, "FilterCatalogs", new OpaQueryInputResource.Builder().catalog(catalog).build())).collect(Collectors.toSet());
    }

    @Override
    public void checkCanCreateSchema(SystemSecurityContext context, CatalogSchemaName schema, Map<String, Object> properties)
    {
        OpaQueryInputResource resource = new OpaQueryInputResource.Builder().schema(new OpaQueryInputResource.CatalogSchema(schema, properties)).build();
        if (!queryOpaWithSimpleResource(context, "CreateSchema", resource)) {
            SystemAccessControl.super.checkCanCreateSchema(context, schema, properties);
        }
    }

    @Override
    public void checkCanDropSchema(SystemSecurityContext context, CatalogSchemaName schema)
    {
        OpaQueryInputResource resource = new OpaQueryInputResource.Builder().schema(new OpaQueryInputResource.CatalogSchema(schema)).build();
        if (!queryOpaWithSimpleResource(context, "DropSchema", resource)) {
            SystemAccessControl.super.checkCanDropSchema(context, schema);
        }
    }

    @Override
    public void checkCanRenameSchema(SystemSecurityContext context, CatalogSchemaName schema, String newSchemaName)
    {
        OpaQueryInputResource resource = new OpaQueryInputResource.Builder().schema(new OpaQueryInputResource.CatalogSchema(schema)).build();
        OpaQueryInputResource targetResource = new OpaQueryInputResource.Builder().schema(new OpaQueryInputResource.CatalogSchema(schema.getCatalogName(), newSchemaName)).build();
        OpaQueryInputAction action = new OpaQueryInputAction.Builder().operation("RenameSchema").resource(resource).targetResource(targetResource).build();
        OpaQueryInput input = new OpaQueryInput(context, action);

        if (!queryOpa(input)) {
            SystemAccessControl.super.checkCanRenameSchema(context, schema, newSchemaName);
        }
    }

    @Override
    public void checkCanSetSchemaAuthorization(SystemSecurityContext context, CatalogSchemaName schema, TrinoPrincipal principal)
    {
        OpaQueryInputResource resource = new OpaQueryInputResource.Builder().schema(new OpaQueryInputResource.CatalogSchema(schema)).build();
        OpaQueryInputGrant grantee = new OpaQueryInputGrant.Builder().principal(principal).build();
        OpaQueryInputAction action = new OpaQueryInputAction.Builder().operation("SetSchemaAuthorization").resource(resource).grantee(grantee).build();
        OpaQueryInput input = new OpaQueryInput(context, action);

        if (!queryOpa(input)) {
            SystemAccessControl.super.checkCanSetSchemaAuthorization(context, schema, principal);
        }
    }

    @Override
    public void checkCanShowSchemas(SystemSecurityContext context, String catalogName)
    {
        OpaQueryInputResource resource = new OpaQueryInputResource.Builder().catalog(catalogName).build();
        if (!queryOpaWithSimpleResource(context, "ShowSchemas", resource)) {
            SystemAccessControl.super.checkCanShowSchemas(context, catalogName);
        }
    }

    @Override
    public Set<String> filterSchemas(SystemSecurityContext context, String catalogName, Set<String> schemaNames)
    {
        return schemaNames.parallelStream().filter(schemaName -> queryOpaWithSimpleResource(context, "FilterSchemas", new OpaQueryInputResource.Builder().schema(new OpaQueryInputResource.CatalogSchema(catalogName, schemaName)).build())).collect(Collectors.toSet());
    }

    @Override
    public void checkCanShowCreateSchema(SystemSecurityContext context, CatalogSchemaName schemaName)
    {
        OpaQueryInputResource resource = new OpaQueryInputResource.Builder().schema(new OpaQueryInputResource.CatalogSchema(schemaName)).build();
        if (!queryOpaWithSimpleResource(context, "ShowCreateSchema", resource)) {
            SystemAccessControl.super.checkCanShowCreateSchema(context, schemaName);
        }
    }

    @Override
    public void checkCanShowCreateTable(SystemSecurityContext context, CatalogSchemaTableName table)
    {
        OpaQueryInputResource resource = new OpaQueryInputResource.Builder().table(new OpaQueryInputResource.Table(table)).build();
        if (!queryOpaWithSimpleResource(context, "ShowCreateTable", resource)) {
            SystemAccessControl.super.checkCanShowCreateTable(context, table);
        }
    }

    @Override
    public void checkCanCreateTable(SystemSecurityContext context, CatalogSchemaTableName table, Map<String, Object> properties)
    {
        OpaQueryInputResource resource = new OpaQueryInputResource.Builder().table(new OpaQueryInputResource.Table(table, properties)).build();
        if (!queryOpaWithSimpleResource(context, "CreateTable", resource)) {
            SystemAccessControl.super.checkCanCreateTable(context, table, properties);
        }
    }

    @Override
    public void checkCanDropTable(SystemSecurityContext context, CatalogSchemaTableName table)
    {
        OpaQueryInputResource resource = new OpaQueryInputResource.Builder().table(new OpaQueryInputResource.Table(table)).build();
        if (!queryOpaWithSimpleResource(context, "DropTable", resource)) {
            SystemAccessControl.super.checkCanDropTable(context, table);
        }
    }

    @Override
    public void checkCanRenameTable(SystemSecurityContext context, CatalogSchemaTableName table, CatalogSchemaTableName newTable)
    {
        OpaQueryInputResource resource = new OpaQueryInputResource.Builder().table(new OpaQueryInputResource.Table(table)).build();
        OpaQueryInputResource targetResource = new OpaQueryInputResource.Builder().table(new OpaQueryInputResource.Table(newTable)).build();
        OpaQueryInputAction action = new OpaQueryInputAction.Builder().operation("RenameTable").resource(resource).targetResource(targetResource).build();
        OpaQueryInput input = new OpaQueryInput(context, action);

        if (!queryOpa(input)) {
            SystemAccessControl.super.checkCanRenameTable(context, table, newTable);
        }
    }

    @Override
    public void checkCanSetTableProperties(SystemSecurityContext context, CatalogSchemaTableName table, Map<String, Optional<Object>> properties)
    {
        Map<String, Object> transformedProperties = new HashMap<>();
        for (Map.Entry<String, Optional<Object>> entry : properties.entrySet()) {
            transformedProperties.put(entry.getKey(), entry.getValue().orElse(null));
        }

        OpaQueryInputResource resource = new OpaQueryInputResource.Builder().table(new OpaQueryInputResource.Table(table, transformedProperties)).build();

        if (!queryOpaWithSimpleResource(context, "SetTableProperties", resource)) {
            SystemAccessControl.super.checkCanSetTableProperties(context, table, properties);
        }
    }

    @Override
    public void checkCanSetTableComment(SystemSecurityContext context, CatalogSchemaTableName table)
    {
        OpaQueryInputResource resource = new OpaQueryInputResource.Builder().table(new OpaQueryInputResource.Table(table)).build();
        if (!queryOpaWithSimpleResource(context, "SetTableComment", resource)) {
            SystemAccessControl.super.checkCanSetTableComment(context, table);
        }
    }

    @Override
    public void checkCanSetColumnComment(SystemSecurityContext context, CatalogSchemaTableName table)
    {
        OpaQueryInputResource resource = new OpaQueryInputResource.Builder().table(new OpaQueryInputResource.Table(table)).build();
        if (!queryOpaWithSimpleResource(context, "SetColumnComment", resource)) {
            SystemAccessControl.super.checkCanSetColumnComment(context, table);
        }
    }

    @Override
    public void checkCanShowTables(SystemSecurityContext context, CatalogSchemaName schema)
    {
        OpaQueryInputResource resource = new OpaQueryInputResource.Builder().schema(new OpaQueryInputResource.CatalogSchema(schema)).build();
        if (!queryOpaWithSimpleResource(context, "ShowTables", resource)) {
            SystemAccessControl.super.checkCanShowTables(context, schema);
        }
    }

    @Override
    public Set<SchemaTableName> filterTables(SystemSecurityContext context, String catalogName, Set<SchemaTableName> tableNames)
    {
        return tableNames.parallelStream().filter(tableName -> queryOpaWithSimpleResource(context, "FilterTables", new OpaQueryInputResource.Builder().table(new OpaQueryInputResource.Table(catalogName, tableName)).build())).collect(Collectors.toSet());
    }

    @Override
    public void checkCanShowColumns(SystemSecurityContext context, CatalogSchemaTableName table)
    {
        OpaQueryInputResource resource = new OpaQueryInputResource.Builder().table(new OpaQueryInputResource.Table(table)).build();
        if (!queryOpaWithSimpleResource(context, "ShowColumns", resource)) {
            SystemAccessControl.super.checkCanShowColumns(context, table);
        }
    }

    @Override
    public Set<String> filterColumns(SystemSecurityContext context, CatalogSchemaTableName table, Set<String> columns)
    {
        return columns.parallelStream().filter(column -> queryOpaWithSimpleResource(context, "FilterColumns", new OpaQueryInputResource.Builder().table(new OpaQueryInputResource.Table(table, Set.of(column))).build())).collect(Collectors.toSet());
    }

    @Override
    public void checkCanAddColumn(SystemSecurityContext context, CatalogSchemaTableName table)
    {
        OpaQueryInputResource resource = new OpaQueryInputResource.Builder().table(new OpaQueryInputResource.Table(table)).build();
        if (!queryOpaWithSimpleResource(context, "AddColumn", resource)) {
            SystemAccessControl.super.checkCanAddColumn(context, table);
        }
    }

    @Override
    public void checkCanDropColumn(SystemSecurityContext context, CatalogSchemaTableName table)
    {
        OpaQueryInputResource resource = new OpaQueryInputResource.Builder().table(new OpaQueryInputResource.Table(table)).build();
        if (!queryOpaWithSimpleResource(context, "DropColumn", resource)) {
            SystemAccessControl.super.checkCanDropColumn(context, table);
        }
    }

    @Override
    public void checkCanSetTableAuthorization(SystemSecurityContext context, CatalogSchemaTableName table, TrinoPrincipal principal)
    {
        OpaQueryInputResource resource = new OpaQueryInputResource.Builder().table(new OpaQueryInputResource.Table(table)).build();
        OpaQueryInputGrant grantee = new OpaQueryInputGrant.Builder().principal(principal).build();
        OpaQueryInputAction action = new OpaQueryInputAction.Builder().operation("SetTableAuthorization").resource(resource).grantee(grantee).build();
        OpaQueryInput input = new OpaQueryInput(context, action);

        if (!queryOpa(input)) {
            SystemAccessControl.super.checkCanSetTableAuthorization(context, table, principal);
        }
    }

    @Override
    public void checkCanRenameColumn(SystemSecurityContext context, CatalogSchemaTableName table)
    {
        OpaQueryInputResource resource = new OpaQueryInputResource.Builder().table(new OpaQueryInputResource.Table(table)).build();
        if (!queryOpaWithSimpleResource(context, "RenameColumn", resource)) {
            SystemAccessControl.super.checkCanRenameColumn(context, table);
        }
    }

    @Override
    public void checkCanSelectFromColumns(SystemSecurityContext context, CatalogSchemaTableName table, Set<String> columns)
    {
        OpaQueryInputResource resource = new OpaQueryInputResource.Builder().table(new OpaQueryInputResource.Table(table, columns)).build();
        if (!queryOpaWithSimpleResource(context, "SelectFromColumns", resource)) {
            SystemAccessControl.super.checkCanSelectFromColumns(context, table, columns);
        }
    }

    @Override
    public void checkCanInsertIntoTable(SystemSecurityContext context, CatalogSchemaTableName table)
    {
        OpaQueryInputResource resource = new OpaQueryInputResource.Builder().table(new OpaQueryInputResource.Table(table)).build();
        if (!queryOpaWithSimpleResource(context, "InsertIntoTable", resource)) {
            SystemAccessControl.super.checkCanInsertIntoTable(context, table);
        }
    }

    @Override
    public void checkCanDeleteFromTable(SystemSecurityContext context, CatalogSchemaTableName table)
    {
        OpaQueryInputResource resource = new OpaQueryInputResource.Builder().table(new OpaQueryInputResource.Table(table)).build();
        if (!queryOpaWithSimpleResource(context, "DeleteFromTable", resource)) {
            SystemAccessControl.super.checkCanDeleteFromTable(context, table);
        }
    }

    @Override
    public void checkCanTruncateTable(SystemSecurityContext context, CatalogSchemaTableName table)
    {
        OpaQueryInputResource resource = new OpaQueryInputResource.Builder().table(new OpaQueryInputResource.Table(table)).build();
        if (!queryOpaWithSimpleResource(context, "TruncateTable", resource)) {
            SystemAccessControl.super.checkCanTruncateTable(context, table);
        }
    }

    @Override
    public void checkCanUpdateTableColumns(SystemSecurityContext securityContext, CatalogSchemaTableName table, Set<String> updatedColumnNames)
    {
        OpaQueryInputResource resource = new OpaQueryInputResource.Builder().table(new OpaQueryInputResource.Table(table, updatedColumnNames)).build();

        if (!queryOpaWithSimpleResource(securityContext, "UpdateTableColumns", resource)) {
            SystemAccessControl.super.checkCanUpdateTableColumns(securityContext, table, updatedColumnNames);
        }
    }

    @Override
    public void checkCanCreateView(SystemSecurityContext context, CatalogSchemaTableName view)
    {
        OpaQueryInputResource resource = new OpaQueryInputResource.Builder().view(new OpaQueryInputResource.Table(view)).build();
        if (!queryOpaWithSimpleResource(context, "CreateView", resource)) {
            SystemAccessControl.super.checkCanCreateView(context, view);
        }
    }

    @Override
    public void checkCanRenameView(SystemSecurityContext context, CatalogSchemaTableName view, CatalogSchemaTableName newView)
    {
        OpaQueryInputResource resource = new OpaQueryInputResource.Builder().view(new OpaQueryInputResource.Table(view)).build();
        OpaQueryInputResource targetResource = new OpaQueryInputResource.Builder().view(new OpaQueryInputResource.Table(newView)).build();
        OpaQueryInputAction action = new OpaQueryInputAction.Builder().operation("RenameView").resource(resource).targetResource(targetResource).build();
        OpaQueryInput input = new OpaQueryInput(context, action);

        if (!queryOpa(input)) {
            SystemAccessControl.super.checkCanRenameView(context, view, newView);
        }
    }

    @Override
    public void checkCanSetViewAuthorization(SystemSecurityContext context, CatalogSchemaTableName view, TrinoPrincipal principal)
    {
        OpaQueryInputResource resource = new OpaQueryInputResource.Builder().view(new OpaQueryInputResource.Table(view)).build();
        OpaQueryInputGrant grantee = new OpaQueryInputGrant.Builder().principal(principal).build();
        OpaQueryInputAction action = new OpaQueryInputAction.Builder().operation("SetViewAuthorization").resource(resource).grantee(grantee).build();
        OpaQueryInput input = new OpaQueryInput(context, action);

        if (!queryOpa(input)) {
            SystemAccessControl.super.checkCanSetViewAuthorization(context, view, principal);
        }
    }

    @Override
    public void checkCanDropView(SystemSecurityContext context, CatalogSchemaTableName view)
    {
        OpaQueryInputResource resource = new OpaQueryInputResource.Builder().view(new OpaQueryInputResource.Table(view)).build();
        if (!queryOpaWithSimpleResource(context, "DropView", resource)) {
            SystemAccessControl.super.checkCanDropView(context, view);
        }
    }

    @Override
    public void checkCanCreateViewWithSelectFromColumns(SystemSecurityContext context, CatalogSchemaTableName table, Set<String> columns)
    {
        // This refers to a Table, so the resource should be a table and not a view
        OpaQueryInputResource resource = new OpaQueryInputResource.Builder().table(new OpaQueryInputResource.Table(table, columns)).build();
        if (!queryOpaWithSimpleResource(context, "CreateViewWithSelectFromColumns", resource)) {
            SystemAccessControl.super.checkCanCreateViewWithSelectFromColumns(context, table, columns);
        }
    }

    @Override
    public void checkCanCreateMaterializedView(SystemSecurityContext context, CatalogSchemaTableName materializedView, Map<String, Object> properties)
    {
        OpaQueryInputResource resource = new OpaQueryInputResource.Builder().view(new OpaQueryInputResource.Table(materializedView, properties)).build();
        if (!queryOpaWithSimpleResource(context, "CreateMaterializedView", resource)) {
            SystemAccessControl.super.checkCanCreateMaterializedView(context, materializedView, properties);
        }
    }

    @Override
    public void checkCanRefreshMaterializedView(SystemSecurityContext context, CatalogSchemaTableName materializedView)
    {
        OpaQueryInputResource resource = new OpaQueryInputResource.Builder().view(new OpaQueryInputResource.Table(materializedView)).build();
        if (!queryOpaWithSimpleResource(context, "RefreshMaterializedView", resource)) {
            SystemAccessControl.super.checkCanRefreshMaterializedView(context, materializedView);
        }
    }

    @Override
    public void checkCanSetMaterializedViewProperties(SystemSecurityContext context, CatalogSchemaTableName materializedView, Map<String, Optional<Object>> properties)
    {
        Map<String, Object> transformedProperties = new HashMap<>();
        for (Map.Entry<String, Optional<Object>> entry : properties.entrySet()) {
            transformedProperties.put(entry.getKey(), entry.getValue().orElse(null));
        }

        OpaQueryInputResource resource = new OpaQueryInputResource.Builder().view(new OpaQueryInputResource.Table(materializedView, transformedProperties)).build();
        if (!queryOpaWithSimpleResource(context, "SetMaterializedViewProperties", resource)) {
            SystemAccessControl.super.checkCanSetMaterializedViewProperties(context, materializedView, properties);
        }
    }

    @Override
    public void checkCanDropMaterializedView(SystemSecurityContext context, CatalogSchemaTableName materializedView)
    {
        OpaQueryInputResource resource = new OpaQueryInputResource.Builder().view(new OpaQueryInputResource.Table(materializedView)).build();
        if (!queryOpaWithSimpleResource(context, "DropMaterializedView", resource)) {
            SystemAccessControl.super.checkCanDropMaterializedView(context, materializedView);
        }
    }

    @Override
    public void checkCanRenameMaterializedView(SystemSecurityContext context, CatalogSchemaTableName view, CatalogSchemaTableName newView)
    {
        OpaQueryInputResource resource = new OpaQueryInputResource.Builder().view(new OpaQueryInputResource.Table(view)).build();
        OpaQueryInputResource targetResource = new OpaQueryInputResource.Builder().view(new OpaQueryInputResource.Table(newView)).build();

        OpaQueryInputAction action = new OpaQueryInputAction.Builder().operation("RenameMaterializedView").resource(resource).targetResource(targetResource).build();
        OpaQueryInput input = new OpaQueryInput(context, action);

        if (!queryOpa(input)) {
            SystemAccessControl.super.checkCanRenameMaterializedView(context, view, newView);
        }
    }

    @Override
    public void checkCanGrantExecuteFunctionPrivilege(SystemSecurityContext context, String functionName, TrinoPrincipal grantee, boolean grantOption)
    {
        OpaQueryInputResource resource = new OpaQueryInputResource.Builder().function(functionName).build();
        OpaQueryInputGrant opaGrantee = new OpaQueryInputGrant.Builder().principal(grantee).grantOption(grantOption).build();
        OpaQueryInputAction action = new OpaQueryInputAction.Builder().operation("GrantExecuteFunctionPrivilege").resource(resource).grantee(opaGrantee).build();
        OpaQueryInput input = new OpaQueryInput(context, action);

        if (!queryOpa(input)) {
            SystemAccessControl.super.checkCanGrantExecuteFunctionPrivilege(context, functionName, grantee, grantOption);
        }
    }

    @Override
    public void checkCanSetCatalogSessionProperty(SystemSecurityContext context, String catalogName, String propertyName)
    {
        OpaQueryInputResource resource = new OpaQueryInputResource.Builder().catalog(catalogName).catalogSessionProperty(propertyName).build();
        OpaQueryInputAction action = new OpaQueryInputAction.Builder().operation("SetCatalogSessionProperty").resource(resource).build();
        OpaQueryInput input = new OpaQueryInput(context, action);

        if (!queryOpa(input)) {
            SystemAccessControl.super.checkCanSetCatalogSessionProperty(context, catalogName, propertyName);
        }
    }

    @Override
    public void checkCanGrantSchemaPrivilege(SystemSecurityContext context, Privilege privilege, CatalogSchemaName schema, TrinoPrincipal grantee, boolean grantOption)
    {
        OpaQueryInputResource resource = new OpaQueryInputResource.Builder().schema(new OpaQueryInputResource.CatalogSchema(schema)).build();
        OpaQueryInputGrant opaGrantee = new OpaQueryInputGrant.Builder().principal(grantee).grantOption(grantOption).privilege(privilege).build();
        OpaQueryInputAction action = new OpaQueryInputAction.Builder().operation("GrantSchemaPrivilege").resource(resource).grantee(opaGrantee).build();
        OpaQueryInput input = new OpaQueryInput(context, action);

        if (!queryOpa(input)) {
            SystemAccessControl.super.checkCanGrantSchemaPrivilege(context, privilege, schema, grantee, grantOption);
        }
    }

    @Override
    public void checkCanDenySchemaPrivilege(SystemSecurityContext context, Privilege privilege, CatalogSchemaName schema, TrinoPrincipal grantee)
    {
        OpaQueryInputResource resource = new OpaQueryInputResource.Builder().schema(new OpaQueryInputResource.CatalogSchema(schema)).build();
        OpaQueryInputGrant opaGrantee = new OpaQueryInputGrant.Builder().principal(grantee).privilege(privilege).build();
        OpaQueryInputAction action = new OpaQueryInputAction.Builder().operation("DenySchemaPrivilege").resource(resource).grantee(opaGrantee).build();
        OpaQueryInput input = new OpaQueryInput(context, action);

        if (!queryOpa(input)) {
            SystemAccessControl.super.checkCanDenySchemaPrivilege(context, privilege, schema, grantee);
        }
    }

    @Override
    public void checkCanRevokeSchemaPrivilege(SystemSecurityContext context, Privilege privilege, CatalogSchemaName schema, TrinoPrincipal revokee, boolean grantOption)
    {
        OpaQueryInputResource resource = new OpaQueryInputResource.Builder().schema(new OpaQueryInputResource.CatalogSchema(schema)).build();
        OpaQueryInputGrant opaGrantee = new OpaQueryInputGrant.Builder().principal(revokee).grantOption(grantOption).privilege(privilege).build();
        OpaQueryInputAction action = new OpaQueryInputAction.Builder().operation("RevokeSchemaPrivilege").resource(resource).grantee(opaGrantee).build();
        OpaQueryInput input = new OpaQueryInput(context, action);

        if (!queryOpa(input)) {
            SystemAccessControl.super.checkCanRevokeSchemaPrivilege(context, privilege, schema, revokee, grantOption);
        }
    }

    @Override
    public void checkCanGrantTablePrivilege(SystemSecurityContext context, Privilege privilege, CatalogSchemaTableName table, TrinoPrincipal grantee, boolean grantOption)
    {
        OpaQueryInputResource resource = new OpaQueryInputResource.Builder().table(new OpaQueryInputResource.Table(table)).build();
        OpaQueryInputGrant opaGrantee = new OpaQueryInputGrant.Builder().principal(grantee).grantOption(grantOption).privilege(privilege).build();
        OpaQueryInputAction action = new OpaQueryInputAction.Builder().operation("GrantTablePrivilege").resource(resource).grantee(opaGrantee).build();
        OpaQueryInput input = new OpaQueryInput(context, action);

        if (!queryOpa(input)) {
            SystemAccessControl.super.checkCanGrantTablePrivilege(context, privilege, table, grantee, grantOption);
        }
    }

    @Override
    public void checkCanDenyTablePrivilege(SystemSecurityContext context, Privilege privilege, CatalogSchemaTableName table, TrinoPrincipal grantee)
    {
        OpaQueryInputResource resource = new OpaQueryInputResource.Builder().table(new OpaQueryInputResource.Table(table)).build();
        OpaQueryInputGrant opaGrantee = new OpaQueryInputGrant.Builder().principal(grantee).privilege(privilege).build();
        OpaQueryInputAction action = new OpaQueryInputAction.Builder().operation("DenyTablePrivilege").resource(resource).grantee(opaGrantee).build();
        OpaQueryInput input = new OpaQueryInput(context, action);

        if (!queryOpa(input)) {
            SystemAccessControl.super.checkCanDenyTablePrivilege(context, privilege, table, grantee);
        }
    }

    @Override
    public void checkCanRevokeTablePrivilege(SystemSecurityContext context, Privilege privilege, CatalogSchemaTableName table, TrinoPrincipal revokee, boolean grantOption)
    {
        OpaQueryInputResource resource = new OpaQueryInputResource.Builder().table(new OpaQueryInputResource.Table(table)).build();
        OpaQueryInputGrant opaRevokee = new OpaQueryInputGrant.Builder().principal(revokee).privilege(privilege).grantOption(grantOption).build();
        OpaQueryInputAction action = new OpaQueryInputAction.Builder().operation("RevokeTablePrivilege").resource(resource).grantee(opaRevokee).build();
        OpaQueryInput input = new OpaQueryInput(context, action);

        if (!queryOpa(input)) {
            SystemAccessControl.super.checkCanRevokeTablePrivilege(context, privilege, table, revokee, grantOption);
        }
    }

    @Override
    public void checkCanShowRoles(SystemSecurityContext context)
    {
        if (!queryOpaWithSimpleAction(context, "ShowRoles")) {
            SystemAccessControl.super.checkCanShowRoles(context);
        }
    }

    @Override
    public void checkCanCreateRole(SystemSecurityContext context, String role, Optional<TrinoPrincipal> grantor)
    {
        OpaQueryInputResource resource = new OpaQueryInputResource.Builder().role(role).build();
        OpaQueryInputAction action = new OpaQueryInputAction.Builder().operation("CreateRole").resource(resource).grantor(grantor.orElse(null)).build();
        OpaQueryInput input = new OpaQueryInput(context, action);

        if (!queryOpa(input)) {
            SystemAccessControl.super.checkCanCreateRole(context, role, grantor);
        }
    }

    @Override
    public void checkCanDropRole(SystemSecurityContext context, String role)
    {
        OpaQueryInputResource resource = new OpaQueryInputResource.Builder().role(role).build();
        if (!queryOpaWithSimpleResource(context, "DropRole", resource)) {
            SystemAccessControl.super.checkCanDropRole(context, role);
        }
    }

    @Override
    public void checkCanGrantRoles(SystemSecurityContext context, Set<String> roles, Set<TrinoPrincipal> grantees, boolean adminOption, Optional<TrinoPrincipal> grantor)
    {
        OpaQueryInputResource resource = new OpaQueryInputResource.Builder().roles(roles).build();
        OpaQueryInputGrant opaGrantees = new OpaQueryInputGrant.Builder().grantOption(adminOption).principals(grantees).build();
        OpaQueryInputAction action = new OpaQueryInputAction.Builder().operation("GrantRoles").resource(resource).grantee(opaGrantees).grantor(grantor.orElse(null)).build();
        OpaQueryInput input = new OpaQueryInput(context, action);

        if (!queryOpa(input)) {
            SystemAccessControl.super.checkCanGrantRoles(context, roles, grantees, adminOption, grantor);
        }
    }

    @Override
    public void checkCanRevokeRoles(SystemSecurityContext context, Set<String> roles, Set<TrinoPrincipal> grantees, boolean adminOption, Optional<TrinoPrincipal> grantor)
    {
        OpaQueryInputResource resource = new OpaQueryInputResource.Builder().roles(roles).build();
        OpaQueryInputGrant opaGrantees = new OpaQueryInputGrant.Builder().grantOption(adminOption).principals(grantees).build();
        OpaQueryInputAction action = new OpaQueryInputAction.Builder().operation("RevokeRoles").resource(resource).grantee(opaGrantees).grantor(grantor.orElse(null)).build();
        OpaQueryInput input = new OpaQueryInput(context, action);

        if (!queryOpa(input)) {
            SystemAccessControl.super.checkCanRevokeRoles(context, roles, grantees, adminOption, grantor);
        }
    }

    @Override
    public void checkCanShowRoleAuthorizationDescriptors(SystemSecurityContext context)
    {
        if (!queryOpaWithSimpleAction(context, "ShowRoleAuthorizationDescriptors")) {
            SystemAccessControl.super.checkCanShowRoleAuthorizationDescriptors(context);
        }
    }

    @Override
    public void checkCanShowCurrentRoles(SystemSecurityContext context)
    {
        if (!queryOpaWithSimpleAction(context, "ShowCurrentRoles")) {
            SystemAccessControl.super.checkCanShowCurrentRoles(context);
        }
    }

    @Override
    public void checkCanShowRoleGrants(SystemSecurityContext context)
    {
        if (!queryOpaWithSimpleAction(context, "ShowRoleGrants")) {
            SystemAccessControl.super.checkCanShowRoleGrants(context);
        }
    }

    @Override
    public void checkCanExecuteProcedure(SystemSecurityContext systemSecurityContext, CatalogSchemaRoutineName procedure)
    {
        OpaQueryInputResource resource = new OpaQueryInputResource.Builder().schema(new OpaQueryInputResource.CatalogSchema(procedure.getCatalogName(), procedure.getSchemaName())).function(procedure.getRoutineName()).build();
        if (!queryOpaWithSimpleResource(systemSecurityContext, "ExecuteProcedure", resource)) {
            SystemAccessControl.super.checkCanExecuteProcedure(systemSecurityContext, procedure);
        }
    }

    @Override
    public void checkCanExecuteFunction(SystemSecurityContext systemSecurityContext, String functionName)
    {
        OpaQueryInputResource resource = new OpaQueryInputResource.Builder().function(functionName).build();

        if (!queryOpaWithSimpleResource(systemSecurityContext, "ExecuteFunction", resource)) {
            SystemAccessControl.super.checkCanExecuteFunction(systemSecurityContext, functionName);
        }
    }

    @Override
    public void checkCanExecuteFunction(SystemSecurityContext systemSecurityContext, FunctionKind functionKind, CatalogSchemaRoutineName functionName)
    {
        OpaQueryInputResource resource = new OpaQueryInputResource.Builder().schema(new OpaQueryInputResource.CatalogSchema(functionName.getCatalogName(), functionName.getSchemaName())).function(new OpaQueryInputResource.Function(functionName.getRoutineName(), functionKind)).build();

        if (!queryOpaWithSimpleResource(systemSecurityContext, "ExecuteFunction", resource)) {
            SystemAccessControl.super.checkCanExecuteFunction(systemSecurityContext, functionKind, functionName);
        }
    }

    @Override
    public void checkCanExecuteTableProcedure(SystemSecurityContext systemSecurityContext, CatalogSchemaTableName table, String procedure)
    {
        OpaQueryInputResource resource = new OpaQueryInputResource.Builder().table(new OpaQueryInputResource.Table(table)).function(procedure).build();
        if (!queryOpaWithSimpleResource(systemSecurityContext, "ExecuteTableProcedure", resource)) {
            SystemAccessControl.super.checkCanExecuteTableProcedure(systemSecurityContext, table, procedure);
        }
    }

    @Override
    public Iterable<EventListener> getEventListeners()
    {
        return SystemAccessControl.super.getEventListeners();
    }

    private static class OpaQuery
    {
        public OpaQueryInput input;

        public OpaQuery(OpaQueryInput input)
        {
            this.input = input;
        }
    }

    public static class OpaQueryResult
    {
        @JsonProperty("decision_id")
        public String decisionId;
        // boxed Boolean to detect not-present vs explicitly false
        public Boolean result;
    }
}
