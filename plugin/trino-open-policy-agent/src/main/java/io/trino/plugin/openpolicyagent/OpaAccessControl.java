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
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import static io.trino.spi.security.AccessDeniedException.denyAddColumn;
import static io.trino.spi.security.AccessDeniedException.denyCatalogAccess;
import static io.trino.spi.security.AccessDeniedException.denyCommentColumn;
import static io.trino.spi.security.AccessDeniedException.denyCommentTable;
import static io.trino.spi.security.AccessDeniedException.denyCreateMaterializedView;
import static io.trino.spi.security.AccessDeniedException.denyCreateRole;
import static io.trino.spi.security.AccessDeniedException.denyCreateSchema;
import static io.trino.spi.security.AccessDeniedException.denyCreateTable;
import static io.trino.spi.security.AccessDeniedException.denyCreateView;
import static io.trino.spi.security.AccessDeniedException.denyCreateViewWithSelect;
import static io.trino.spi.security.AccessDeniedException.denyDeleteTable;
import static io.trino.spi.security.AccessDeniedException.denyDenySchemaPrivilege;
import static io.trino.spi.security.AccessDeniedException.denyDenyTablePrivilege;
import static io.trino.spi.security.AccessDeniedException.denyDropColumn;
import static io.trino.spi.security.AccessDeniedException.denyDropMaterializedView;
import static io.trino.spi.security.AccessDeniedException.denyDropRole;
import static io.trino.spi.security.AccessDeniedException.denyDropSchema;
import static io.trino.spi.security.AccessDeniedException.denyDropTable;
import static io.trino.spi.security.AccessDeniedException.denyDropView;
import static io.trino.spi.security.AccessDeniedException.denyExecuteFunction;
import static io.trino.spi.security.AccessDeniedException.denyExecuteProcedure;
import static io.trino.spi.security.AccessDeniedException.denyExecuteQuery;
import static io.trino.spi.security.AccessDeniedException.denyExecuteTableProcedure;
import static io.trino.spi.security.AccessDeniedException.denyGrantExecuteFunctionPrivilege;
import static io.trino.spi.security.AccessDeniedException.denyGrantRoles;
import static io.trino.spi.security.AccessDeniedException.denyGrantSchemaPrivilege;
import static io.trino.spi.security.AccessDeniedException.denyGrantTablePrivilege;
import static io.trino.spi.security.AccessDeniedException.denyImpersonateUser;
import static io.trino.spi.security.AccessDeniedException.denyInsertTable;
import static io.trino.spi.security.AccessDeniedException.denyKillQuery;
import static io.trino.spi.security.AccessDeniedException.denyReadSystemInformationAccess;
import static io.trino.spi.security.AccessDeniedException.denyRefreshMaterializedView;
import static io.trino.spi.security.AccessDeniedException.denyRenameColumn;
import static io.trino.spi.security.AccessDeniedException.denyRenameMaterializedView;
import static io.trino.spi.security.AccessDeniedException.denyRenameSchema;
import static io.trino.spi.security.AccessDeniedException.denyRenameTable;
import static io.trino.spi.security.AccessDeniedException.denyRenameView;
import static io.trino.spi.security.AccessDeniedException.denyRevokeRoles;
import static io.trino.spi.security.AccessDeniedException.denyRevokeSchemaPrivilege;
import static io.trino.spi.security.AccessDeniedException.denyRevokeTablePrivilege;
import static io.trino.spi.security.AccessDeniedException.denySelectColumns;
import static io.trino.spi.security.AccessDeniedException.denySetCatalogSessionProperty;
import static io.trino.spi.security.AccessDeniedException.denySetMaterializedViewProperties;
import static io.trino.spi.security.AccessDeniedException.denySetSchemaAuthorization;
import static io.trino.spi.security.AccessDeniedException.denySetSystemSessionProperty;
import static io.trino.spi.security.AccessDeniedException.denySetTableAuthorization;
import static io.trino.spi.security.AccessDeniedException.denySetTableProperties;
import static io.trino.spi.security.AccessDeniedException.denySetViewAuthorization;
import static io.trino.spi.security.AccessDeniedException.denyShowColumns;
import static io.trino.spi.security.AccessDeniedException.denyShowCreateSchema;
import static io.trino.spi.security.AccessDeniedException.denyShowCreateTable;
import static io.trino.spi.security.AccessDeniedException.denyShowCurrentRoles;
import static io.trino.spi.security.AccessDeniedException.denyShowRoleAuthorizationDescriptors;
import static io.trino.spi.security.AccessDeniedException.denyShowRoleGrants;
import static io.trino.spi.security.AccessDeniedException.denyShowRoles;
import static io.trino.spi.security.AccessDeniedException.denyShowSchemas;
import static io.trino.spi.security.AccessDeniedException.denyShowTables;
import static io.trino.spi.security.AccessDeniedException.denyTruncateTable;
import static io.trino.spi.security.AccessDeniedException.denyUpdateTableColumns;
import static io.trino.spi.security.AccessDeniedException.denyViewQuery;
import static io.trino.spi.security.AccessDeniedException.denyWriteSystemInformationAccess;
import static java.lang.String.format;

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

    private static String trinoPrincipalToString(TrinoPrincipal principal)
    {
        return format("%s '%s'", principal.getType().name().toLowerCase(Locale.ENGLISH), principal.getName());
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
            denyImpersonateUser(context.getIdentity().getUser(), userName);
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
            denyExecuteQuery();
        }
    }

    @Override
    public void checkCanViewQueryOwnedBy(SystemSecurityContext context, Identity queryOwner)
    {
        OpaQueryInputResource resource = new OpaQueryInputResource.Builder().user(new OpaQueryInputResource.User(queryOwner)).build();
        if (!queryOpaWithSimpleResource(context, "ViewQueryOwnedBy", resource)) {
            denyViewQuery();
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
            denyKillQuery();
        }
    }

    @Override
    public void checkCanReadSystemInformation(SystemSecurityContext context)
    {
        if (!queryOpaWithSimpleAction(context, "ReadSystemInformation")) {
            denyReadSystemInformationAccess();
        }
    }

    @Override
    public void checkCanWriteSystemInformation(SystemSecurityContext context)
    {
        if (!queryOpaWithSimpleAction(context, "WriteSystemInformation")) {
            denyWriteSystemInformationAccess();
        }
    }

    @Override
    public void checkCanSetSystemSessionProperty(SystemSecurityContext context, String propertyName)
    {
        OpaQueryInputResource resource = new OpaQueryInputResource.Builder().systemSessionProperty(propertyName).build();

        if (!queryOpaWithSimpleResource(context, "SetSystemSessionProperty", resource)) {
            denySetSystemSessionProperty(propertyName);
        }
    }

    @Override
    public void checkCanAccessCatalog(SystemSecurityContext context, String catalogName)
    {
        OpaQueryInputResource resource = new OpaQueryInputResource.Builder().catalog(catalogName).build();
        if (!queryOpaWithSimpleResource(context, "AccessCatalog", resource)) {
            denyCatalogAccess(catalogName);
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
            denyCreateSchema(schema.toString());
        }
    }

    @Override
    public void checkCanDropSchema(SystemSecurityContext context, CatalogSchemaName schema)
    {
        OpaQueryInputResource resource = new OpaQueryInputResource.Builder().schema(new OpaQueryInputResource.CatalogSchema(schema)).build();
        if (!queryOpaWithSimpleResource(context, "DropSchema", resource)) {
            denyDropSchema(schema.toString());
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
            denyRenameSchema(schema.toString(), newSchemaName);
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
            denySetSchemaAuthorization(schema.toString(), principal);
        }
    }

    @Override
    public void checkCanShowSchemas(SystemSecurityContext context, String catalogName)
    {
        OpaQueryInputResource resource = new OpaQueryInputResource.Builder().catalog(catalogName).build();
        if (!queryOpaWithSimpleResource(context, "ShowSchemas", resource)) {
            denyShowSchemas();
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
            denyShowCreateSchema(schemaName.toString());
        }
    }

    @Override
    public void checkCanShowCreateTable(SystemSecurityContext context, CatalogSchemaTableName table)
    {
        OpaQueryInputResource resource = new OpaQueryInputResource.Builder().table(new OpaQueryInputResource.Table(table)).build();
        if (!queryOpaWithSimpleResource(context, "ShowCreateTable", resource)) {
            denyShowCreateTable(table.toString());
        }
    }

    @Override
    public void checkCanCreateTable(SystemSecurityContext context, CatalogSchemaTableName table, Map<String, Object> properties)
    {
        OpaQueryInputResource resource = new OpaQueryInputResource.Builder().table(new OpaQueryInputResource.Table(table, properties)).build();
        if (!queryOpaWithSimpleResource(context, "CreateTable", resource)) {
            denyCreateTable(table.toString());
        }
    }

    @Override
    public void checkCanDropTable(SystemSecurityContext context, CatalogSchemaTableName table)
    {
        OpaQueryInputResource resource = new OpaQueryInputResource.Builder().table(new OpaQueryInputResource.Table(table)).build();
        if (!queryOpaWithSimpleResource(context, "DropTable", resource)) {
            denyDropTable(table.toString());
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
            denyRenameTable(table.toString(), newTable.toString());
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
            denySetTableProperties(table.toString());
        }
    }

    @Override
    public void checkCanSetTableComment(SystemSecurityContext context, CatalogSchemaTableName table)
    {
        OpaQueryInputResource resource = new OpaQueryInputResource.Builder().table(new OpaQueryInputResource.Table(table)).build();
        if (!queryOpaWithSimpleResource(context, "SetTableComment", resource)) {
            denyCommentTable(table.toString());
        }
    }

    @Override
    public void checkCanSetColumnComment(SystemSecurityContext context, CatalogSchemaTableName table)
    {
        OpaQueryInputResource resource = new OpaQueryInputResource.Builder().table(new OpaQueryInputResource.Table(table)).build();
        if (!queryOpaWithSimpleResource(context, "SetColumnComment", resource)) {
            denyCommentColumn(table.toString());
        }
    }

    @Override
    public void checkCanShowTables(SystemSecurityContext context, CatalogSchemaName schema)
    {
        OpaQueryInputResource resource = new OpaQueryInputResource.Builder().schema(new OpaQueryInputResource.CatalogSchema(schema)).build();
        if (!queryOpaWithSimpleResource(context, "ShowTables", resource)) {
            denyShowTables(schema.toString());
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
            denyShowColumns(table.toString());
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
            denyAddColumn(table.toString());
        }
    }

    @Override
    public void checkCanDropColumn(SystemSecurityContext context, CatalogSchemaTableName table)
    {
        OpaQueryInputResource resource = new OpaQueryInputResource.Builder().table(new OpaQueryInputResource.Table(table)).build();
        if (!queryOpaWithSimpleResource(context, "DropColumn", resource)) {
            denyDropColumn(table.toString());
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
            denySetTableAuthorization(table.toString(), principal);
        }
    }

    @Override
    public void checkCanRenameColumn(SystemSecurityContext context, CatalogSchemaTableName table)
    {
        OpaQueryInputResource resource = new OpaQueryInputResource.Builder().table(new OpaQueryInputResource.Table(table)).build();
        if (!queryOpaWithSimpleResource(context, "RenameColumn", resource)) {
            denyRenameColumn(table.toString());
        }
    }

    @Override
    public void checkCanSelectFromColumns(SystemSecurityContext context, CatalogSchemaTableName table, Set<String> columns)
    {
        OpaQueryInputResource resource = new OpaQueryInputResource.Builder().table(new OpaQueryInputResource.Table(table, columns)).build();
        if (!queryOpaWithSimpleResource(context, "SelectFromColumns", resource)) {
            denySelectColumns(table.toString(), columns);
        }
    }

    @Override
    public void checkCanInsertIntoTable(SystemSecurityContext context, CatalogSchemaTableName table)
    {
        OpaQueryInputResource resource = new OpaQueryInputResource.Builder().table(new OpaQueryInputResource.Table(table)).build();
        if (!queryOpaWithSimpleResource(context, "InsertIntoTable", resource)) {
            denyInsertTable(table.toString());
        }
    }

    @Override
    public void checkCanDeleteFromTable(SystemSecurityContext context, CatalogSchemaTableName table)
    {
        OpaQueryInputResource resource = new OpaQueryInputResource.Builder().table(new OpaQueryInputResource.Table(table)).build();
        if (!queryOpaWithSimpleResource(context, "DeleteFromTable", resource)) {
            denyDeleteTable(table.toString());
        }
    }

    @Override
    public void checkCanTruncateTable(SystemSecurityContext context, CatalogSchemaTableName table)
    {
        OpaQueryInputResource resource = new OpaQueryInputResource.Builder().table(new OpaQueryInputResource.Table(table)).build();
        if (!queryOpaWithSimpleResource(context, "TruncateTable", resource)) {
            denyTruncateTable(table.toString());
        }
    }

    @Override
    public void checkCanUpdateTableColumns(SystemSecurityContext securityContext, CatalogSchemaTableName table, Set<String> updatedColumnNames)
    {
        OpaQueryInputResource resource = new OpaQueryInputResource.Builder().table(new OpaQueryInputResource.Table(table, updatedColumnNames)).build();

        if (!queryOpaWithSimpleResource(securityContext, "UpdateTableColumns", resource)) {
            denyUpdateTableColumns(table.toString(), updatedColumnNames);
        }
    }

    @Override
    public void checkCanCreateView(SystemSecurityContext context, CatalogSchemaTableName view)
    {
        OpaQueryInputResource resource = new OpaQueryInputResource.Builder().view(new OpaQueryInputResource.Table(view)).build();
        if (!queryOpaWithSimpleResource(context, "CreateView", resource)) {
            denyCreateView(view.toString());
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
            denyRenameView(view.toString(), newView.toString());
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
            denySetViewAuthorization(view.toString(), principal);
        }
    }

    @Override
    public void checkCanDropView(SystemSecurityContext context, CatalogSchemaTableName view)
    {
        OpaQueryInputResource resource = new OpaQueryInputResource.Builder().view(new OpaQueryInputResource.Table(view)).build();
        if (!queryOpaWithSimpleResource(context, "DropView", resource)) {
            denyDropView(view.toString());
        }
    }

    @Override
    public void checkCanCreateViewWithSelectFromColumns(SystemSecurityContext context, CatalogSchemaTableName table, Set<String> columns)
    {
        // This refers to a Table, so the resource should be a table and not a view
        OpaQueryInputResource resource = new OpaQueryInputResource.Builder().table(new OpaQueryInputResource.Table(table, columns)).build();
        if (!queryOpaWithSimpleResource(context, "CreateViewWithSelectFromColumns", resource)) {
            denyCreateViewWithSelect(table.toString(), context.getIdentity());
        }
    }

    @Override
    public void checkCanCreateMaterializedView(SystemSecurityContext context, CatalogSchemaTableName materializedView, Map<String, Object> properties)
    {
        OpaQueryInputResource resource = new OpaQueryInputResource.Builder().view(new OpaQueryInputResource.Table(materializedView, properties)).build();
        if (!queryOpaWithSimpleResource(context, "CreateMaterializedView", resource)) {
            denyCreateMaterializedView(materializedView.toString());
        }
    }

    @Override
    public void checkCanRefreshMaterializedView(SystemSecurityContext context, CatalogSchemaTableName materializedView)
    {
        OpaQueryInputResource resource = new OpaQueryInputResource.Builder().view(new OpaQueryInputResource.Table(materializedView)).build();
        if (!queryOpaWithSimpleResource(context, "RefreshMaterializedView", resource)) {
            denyRefreshMaterializedView(materializedView.toString());
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
            denySetMaterializedViewProperties(materializedView.toString());
        }
    }

    @Override
    public void checkCanDropMaterializedView(SystemSecurityContext context, CatalogSchemaTableName materializedView)
    {
        OpaQueryInputResource resource = new OpaQueryInputResource.Builder().view(new OpaQueryInputResource.Table(materializedView)).build();
        if (!queryOpaWithSimpleResource(context, "DropMaterializedView", resource)) {
            denyDropMaterializedView(materializedView.toString());
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
            denyRenameMaterializedView(view.toString(), newView.toString());
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
            denyGrantExecuteFunctionPrivilege(functionName, context.getIdentity(), trinoPrincipalToString(grantee));
        }
    }

    @Override
    public void checkCanSetCatalogSessionProperty(SystemSecurityContext context, String catalogName, String propertyName)
    {
        OpaQueryInputResource resource = new OpaQueryInputResource.Builder().catalog(catalogName).catalogSessionProperty(propertyName).build();
        OpaQueryInputAction action = new OpaQueryInputAction.Builder().operation("SetCatalogSessionProperty").resource(resource).build();
        OpaQueryInput input = new OpaQueryInput(context, action);

        if (!queryOpa(input)) {
            denySetCatalogSessionProperty(propertyName);
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
            denyGrantSchemaPrivilege(privilege.toString(), schema.toString());
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
            denyDenySchemaPrivilege(privilege.toString(), schema.toString());
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
            denyRevokeSchemaPrivilege(privilege.toString(), schema.toString());
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
            denyGrantTablePrivilege(privilege.toString(), table.toString());
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
            denyDenyTablePrivilege(privilege.toString(), table.toString());
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
            denyRevokeTablePrivilege(privilege.toString(), table.toString());
        }
    }

    @Override
    public void checkCanShowRoles(SystemSecurityContext context)
    {
        if (!queryOpaWithSimpleAction(context, "ShowRoles")) {
            denyShowRoles();
        }
    }

    @Override
    public void checkCanCreateRole(SystemSecurityContext context, String role, Optional<TrinoPrincipal> grantor)
    {
        OpaQueryInputResource resource = new OpaQueryInputResource.Builder().role(role).build();
        OpaQueryInputAction action = new OpaQueryInputAction.Builder().operation("CreateRole").resource(resource).grantor(grantor.orElse(null)).build();
        OpaQueryInput input = new OpaQueryInput(context, action);

        if (!queryOpa(input)) {
            denyCreateRole(role);
        }
    }

    @Override
    public void checkCanDropRole(SystemSecurityContext context, String role)
    {
        OpaQueryInputResource resource = new OpaQueryInputResource.Builder().role(role).build();
        if (!queryOpaWithSimpleResource(context, "DropRole", resource)) {
            denyDropRole(role);
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
            denyGrantRoles(roles, grantees);
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
            denyRevokeRoles(roles, grantees);
        }
    }

    @Override
    public void checkCanShowRoleAuthorizationDescriptors(SystemSecurityContext context)
    {
        if (!queryOpaWithSimpleAction(context, "ShowRoleAuthorizationDescriptors")) {
            denyShowRoleAuthorizationDescriptors();
        }
    }

    @Override
    public void checkCanShowCurrentRoles(SystemSecurityContext context)
    {
        if (!queryOpaWithSimpleAction(context, "ShowCurrentRoles")) {
            denyShowCurrentRoles();
        }
    }

    @Override
    public void checkCanShowRoleGrants(SystemSecurityContext context)
    {
        if (!queryOpaWithSimpleAction(context, "ShowRoleGrants")) {
            denyShowRoleGrants();
        }
    }

    @Override
    public void checkCanExecuteProcedure(SystemSecurityContext systemSecurityContext, CatalogSchemaRoutineName procedure)
    {
        OpaQueryInputResource resource = new OpaQueryInputResource.Builder().schema(new OpaQueryInputResource.CatalogSchema(procedure.getCatalogName(), procedure.getSchemaName())).function(procedure.getRoutineName()).build();
        if (!queryOpaWithSimpleResource(systemSecurityContext, "ExecuteProcedure", resource)) {
            denyExecuteProcedure(procedure.toString());
        }
    }

    @Override
    public void checkCanExecuteFunction(SystemSecurityContext systemSecurityContext, String functionName)
    {
        OpaQueryInputResource resource = new OpaQueryInputResource.Builder().function(functionName).build();

        if (!queryOpaWithSimpleResource(systemSecurityContext, "ExecuteFunction", resource)) {
            denyExecuteFunction(functionName);
        }
    }

    @Override
    public void checkCanExecuteFunction(SystemSecurityContext systemSecurityContext, FunctionKind functionKind, CatalogSchemaRoutineName functionName)
    {
        OpaQueryInputResource resource = new OpaQueryInputResource.Builder().schema(new OpaQueryInputResource.CatalogSchema(functionName.getCatalogName(), functionName.getSchemaName())).function(new OpaQueryInputResource.Function(functionName.getRoutineName(), functionKind)).build();

        if (!queryOpaWithSimpleResource(systemSecurityContext, "ExecuteFunction", resource)) {
            denyExecuteFunction(functionName.toString());
        }
    }

    @Override
    public void checkCanExecuteTableProcedure(SystemSecurityContext systemSecurityContext, CatalogSchemaTableName table, String procedure)
    {
        OpaQueryInputResource resource = new OpaQueryInputResource.Builder().table(new OpaQueryInputResource.Table(table)).function(procedure).build();

        if (!queryOpaWithSimpleResource(systemSecurityContext, "ExecuteTableProcedure", resource)) {
            denyExecuteTableProcedure(table.toString(), procedure);
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
