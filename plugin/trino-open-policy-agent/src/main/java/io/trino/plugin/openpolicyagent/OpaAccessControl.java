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

import com.google.common.collect.ImmutableSet;
import com.google.inject.Inject;
import io.trino.plugin.openpolicyagent.schema.OpaQueryContext;
import io.trino.plugin.openpolicyagent.schema.OpaQueryInput;
import io.trino.plugin.openpolicyagent.schema.OpaQueryInputAction;
import io.trino.plugin.openpolicyagent.schema.OpaQueryInputGrant;
import io.trino.plugin.openpolicyagent.schema.OpaQueryInputResource;
import io.trino.plugin.openpolicyagent.schema.TrinoFunction;
import io.trino.plugin.openpolicyagent.schema.TrinoGrantPrincipal;
import io.trino.plugin.openpolicyagent.schema.TrinoSchema;
import io.trino.plugin.openpolicyagent.schema.TrinoTable;
import io.trino.plugin.openpolicyagent.schema.TrinoUser;
import io.trino.spi.connector.CatalogSchemaName;
import io.trino.spi.connector.CatalogSchemaRoutineName;
import io.trino.spi.connector.CatalogSchemaTableName;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.function.FunctionKind;
import io.trino.spi.security.AccessDeniedException;
import io.trino.spi.security.Identity;
import io.trino.spi.security.Privilege;
import io.trino.spi.security.SystemAccessControl;
import io.trino.spi.security.SystemSecurityContext;
import io.trino.spi.security.TrinoPrincipal;

import java.security.Principal;
import java.util.Collection;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static io.trino.plugin.openpolicyagent.OpaHighLevelClient.DenyCallable;
import static io.trino.plugin.openpolicyagent.OpaHighLevelClient.buildQueryInputForSimpleResource;
import static io.trino.spi.security.AccessDeniedException.denyCreateRole;
import static io.trino.spi.security.AccessDeniedException.denyCreateViewWithSelect;
import static io.trino.spi.security.AccessDeniedException.denyDenySchemaPrivilege;
import static io.trino.spi.security.AccessDeniedException.denyDenyTablePrivilege;
import static io.trino.spi.security.AccessDeniedException.denyExecuteFunction;
import static io.trino.spi.security.AccessDeniedException.denyExecuteProcedure;
import static io.trino.spi.security.AccessDeniedException.denyExecuteTableProcedure;
import static io.trino.spi.security.AccessDeniedException.denyGrantExecuteFunctionPrivilege;
import static io.trino.spi.security.AccessDeniedException.denyGrantRoles;
import static io.trino.spi.security.AccessDeniedException.denyGrantSchemaPrivilege;
import static io.trino.spi.security.AccessDeniedException.denyGrantTablePrivilege;
import static io.trino.spi.security.AccessDeniedException.denyImpersonateUser;
import static io.trino.spi.security.AccessDeniedException.denyRenameMaterializedView;
import static io.trino.spi.security.AccessDeniedException.denyRenameSchema;
import static io.trino.spi.security.AccessDeniedException.denyRenameTable;
import static io.trino.spi.security.AccessDeniedException.denyRenameView;
import static io.trino.spi.security.AccessDeniedException.denyRevokeRoles;
import static io.trino.spi.security.AccessDeniedException.denyRevokeSchemaPrivilege;
import static io.trino.spi.security.AccessDeniedException.denyRevokeTablePrivilege;
import static io.trino.spi.security.AccessDeniedException.denySetCatalogSessionProperty;
import static io.trino.spi.security.AccessDeniedException.denySetSchemaAuthorization;
import static io.trino.spi.security.AccessDeniedException.denySetTableAuthorization;
import static io.trino.spi.security.AccessDeniedException.denySetViewAuthorization;
import static java.lang.String.format;

public class OpaAccessControl
        implements SystemAccessControl
{
    protected final OpaHighLevelClient opaHighLevelClient;

    @Inject
    public OpaAccessControl(OpaHighLevelClient opaHighLevelClient)
    {
        this.opaHighLevelClient = opaHighLevelClient;
    }

    private static String trinoPrincipalToString(TrinoPrincipal principal)
    {
        return format("%s '%s'", principal.getType().name().toLowerCase(Locale.ENGLISH), principal.getName());
    }

    @Override
    public void checkCanImpersonateUser(SystemSecurityContext context, String userName)
    {
        opaHighLevelClient.queryAndEnforce(
                context,
                "ImpersonateUser",
                () -> denyImpersonateUser(context.getIdentity().getUser(), userName),
                OpaQueryInputResource.Builder::user,
                new TrinoUser(userName));
    }

    @Override
    public void checkCanSetUser(Optional<Principal> principal, String userName)
    {
        // This method is deprecated and is called for any identity, let's no-op
    }

    @Override
    public void checkCanExecuteQuery(SystemSecurityContext context)
    {
        opaHighLevelClient.queryAndEnforce(context, "ExecuteQuery", AccessDeniedException::denyExecuteQuery);
    }

    @Override
    public void checkCanViewQueryOwnedBy(SystemSecurityContext context, Identity queryOwner)
    {
        opaHighLevelClient.queryAndEnforce(context, "ViewQueryOwnedBy", AccessDeniedException::denyViewQuery, queryOwner);
    }

    @Override
    public Collection<Identity> filterViewQueryOwnedBy(SystemSecurityContext context, Collection<Identity> queryOwners)
    {
        return opaHighLevelClient.parallelFilterFromOpa(
                queryOwners,
                (i) -> buildQueryInputForSimpleResource(
                        context,
                        "FilterViewQueryOwnedBy",
                        new OpaQueryInputResource.Builder()
                                .user(new TrinoUser(i))
                                .build()));
    }

    @Override
    public void checkCanKillQueryOwnedBy(SystemSecurityContext context, Identity queryOwner)
    {
        opaHighLevelClient.queryAndEnforce(context, "KillQueryOwnedBy", AccessDeniedException::denyKillQuery, queryOwner);
    }

    @Override
    public void checkCanReadSystemInformation(SystemSecurityContext context)
    {
        opaHighLevelClient.queryAndEnforce(context, "ReadSystemInformation", AccessDeniedException::denyReadSystemInformationAccess);
    }

    @Override
    public void checkCanWriteSystemInformation(SystemSecurityContext context)
    {
        opaHighLevelClient.queryAndEnforce(context, "WriteSystemInformation", AccessDeniedException::denyWriteSystemInformationAccess);
    }

    @Override
    public void checkCanSetSystemSessionProperty(SystemSecurityContext context, String propertyName)
    {
        opaHighLevelClient.queryAndEnforce(
                context,
                "SetSystemSessionProperty",
                AccessDeniedException::denySetSystemSessionProperty,
                OpaQueryInputResource.Builder::systemSessionProperty,
                propertyName);
    }

    @Override
    public void checkCanAccessCatalog(SystemSecurityContext context, String catalogName)
    {
        opaHighLevelClient.queryAndEnforce(
                context,
                "AccessCatalog",
                AccessDeniedException::denyCatalogAccess,
                OpaQueryInputResource.Builder::catalog,
                catalogName);
    }

    @Override
    public void checkCanCreateCatalog(SystemSecurityContext context, String catalog)
    {
        opaHighLevelClient.queryAndEnforce(
                context,
                "CreateCatalog",
                AccessDeniedException::denyCreateCatalog,
                OpaQueryInputResource.Builder::catalog,
                catalog);
    }

    @Override
    public void checkCanDropCatalog(SystemSecurityContext context, String catalog)
    {
        opaHighLevelClient.queryAndEnforce(
                context,
                "DropCatalog",
                AccessDeniedException::denyDropCatalog,
                OpaQueryInputResource.Builder::catalog,
                catalog);
    }

    @Override
    public Set<String> filterCatalogs(SystemSecurityContext context, Set<String> catalogs)
    {
        return opaHighLevelClient.parallelFilterFromOpa(
                catalogs,
                (c) -> buildQueryInputForSimpleResource(
                        context,
                        "FilterCatalogs",
                        new OpaQueryInputResource
                                .Builder()
                                .catalog(c)
                                .build()));
    }

    @Override
    public void checkCanCreateSchema(SystemSecurityContext context, CatalogSchemaName schema, Map<String, Object> properties)
    {
        opaHighLevelClient.queryAndEnforce(
                context,
                "CreateSchema",
                AccessDeniedException::denyCreateSchema,
                schema,
                properties);
    }

    @Override
    public void checkCanDropSchema(SystemSecurityContext context, CatalogSchemaName schema)
    {
        opaHighLevelClient.queryAndEnforce(context, "DropSchema", AccessDeniedException::denyDropSchema, schema);
    }

    @Override
    public void checkCanRenameSchema(SystemSecurityContext context, CatalogSchemaName schema, String newSchemaName)
    {
        OpaQueryInputResource resource = new OpaQueryInputResource
                .Builder()
                .schema(TrinoSchema.fromTrinoCatalogSchema(schema))
                .build();
        OpaQueryInputResource targetResource = new OpaQueryInputResource
                .Builder()
                .schema(new TrinoSchema.Builder()
                        .catalogName(schema.getCatalogName())
                        .schemaName(newSchemaName)
                        .build())
                .build();

        if (!opaHighLevelClient.queryOpaWithSourceAndTargetResource(context, "RenameSchema", resource, targetResource)) {
            denyRenameSchema(schema.toString(), newSchemaName);
        }
    }

    @Override
    public void checkCanSetSchemaAuthorization(SystemSecurityContext context, CatalogSchemaName schema, TrinoPrincipal principal)
    {
        OpaQueryInputResource resource = new OpaQueryInputResource
                .Builder()
                .schema(TrinoSchema.fromTrinoCatalogSchema(schema))
                .build();
        OpaQueryInputGrant grantee = new OpaQueryInputGrant
                .Builder()
                .principal(TrinoGrantPrincipal.fromTrinoPrincipal(principal))
                .build();
        OpaQueryInputAction action = new OpaQueryInputAction
                .Builder()
                .operation("SetSchemaAuthorization")
                .resource(resource)
                .grantee(grantee)
                .build();
        OpaQueryInput input = new OpaQueryInput(OpaQueryContext.fromSystemSecurityContext(context), action);

        if (!opaHighLevelClient.queryOpa(input)) {
            denySetSchemaAuthorization(schema.toString(), principal);
        }
    }

    @Override
    public void checkCanShowSchemas(SystemSecurityContext context, String catalogName)
    {
        opaHighLevelClient.queryAndEnforce(
                context,
                "ShowSchemas",
                (DenyCallable) AccessDeniedException::denyShowSchemas,
                OpaQueryInputResource.Builder::catalog,
                catalogName);
    }

    @Override
    public Set<String> filterSchemas(SystemSecurityContext context, String catalogName, Set<String> schemaNames)
    {
        return opaHighLevelClient.parallelFilterFromOpa(
                schemaNames,
                (s) -> buildQueryInputForSimpleResource(
                        context,
                        "FilterSchemas",
                        new OpaQueryInputResource
                                .Builder()
                                .schema(new TrinoSchema.Builder()
                                        .catalogName(catalogName)
                                        .schemaName(s)
                                        .build())
                                .build()));
    }

    @Override
    public void checkCanShowCreateSchema(SystemSecurityContext context, CatalogSchemaName schemaName)
    {
        opaHighLevelClient.queryAndEnforce(context, "ShowCreateSchema", AccessDeniedException::denyShowCreateSchema, schemaName);
    }

    @Override
    public void checkCanShowCreateTable(SystemSecurityContext context, CatalogSchemaTableName table)
    {
        opaHighLevelClient.queryAndEnforce(context, "ShowCreateTable", AccessDeniedException::denyShowCreateTable, table);
    }

    @Override
    public void checkCanCreateTable(SystemSecurityContext context, CatalogSchemaTableName table, Map<String, Object> properties)
    {
        opaHighLevelClient.queryAndEnforce(
                context,
                "CreateTable",
                AccessDeniedException::denyCreateTable,
                table,
                properties);
    }

    @Override
    public void checkCanDropTable(SystemSecurityContext context, CatalogSchemaTableName table)
    {
        opaHighLevelClient.queryAndEnforce(context, "DropTable", AccessDeniedException::denyDropTable, table);
    }

    @Override
    public void checkCanRenameTable(SystemSecurityContext context, CatalogSchemaTableName table, CatalogSchemaTableName newTable)
    {
        OpaQueryInputResource oldResource = new OpaQueryInputResource
                .Builder()
                .table(TrinoTable.fromTrinoTable(table))
                .build();
        OpaQueryInputResource newResource = new OpaQueryInputResource
                .Builder()
                .table(TrinoTable.fromTrinoTable(newTable))
                .build();
        if (!opaHighLevelClient.queryOpaWithSourceAndTargetResource(context, "RenameTable", oldResource, newResource)) {
            denyRenameTable(table.toString(), newTable.toString());
        }
    }

    @Override
    public void checkCanSetTableProperties(SystemSecurityContext context, CatalogSchemaTableName table, Map<String, Optional<Object>> properties)
    {
        opaHighLevelClient.queryAndEnforce(
                context,
                "SetTableProperties",
                AccessDeniedException::denySetTableProperties,
                table,
                properties);
    }

    @Override
    public void checkCanSetTableComment(SystemSecurityContext context, CatalogSchemaTableName table)
    {
        opaHighLevelClient.queryAndEnforce(context, "SetTableComment", AccessDeniedException::denyCommentTable, table);
    }

    @Override
    public void checkCanSetViewComment(SystemSecurityContext context, CatalogSchemaTableName view)
    {
        opaHighLevelClient.queryAndEnforce(context, "SetViewComment", AccessDeniedException::denyCommentView, view);
    }

    @Override
    public void checkCanSetColumnComment(SystemSecurityContext context, CatalogSchemaTableName table)
    {
        opaHighLevelClient.queryAndEnforce(context, "SetColumnComment", AccessDeniedException::denyCommentColumn, table);
    }

    @Override
    public void checkCanShowTables(SystemSecurityContext context, CatalogSchemaName schema)
    {
        opaHighLevelClient.queryAndEnforce(context, "ShowTables", AccessDeniedException::denyShowTables, schema);
    }

    @Override
    public Set<SchemaTableName> filterTables(SystemSecurityContext context, String catalogName, Set<SchemaTableName> tableNames)
    {
        return opaHighLevelClient.parallelFilterFromOpa(
                tableNames,
                (tbl) -> buildQueryInputForSimpleResource(
                        context,
                        "FilterTables",
                        new OpaQueryInputResource
                                .Builder()
                                .table(new TrinoTable.Builder()
                                        .catalogName(catalogName)
                                        .schemaName(tbl.getSchemaName())
                                        .tableName(tbl.getTableName())
                                        .build())
                                .build()));
    }

    @Override
    public void checkCanShowColumns(SystemSecurityContext context, CatalogSchemaTableName table)
    {
        opaHighLevelClient.queryAndEnforce(context, "ShowColumns", AccessDeniedException::denyShowColumns, table);
    }

    @Override
    public Set<String> filterColumns(SystemSecurityContext context, CatalogSchemaTableName table, Set<String> columns)
    {
        return opaHighLevelClient.parallelFilterFromOpa(
                columns,
                (column) -> buildQueryInputForSimpleResource(
                        context,
                        "FilterColumns",
                        new OpaQueryInputResource
                                .Builder()
                                .table(TrinoTable.Builder
                                        .fromTrinoTable(table)
                                        .columns(ImmutableSet.of(column))
                                        .build())
                                .build()));
    }

    @Override
    public void checkCanAddColumn(SystemSecurityContext context, CatalogSchemaTableName table)
    {
        opaHighLevelClient.queryAndEnforce(context, "AddColumn", AccessDeniedException::denyAddColumn, table);
    }

    @Override
    public void checkCanAlterColumn(SystemSecurityContext context, CatalogSchemaTableName table)
    {
        opaHighLevelClient.queryAndEnforce(context, "AlterColumn", AccessDeniedException::denyAlterColumn, table);
    }

    @Override
    public void checkCanDropColumn(SystemSecurityContext context, CatalogSchemaTableName table)
    {
        opaHighLevelClient.queryAndEnforce(context, "DropColumn", AccessDeniedException::denyDropColumn, table);
    }

    @Override
    public void checkCanSetTableAuthorization(SystemSecurityContext context, CatalogSchemaTableName table, TrinoPrincipal principal)
    {
        OpaQueryInputResource resource = new OpaQueryInputResource
                .Builder()
                .table(TrinoTable.fromTrinoTable(table))
                .build();
        OpaQueryInputGrant grantee = new OpaQueryInputGrant
                .Builder()
                .principal(TrinoGrantPrincipal.fromTrinoPrincipal(principal))
                .build();
        OpaQueryInputAction action = new OpaQueryInputAction
                .Builder()
                .operation("SetTableAuthorization")
                .resource(resource)
                .grantee(grantee)
                .build();
        OpaQueryInput input = new OpaQueryInput(OpaQueryContext.fromSystemSecurityContext(context), action);

        if (!opaHighLevelClient.queryOpa(input)) {
            denySetTableAuthorization(table.toString(), principal);
        }
    }

    @Override
    public void checkCanRenameColumn(SystemSecurityContext context, CatalogSchemaTableName table)
    {
        opaHighLevelClient.queryAndEnforce(context, "RenameColumn", AccessDeniedException::denyRenameColumn, table);
    }

    @Override
    public void checkCanSelectFromColumns(SystemSecurityContext context, CatalogSchemaTableName table, Set<String> columns)
    {
        opaHighLevelClient.queryAndEnforce(
                context,
                "SelectFromColumns",
                AccessDeniedException::denySelectColumns,
                table,
                columns);
    }

    @Override
    public void checkCanInsertIntoTable(SystemSecurityContext context, CatalogSchemaTableName table)
    {
        opaHighLevelClient.queryAndEnforce(context, "InsertIntoTable", AccessDeniedException::denyInsertTable, table);
    }

    @Override
    public void checkCanDeleteFromTable(SystemSecurityContext context, CatalogSchemaTableName table)
    {
        opaHighLevelClient.queryAndEnforce(context, "DeleteFromTable", AccessDeniedException::denyDeleteTable, table);
    }

    @Override
    public void checkCanTruncateTable(SystemSecurityContext context, CatalogSchemaTableName table)
    {
        opaHighLevelClient.queryAndEnforce(context, "TruncateTable", AccessDeniedException::denyTruncateTable, table);
    }

    @Override
    public void checkCanUpdateTableColumns(SystemSecurityContext securityContext, CatalogSchemaTableName table, Set<String> updatedColumnNames)
    {
        opaHighLevelClient.queryAndEnforce(
                securityContext,
                "UpdateTableColumns",
                AccessDeniedException::denyUpdateTableColumns,
                table,
                updatedColumnNames);
    }

    @Override
    public void checkCanCreateView(SystemSecurityContext context, CatalogSchemaTableName view)
    {
        opaHighLevelClient.queryAndEnforce(context, "CreateView", AccessDeniedException::denyCreateView, view);
    }

    @Override
    public void checkCanRenameView(SystemSecurityContext context, CatalogSchemaTableName view, CatalogSchemaTableName newView)
    {
        OpaQueryInputResource oldResource = new OpaQueryInputResource
                .Builder()
                .table(TrinoTable.fromTrinoTable(view))
                .build();
        OpaQueryInputResource newResource = new OpaQueryInputResource
                .Builder()
                .table(TrinoTable.fromTrinoTable(newView))
                .build();
        if (!opaHighLevelClient.queryOpaWithSourceAndTargetResource(context, "RenameView", oldResource, newResource)) {
            denyRenameView(view.toString(), newView.toString());
        }
    }

    @Override
    public void checkCanSetViewAuthorization(SystemSecurityContext context, CatalogSchemaTableName view, TrinoPrincipal principal)
    {
        OpaQueryInputResource resource = new OpaQueryInputResource
                .Builder()
                .table(TrinoTable.fromTrinoTable(view))
                .build();
        OpaQueryInputGrant grantee = new OpaQueryInputGrant
                .Builder()
                .principal(TrinoGrantPrincipal.fromTrinoPrincipal(principal))
                .build();
        OpaQueryInputAction action = new OpaQueryInputAction
                .Builder()
                .operation("SetViewAuthorization")
                .resource(resource)
                .grantee(grantee)
                .build();
        OpaQueryInput input = new OpaQueryInput(OpaQueryContext.fromSystemSecurityContext(context), action);

        if (!opaHighLevelClient.queryOpa(input)) {
            denySetViewAuthorization(view.toString(), principal);
        }
    }

    @Override
    public void checkCanDropView(SystemSecurityContext context, CatalogSchemaTableName view)
    {
        opaHighLevelClient.queryAndEnforce(context, "DropView", AccessDeniedException::denyDropView, view);
    }

    @Override
    public void checkCanCreateViewWithSelectFromColumns(SystemSecurityContext context, CatalogSchemaTableName table, Set<String> columns)
    {
        opaHighLevelClient.queryAndEnforce(
                context,
                "CreateViewWithSelectFromColumns",
                (tableAsString, columnSet) -> denyCreateViewWithSelect(tableAsString, context.getIdentity()),
                table,
                columns);
    }

    @Override
    public void checkCanCreateMaterializedView(SystemSecurityContext context, CatalogSchemaTableName materializedView, Map<String, Object> properties)
    {
        opaHighLevelClient.queryAndEnforce(
                context,
                "CreateMaterializedView",
                AccessDeniedException::denyCreateMaterializedView,
                materializedView,
                properties);
    }

    @Override
    public void checkCanRefreshMaterializedView(SystemSecurityContext context, CatalogSchemaTableName materializedView)
    {
        opaHighLevelClient.queryAndEnforce(context, "RefreshMaterializedView", AccessDeniedException::denyRefreshMaterializedView, materializedView);
    }

    @Override
    public void checkCanSetMaterializedViewProperties(SystemSecurityContext context, CatalogSchemaTableName materializedView, Map<String, Optional<Object>> properties)
    {
        opaHighLevelClient.queryAndEnforce(
                context,
                "SetMaterializedViewProperties",
                AccessDeniedException::denySetMaterializedViewProperties,
                materializedView,
                properties);
    }

    @Override
    public void checkCanDropMaterializedView(SystemSecurityContext context, CatalogSchemaTableName materializedView)
    {
        opaHighLevelClient.queryAndEnforce(context, "DropMaterializedView", AccessDeniedException::denyDropMaterializedView, materializedView);
    }

    @Override
    public void checkCanRenameMaterializedView(SystemSecurityContext context, CatalogSchemaTableName view, CatalogSchemaTableName newView)
    {
        OpaQueryInputResource oldResource = new OpaQueryInputResource
                .Builder()
                .table(TrinoTable.fromTrinoTable(view))
                .build();
        OpaQueryInputResource newResource = new OpaQueryInputResource
                .Builder()
                .table(TrinoTable.fromTrinoTable(newView))
                .build();
        if (!opaHighLevelClient.queryOpaWithSourceAndTargetResource(context, "RenameMaterializedView", oldResource, newResource)) {
            denyRenameMaterializedView(view.toString(), newView.toString());
        }
    }

    @Override
    public void checkCanGrantExecuteFunctionPrivilege(SystemSecurityContext context, String functionName, TrinoPrincipal grantee, boolean grantOption)
    {
        OpaQueryInputResource resource = new OpaQueryInputResource
                .Builder()
                .function(functionName)
                .build();
        OpaQueryInputGrant opaGrantee = new OpaQueryInputGrant
                .Builder()
                .principal(TrinoGrantPrincipal.fromTrinoPrincipal(grantee))
                .grantOption(grantOption)
                .build();
        OpaQueryInputAction action = new OpaQueryInputAction
                .Builder()
                .operation("GrantExecuteFunctionPrivilege")
                .resource(resource)
                .grantee(opaGrantee)
                .build();
        OpaQueryInput input = new OpaQueryInput(OpaQueryContext.fromSystemSecurityContext(context), action);

        if (!opaHighLevelClient.queryOpa(input)) {
            denyGrantExecuteFunctionPrivilege(functionName, context.getIdentity(), trinoPrincipalToString(grantee));
        }
    }

    @Override
    public void checkCanGrantExecuteFunctionPrivilege(SystemSecurityContext context, FunctionKind functionKind, CatalogSchemaRoutineName functionName, TrinoPrincipal grantee, boolean grantOption)
    {
        OpaQueryInputResource resource = new OpaQueryInputResource
                .Builder()
                .schema(new TrinoSchema
                        .Builder()
                        .catalogName(functionName.getCatalogName())
                        .schemaName(functionName.getSchemaName())
                        .build())
                .function(new TrinoFunction(functionName.getRoutineName(), functionKind.name()))
                .build();
        OpaQueryInputGrant opaGrantee = new OpaQueryInputGrant
                .Builder()
                .principal(TrinoGrantPrincipal.fromTrinoPrincipal(grantee))
                .grantOption(grantOption)
                .build();
        OpaQueryInputAction action = new OpaQueryInputAction
                .Builder()
                .operation("GrantExecuteFunctionPrivilege")
                .resource(resource)
                .grantee(opaGrantee)
                .build();
        OpaQueryInput input = new OpaQueryInput(OpaQueryContext.fromSystemSecurityContext(context), action);

        if (!opaHighLevelClient.queryOpa(input)) {
            denyGrantExecuteFunctionPrivilege(functionName.toString(), context.getIdentity(), trinoPrincipalToString(grantee));
        }
    }

    @Override
    public void checkCanSetCatalogSessionProperty(SystemSecurityContext context, String catalogName, String propertyName)
    {
        OpaQueryInputResource resource = new OpaQueryInputResource
                .Builder()
                .catalog(catalogName)
                .catalogSessionProperty(propertyName)
                .build();

        if (!opaHighLevelClient.queryOpaWithSimpleResource(context, "SetCatalogSessionProperty", resource)) {
            denySetCatalogSessionProperty(propertyName);
        }
    }

    @Override
    public void checkCanGrantSchemaPrivilege(SystemSecurityContext context, Privilege privilege, CatalogSchemaName schema, TrinoPrincipal grantee, boolean grantOption)
    {
        OpaQueryInputResource resource = new OpaQueryInputResource
                .Builder()
                .schema(TrinoSchema.fromTrinoCatalogSchema(schema))
                .build();
        OpaQueryInputGrant opaGrantee = new OpaQueryInputGrant
                .Builder()
                .principal(TrinoGrantPrincipal.fromTrinoPrincipal(grantee))
                .grantOption(grantOption)
                .privilege(privilege)
                .build();
        OpaQueryInputAction action = new OpaQueryInputAction
                .Builder()
                .operation("GrantSchemaPrivilege")
                .resource(resource)
                .grantee(opaGrantee)
                .build();
        OpaQueryInput input = new OpaQueryInput(OpaQueryContext.fromSystemSecurityContext(context), action);

        if (!opaHighLevelClient.queryOpa(input)) {
            denyGrantSchemaPrivilege(privilege.toString(), schema.toString());
        }
    }

    @Override
    public void checkCanDenySchemaPrivilege(SystemSecurityContext context, Privilege privilege, CatalogSchemaName schema, TrinoPrincipal grantee)
    {
        OpaQueryInputResource resource = new OpaQueryInputResource
                .Builder()
                .schema(TrinoSchema.fromTrinoCatalogSchema(schema))
                .build();
        OpaQueryInputGrant opaGrantee = new OpaQueryInputGrant
                .Builder()
                .principal(TrinoGrantPrincipal.fromTrinoPrincipal(grantee))
                .privilege(privilege)
                .build();
        OpaQueryInputAction action = new OpaQueryInputAction
                .Builder()
                .operation("DenySchemaPrivilege")
                .resource(resource)
                .grantee(opaGrantee)
                .build();
        OpaQueryInput input = new OpaQueryInput(OpaQueryContext.fromSystemSecurityContext(context), action);

        if (!opaHighLevelClient.queryOpa(input)) {
            denyDenySchemaPrivilege(privilege.toString(), schema.toString());
        }
    }

    @Override
    public void checkCanRevokeSchemaPrivilege(SystemSecurityContext context, Privilege privilege, CatalogSchemaName schema, TrinoPrincipal revokee, boolean grantOption)
    {
        OpaQueryInputResource resource = new OpaQueryInputResource
                .Builder()
                .schema(TrinoSchema.fromTrinoCatalogSchema(schema))
                .build();
        OpaQueryInputGrant opaGrantee = new OpaQueryInputGrant
                .Builder()
                .principal(TrinoGrantPrincipal.fromTrinoPrincipal(revokee))
                .grantOption(grantOption)
                .privilege(privilege)
                .build();
        OpaQueryInputAction action = new OpaQueryInputAction
                .Builder()
                .operation("RevokeSchemaPrivilege")
                .resource(resource)
                .grantee(opaGrantee)
                .build();
        OpaQueryInput input = new OpaQueryInput(OpaQueryContext.fromSystemSecurityContext(context), action);

        if (!opaHighLevelClient.queryOpa(input)) {
            denyRevokeSchemaPrivilege(privilege.toString(), schema.toString());
        }
    }

    @Override
    public void checkCanGrantTablePrivilege(SystemSecurityContext context, Privilege privilege, CatalogSchemaTableName table, TrinoPrincipal grantee, boolean grantOption)
    {
        OpaQueryInputResource resource = new OpaQueryInputResource
                .Builder()
                .table(TrinoTable.fromTrinoTable(table))
                .build();
        OpaQueryInputGrant opaGrantee = new OpaQueryInputGrant
                .Builder()
                .principal(TrinoGrantPrincipal.fromTrinoPrincipal(grantee))
                .grantOption(grantOption)
                .privilege(privilege)
                .build();
        OpaQueryInputAction action = new OpaQueryInputAction
                .Builder()
                .operation("GrantTablePrivilege")
                .resource(resource)
                .grantee(opaGrantee)
                .build();
        OpaQueryInput input = new OpaQueryInput(OpaQueryContext.fromSystemSecurityContext(context), action);

        if (!opaHighLevelClient.queryOpa(input)) {
            denyGrantTablePrivilege(privilege.toString(), table.toString());
        }
    }

    @Override
    public void checkCanDenyTablePrivilege(SystemSecurityContext context, Privilege privilege, CatalogSchemaTableName table, TrinoPrincipal grantee)
    {
        OpaQueryInputResource resource = new OpaQueryInputResource
                .Builder()
                .table(TrinoTable.fromTrinoTable(table))
                .build();
        OpaQueryInputGrant opaGrantee = new OpaQueryInputGrant
                .Builder()
                .principal(TrinoGrantPrincipal.fromTrinoPrincipal(grantee))
                .privilege(privilege)
                .build();
        OpaQueryInputAction action = new OpaQueryInputAction
                .Builder()
                .operation("DenyTablePrivilege")
                .resource(resource)
                .grantee(opaGrantee)
                .build();
        OpaQueryInput input = new OpaQueryInput(OpaQueryContext.fromSystemSecurityContext(context), action);

        if (!opaHighLevelClient.queryOpa(input)) {
            denyDenyTablePrivilege(privilege.toString(), table.toString());
        }
    }

    @Override
    public void checkCanRevokeTablePrivilege(SystemSecurityContext context, Privilege privilege, CatalogSchemaTableName table, TrinoPrincipal revokee, boolean grantOption)
    {
        OpaQueryInputResource resource = new OpaQueryInputResource
                .Builder()
                .table(TrinoTable.fromTrinoTable(table))
                .build();
        OpaQueryInputGrant opaRevokee = new OpaQueryInputGrant
                .Builder()
                .principal(TrinoGrantPrincipal.fromTrinoPrincipal(revokee))
                .privilege(privilege)
                .grantOption(grantOption)
                .build();
        OpaQueryInputAction action = new OpaQueryInputAction
                .Builder()
                .operation("RevokeTablePrivilege")
                .resource(resource)
                .grantee(opaRevokee)
                .build();
        OpaQueryInput input = new OpaQueryInput(OpaQueryContext.fromSystemSecurityContext(context), action);

        if (!opaHighLevelClient.queryOpa(input)) {
            denyRevokeTablePrivilege(privilege.toString(), table.toString());
        }
    }

    @Override
    public void checkCanShowRoles(SystemSecurityContext context)
    {
        opaHighLevelClient.queryAndEnforce(context, "ShowRoles", AccessDeniedException::denyShowRoles);
    }

    @Override
    public void checkCanCreateRole(SystemSecurityContext context, String role, Optional<TrinoPrincipal> grantor)
    {
        OpaQueryInputResource resource = new OpaQueryInputResource
                .Builder()
                .role(role)
                .build();
        OpaQueryInputAction action = new OpaQueryInputAction
                .Builder()
                .operation("CreateRole")
                .resource(resource)
                .grantor(TrinoGrantPrincipal.fromTrinoPrincipal(grantor))
                .build();
        OpaQueryInput input = new OpaQueryInput(OpaQueryContext.fromSystemSecurityContext(context), action);

        if (!opaHighLevelClient.queryOpa(input)) {
            denyCreateRole(role);
        }
    }

    @Override
    public void checkCanDropRole(SystemSecurityContext context, String role)
    {
        opaHighLevelClient.queryAndEnforce(context, "DropRole", AccessDeniedException::denyDropRole, OpaQueryInputResource.Builder::role, role);
    }

    @Override
    public void checkCanGrantRoles(SystemSecurityContext context, Set<String> roles, Set<TrinoPrincipal> grantees, boolean adminOption, Optional<TrinoPrincipal> grantor)
    {
        OpaQueryInputResource resource = new OpaQueryInputResource
                .Builder()
                .roles(roles)
                .build();
        OpaQueryInputGrant opaGrantees = new OpaQueryInputGrant
                .Builder()
                .grantOption(adminOption)
                .principals(grantees
                        .stream()
                        .map(TrinoGrantPrincipal::fromTrinoPrincipal)
                        .collect(toImmutableSet()))
                .build();
        OpaQueryInputAction action = new OpaQueryInputAction
                .Builder()
                .operation("GrantRoles")
                .resource(resource)
                .grantee(opaGrantees)
                .grantor(TrinoGrantPrincipal.fromTrinoPrincipal(grantor))
                .build();
        OpaQueryInput input = new OpaQueryInput(OpaQueryContext.fromSystemSecurityContext(context), action);

        if (!opaHighLevelClient.queryOpa(input)) {
            denyGrantRoles(roles, grantees);
        }
    }

    @Override
    public void checkCanRevokeRoles(SystemSecurityContext context, Set<String> roles, Set<TrinoPrincipal> grantees, boolean adminOption, Optional<TrinoPrincipal> grantor)
    {
        OpaQueryInputResource resource = new OpaQueryInputResource
                .Builder()
                .roles(roles)
                .build();
        OpaQueryInputGrant opaGrantees = new OpaQueryInputGrant
                .Builder()
                .grantOption(adminOption)
                .principals(
                        grantees
                                .stream()
                                .map(TrinoGrantPrincipal::fromTrinoPrincipal)
                                .collect(toImmutableSet()))
                .build();
        OpaQueryInputAction action = new OpaQueryInputAction
                .Builder()
                .operation("RevokeRoles")
                .resource(resource)
                .grantee(opaGrantees)
                .grantor(TrinoGrantPrincipal.fromTrinoPrincipal(grantor))
                .build();
        OpaQueryInput input = new OpaQueryInput(OpaQueryContext.fromSystemSecurityContext(context), action);

        if (!opaHighLevelClient.queryOpa(input)) {
            denyRevokeRoles(roles, grantees);
        }
    }

    @Override
    public void checkCanShowRoleAuthorizationDescriptors(SystemSecurityContext context)
    {
        opaHighLevelClient.queryAndEnforce(context, "ShowRoleAuthorizationDescriptors", AccessDeniedException::denyShowRoleAuthorizationDescriptors);
    }

    @Override
    public void checkCanShowCurrentRoles(SystemSecurityContext context)
    {
        opaHighLevelClient.queryAndEnforce(context, "ShowCurrentRoles", AccessDeniedException::denyShowCurrentRoles);
    }

    @Override
    public void checkCanShowRoleGrants(SystemSecurityContext context)
    {
        opaHighLevelClient.queryAndEnforce(context, "ShowRoleGrants", AccessDeniedException::denyShowRoleGrants);
    }

    @Override
    public void checkCanExecuteProcedure(SystemSecurityContext systemSecurityContext, CatalogSchemaRoutineName procedure)
    {
        OpaQueryInputResource resource = new OpaQueryInputResource
                .Builder()
                .schema(new TrinoSchema
                        .Builder()
                        .catalogName(procedure.getCatalogName())
                        .schemaName(procedure.getSchemaName())
                        .build())
                .function(procedure.getRoutineName())
                .build();
        if (!opaHighLevelClient.queryOpaWithSimpleResource(systemSecurityContext, "ExecuteProcedure", resource)) {
            denyExecuteProcedure(procedure.toString());
        }
    }

    @Override
    public void checkCanExecuteFunction(SystemSecurityContext systemSecurityContext, String functionName)
    {
        opaHighLevelClient.queryAndEnforce(
                systemSecurityContext,
                "ExecuteFunction",
                AccessDeniedException::denyExecuteFunction,
                OpaQueryInputResource.Builder::function,
                functionName);
    }

    @Override
    public void checkCanExecuteFunction(SystemSecurityContext systemSecurityContext, FunctionKind functionKind, CatalogSchemaRoutineName functionName)
    {
        OpaQueryInputResource resource = new OpaQueryInputResource
                .Builder()
                .schema(new TrinoSchema
                        .Builder()
                        .catalogName(functionName.getCatalogName())
                        .schemaName(functionName.getSchemaName())
                        .build())
                .function(new TrinoFunction(functionName.getRoutineName(), functionKind.name()))
                .build();

        if (!opaHighLevelClient.queryOpaWithSimpleResource(systemSecurityContext, "ExecuteFunction", resource)) {
            denyExecuteFunction(functionName.toString());
        }
    }

    @Override
    public void checkCanExecuteTableProcedure(SystemSecurityContext systemSecurityContext, CatalogSchemaTableName table, String procedure)
    {
        OpaQueryInputResource resource = new OpaQueryInputResource
                .Builder()
                .table(TrinoTable.fromTrinoTable(table))
                .function(procedure)
                .build();

        if (!opaHighLevelClient.queryOpaWithSimpleResource(systemSecurityContext, "ExecuteTableProcedure", resource)) {
            denyExecuteTableProcedure(table.toString(), procedure);
        }
    }
}
