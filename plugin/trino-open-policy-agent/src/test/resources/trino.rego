package trino

import future.keywords.contains
import future.keywords.if
import future.keywords.in

default allow = false

group_member(group) if input.context.identity.user in data.groups[group]
user_is_group_member(user, group) if user in data.groups[group]

# Admins can do anything
allow {
    group_member("admin")
}
extended[i] {
    some i
    input.action.filterResources[i]
    group_member("admin")
}

# Everyone can execute queries
allow {
    input.action.operation in ["ExecuteQuery", "ExecuteFunction", "ReadSystemInformation"]
}

# Everyone can see and kill his own queries
allow {
    input.action.operation in ["ViewQueryOwnedBy", "KillQueryOwnedBy"]
    input.action.resource.user.name == input.context.identity.user
}

allow {
    input.action.operation in [
        "AccessCatalog",
        "SetCatalogSessionProperty",
        "ShowSchemas",
    ]

    has_permission_for_any_table_in_catalog(input.action.resource.catalog.name, "ro")
}

allow {
    input.action.operation in [
        "CreateSchema",
        "DropSchema",
    ]

    has_schema_permission(input.action.resource.schema.catalogName, input.action.resource.schema.schemaName, "full")
}

allow {
    input.action.operation in [
        "RenameSchema",
    ]

    has_schema_permission(input.action.resource.schema.catalogName, input.action.resource.schema.schemaName, "full")
    has_schema_permission(input.action.targetResource.schema.catalogName, input.action.targetResource.schema.schemaName, "full")
}

allow {
    input.action.operation in [
        "CreateTable",
        "DropTable",
        "AddColumn",
        "DropColumn",
        "RenameColumn",
        "UpdateTableColumns",
        "SetColumnComment",
        "SetTableProperties",
        "SetTableComment",
        "CreateViewWithSelectFromColumns", # This has input.action.resource.table set instead of input.action.resource.view
    ]

    has_table_permission(input.action.resource.table.catalogName, input.action.resource.table.schemaName, input.action.resource.table.tableName, "full")
}

allow {
    input.action.operation in [
        "CreateView",
        "DropView",
        "DropMaterializedView",
        "SetMaterializedViewProperties",
    ]

    has_table_permission(input.action.resource.view.catalogName, input.action.resource.view.schemaName, input.action.resource.view.tableName, "full")
}

allow {
    input.action.operation in [
        "RefreshMaterializedView",
    ]

    has_table_permission(input.action.resource.view.catalogName, input.action.resource.view.schemaName, input.action.resource.view.tableName, "rw")
}

allow {
    input.action.operation in [
        "InsertIntoTable",
        "DeleteFromTable",
        "TruncateTable",
    ]

    has_table_permission(input.action.resource.table.catalogName, input.action.resource.table.schemaName, input.action.resource.table.tableName, "rw")
}

allow {
    input.action.operation in [
        "SelectFromColumns",
        "ShowCreateTable"
    ]

    has_table_permission(input.action.resource.table.catalogName, input.action.resource.table.schemaName, input.action.resource.table.tableName, "ro")
}

allow {
    input.action.operation in [
        "RenameTable",
    ]

    has_table_permission(input.action.resource.table.catalogName, input.action.resource.table.schemaName, input.action.resource.table.tableName, "full")
    has_table_permission(input.action.targetResource.table.catalogName, input.action.targetResource.table.schemaName, input.action.targetResource.table.tableName, "full")
}

allow {
    input.action.operation in [
        "RenameView",
        "RenameMaterializedView",
    ]

    has_table_permission(input.action.resource.view.catalogName, input.action.resource.view.schemaName, input.action.resource.table.tableName, "full")
    has_table_permission(input.action.targetResource.view.catalogName, input.action.targetResource.view.schemaName, input.action.targetResource.view.tableName, "full")
}

allow {
    input.action.operation in [
        "RenameView",
    ]

    has_schema_permission(input.action.resource.view.catalogName, input.action.resource.view.schemaName, "full")
    has_schema_permission(input.action.targetResource.view.catalogName, input.action.targetResource.view.schemaName, "full")
}

allow {
    input.action.operation in [
        "ShowCreateSchema",
        "ShowTables",
    ]

    has_permission_for_any_table_in_schema(input.action.resource.schema.catalogName, input.action.resource.schema.schemaName, "ro")
}

extended[i] {
    input.action.operation == "FilterCatalogs"
    some i
    has_permission_for_any_table_in_catalog(input.action.filterResources[i].catalog.name, "ro")
}

extended[i] {
    input.action.operation == "FilterSchemas"
    some i
    has_permission_for_any_table_in_schema(input.action.filterResources[i].schema.catalogName, input.action.filterResources[i].schema.schemaName, "ro")
}

extended[i] {
    input.action.operation == "FilterTables"
    some i
    has_table_permission(input.action.filterResources[i].schema.catalogName, input.action.filterResources[i].schema.schemaName, input.action.filterResources[i].schema.tableName, "ro")
}

grant_hierarchy := {
    "full": ["full", "rw","ro"],
    "rw": ["rw", "ro"],
    "ro": ["ro"],
}

has_catalog_permission(catalog, permission) {
    some group
    input.context.identity.user in data.groups[group]

    some grant
    permission in grant_hierarchy[grant]

    some catalog_id
    data.catalog_acls[catalog_id].catalog = catalog
    group in data.catalog_acls[catalog_id][grant]
}

# Needed internally, e.g. for trino clients or JDBC clients
has_catalog_permission(catalog, permission) {
    catalog = "system"
    permission = "ro"
}

has_schema_permission(catalog, schema, permission) {
    some group
    input.context.identity.user in data.groups[group]

    some grant
    permission in grant_hierarchy[grant]

    some schema_id
    data.schema_acls[schema_id].catalog = catalog
    data.schema_acls[schema_id].schema = schema
    group in data.schema_acls[schema_id][grant]
}

# Permissions granted on catalog level are inherited for schemas as well
has_schema_permission(catalog, schema, permission) {
    has_catalog_permission(catalog, permission)
}

has_permission_for_any_schema_in_catalog(catalog, permission) {
    some schema
    some schema_id
    data.schema_acls[schema_id].schema = schema
    has_schema_permission(catalog, schema, permission)
}

# We might need this explicitly, as their might be no schema within this catalog in data.schema_acls
has_permission_for_any_schema_in_catalog(catalog, permission) {
    has_catalog_permission(catalog, permission)
}

has_table_permission(catalog, schema, table, permission) {
    some group
    input.context.identity.user in data.groups[group]

    some grant
    permission in grant_hierarchy[grant]

    some table_id
    data.table_acls[table_id].catalog = catalog
    data.table_acls[table_id].schema = schema
    data.table_acls[table_id].table = table
    group in data.table_acls[table_id][grant]
}

# Permissions granted on schema level are inherited for tables as well
has_table_permission(catalog, schema, table, permission) {
    has_schema_permission(catalog, schema, permission)
}

allow {
    input.action.operation in ["SelectFromColumns"]
    input.action.resource.table.schemaName = "information_schema"
    input.action.resource.table.tableName = "schemata"
    has_permission_for_any_table_in_catalog(input.action.resource.table.catalogName, "ro")
}

has_permission_for_any_table_in_schema(catalog, schema, permission) {
    some table
    some table_id
    data.table_acls[table_id].schema = schema
    data.table_acls[table_id].table = table
    has_table_permission(catalog, schema, table, permission)
}

# We might need this explicitly, as their might be no table within this schema in data.table_acls
has_permission_for_any_table_in_schema(catalog, schema, permission) {
    has_schema_permission(catalog, schema, permission)
}

has_permission_for_any_table_in_catalog(catalog, permission) {
    some schema
    some schema_id
    some table
    some table_id
    data.schema_acls[schema_id].schema = schema
    data.table_acls[table_id].schema = schema
    data.table_acls[table_id].table = table
    has_schema_permission(catalog, schema, permission)
    has_table_permission(catalog, schema, table, permission)
}

has_permission_for_any_table_in_catalog(catalog, permission) {
    some schema
    some schema_id
    data.schema_acls[schema_id].schema = schema
    has_schema_permission(catalog, schema, permission)
}

has_permission_for_any_table_in_catalog(catalog, permission) {
    has_catalog_permission(catalog, permission)
}

data := {
    "groups" : {
        "admin": ["admin"], # Special group that can do everything
        "impersonating": ["superset"], # Special group that can nothing besides impersonating others (but not admins)

        # Normal users groups
        "data-analysts": ["data-analyst-1", "data-analyst-2", "data-analyst-3"],
        "customer-1": ["customer-1-user-1", "customer-1-user-2"],
        "customer-2": ["customer-2-user-1", "customer-2-user-2"],
    },
    "catalog_acls" : [
        {
            "catalog": "lakehouse",
            "full": ["data-analysts"],
        },
        {
            "catalog": "catalog_without_schemas_acls",
            "full": ["data-analysts"],
        },
        {
            "catalog": "tpch",
            "ro": ["data-analysts"],
        },
    ],
    "schema_acls" : [
        {
            "catalog": "lakehouse",
            "schema": "customer_1",
            "ro": ["customer-1"],
        },
        {
            "catalog": "lakehouse",
            "schema": "customer_2",
            "rw": ["customer-2"],
        }
    ],
    "table_acls" : [
        {
            "catalog": "lakehouse",
            "schema": "customer_1",
            "table": "public_export",
            "ro": ["customer-2"],
        },
    ]
}
