package trino

import future.keywords.contains
import future.keywords.if
import future.keywords.in

default allow = false

group_member(group) if input.context.identity.user in data.groups[group]
user_is_group_member(user, group) if user in data.groups[group]

# TODO REMOVE ###########################################
allow {
    input.action.operation in ["SelectFromColumns"]
}
# TODO REMOVE ###########################################

# Admins can do anything
allow {
    group_member("admin")
}
extended[i] {
    some i
    input.action.filterResources[i]
    group_member("admin")
}

# Every can execute queries
allow {
    input.action.operation == "ExecuteQuery"
}

allow {
    input.action.operation in [
        "AccessCatalog",
        "SetCatalogSessionProperty",
        "ShowSchemas",
    ]

    has_catalog_permission(input.action.resource.catalog.name, "ro")
}

allow {
    input.action.operation in [
        "AccessCatalog",
        "SetCatalogSessionProperty",
        "ShowSchemas",
    ]

    has_permission_for_any_schema_in_catalog(input.action.resource.catalog.name, "ro")
}

allow {
    input.action.operation in [
        "CreateSchema",
        "DropSchema",
        "RenameSchema",
    ]

    has_catalog_permission(input.action.resource.schema.catalogName, "full")
}

allow {
    input.action.operation in [
        "CreateTable",
        "DropTable",
        "RenameTable",
    ]

    has_schema_permission(input.action.resource.table.catalogName, input.action.resource.table.schemaName, "full")
}

allow {
    input.action.operation in [
        "CreateView",
        "DropView",
        "RenameView",
        "CreateMaterializedView",
        "DropMaterializedView",
        "RenameMaterializedView",
        "SetMaterializedViewProperties",
    ]

    has_schema_permission(input.action.resource.view.catalogName, input.action.resource.view.schemaName, "full")
}

extended[i] {
    input.action.operation == "FilterCatalogs"
    some i
    has_catalog_permission(input.action.filterResources[i].catalog.name, "ro")
}

extended[i] {
    input.action.operation == "FilterCatalogs"
    some i
    has_permission_for_any_schema_in_catalog(input.action.filterResources[i].catalog.name, "ro")
}

extended[i] {
    input.action.operation == "FilterSchemas"
    some i
    has_schema_permission(input.action.filterResources[i].schema.catalogName, input.action.filterResources[i].schema.schemaName, "ro")
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
            "ro": ["anyone"],
        },
    ]
}
