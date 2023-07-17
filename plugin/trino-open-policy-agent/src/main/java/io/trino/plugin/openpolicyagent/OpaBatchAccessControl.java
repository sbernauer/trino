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

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.inject.Inject;
import io.trino.spi.connector.CatalogSchemaTableName;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.security.Identity;
import io.trino.spi.security.SystemSecurityContext;

import java.net.URI;
import java.net.http.HttpClient;
import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class OpaBatchAccessControl
        extends OpaAccessControl
{
    private final URI opaBatchedPolicyUri;

    public record OpaBatchQueryResult(@JsonProperty("decision_id") String decisionId, List<Integer> result)
    { }

    @Inject
    public OpaBatchAccessControl(OpaConfig config)
    {
        super(config);
        this.opaBatchedPolicyUri = config.getOpaBatchUri().orElseThrow();
    }

    public OpaBatchAccessControl(OpaConfig config, HttpClient httpClient)
    {
        super(config, httpClient);
        this.opaBatchedPolicyUri = config.getOpaBatchUri().orElseThrow();
    }

    private List<Integer> batchQueryOpa(OpaQueryInput input)
    {
        if (input.action().filterResources == null) {
            throw new OpaQueryException.OpaInternalPluginError("Cannot send a batch request without a collection of resources");
        }
        List<Integer> result = tryGetResponseFromOpa(input, opaBatchedPolicyUri, OpaBatchQueryResult.class).result();
        if (result == null) {
            return List.of();
        }
        return result;
    }

    private <T> Set<T> filterFromOpa(SystemSecurityContext context, String operation, Collection<T> items, Function<Stream<T>, List<OpaQueryInputResource>> converter)
    {
        List<T> orderedItems = List.copyOf(items);
        if (orderedItems.isEmpty()) {
            return Set.of();
        }
        OpaQueryInputAction action = new OpaQueryInputAction.Builder()
                .operation(operation)
                .filterResources(converter.apply(orderedItems.stream()))
                .build();
        OpaQueryInput query = new OpaQueryInput(context, action);
        return batchQueryOpa(query).stream().map(orderedItems::get).collect(Collectors.toSet());
    }

    private <T> Function<Stream<T>, List<OpaQueryInputResource>> mapItemToResource(Function<T, OpaQueryInputResource> converter)
    {
        return (s) -> s.map(converter).toList();
    }

    @Override
    public Collection<Identity> filterViewQueryOwnedBy(SystemSecurityContext context, Collection<Identity> queryOwners)
    {
        return filterFromOpa(
                context,
                "FilterViewQueryOwnedBy",
                queryOwners,
                mapItemToResource((item) -> new OpaQueryInputResource.Builder().user(new OpaQueryInputResource.User(item)).build()));
    }

    @Override
    public Set<String> filterCatalogs(SystemSecurityContext context, Set<String> catalogs)
    {
        return filterFromOpa(context, "FilterCatalogs", catalogs, mapItemToResource((i) -> new OpaQueryInputResource.Builder().catalog(i).build()));
    }

    @Override
    public Set<String> filterSchemas(SystemSecurityContext context, String catalogName, Set<String> schemaNames)
    {
        return filterFromOpa(context, "FilterSchemas", schemaNames, mapItemToResource((i) -> new OpaQueryInputResource.Builder().schema(new OpaQueryInputResource.CatalogSchema(catalogName, i)).build()));
    }

    @Override
    public Set<SchemaTableName> filterTables(SystemSecurityContext context, String catalogName, Set<SchemaTableName> tableNames)
    {
        return filterFromOpa(context, "FilterTables", tableNames, mapItemToResource((i) -> new OpaQueryInputResource.Builder().table(new OpaQueryInputResource.Table(catalogName, i.getSchemaName(), i.getTableName())).build()));
    }

    @Override
    public Set<String> filterColumns(SystemSecurityContext context, CatalogSchemaTableName table, Set<String> columns)
    {
        return filterFromOpa(
                context,
                "FilterColumns",
                columns,
                (s) -> List.of(new OpaQueryInputResource.Builder().table(new OpaQueryInputResource.Table(table, s.collect(Collectors.toSet()))).build()));
    }
}
