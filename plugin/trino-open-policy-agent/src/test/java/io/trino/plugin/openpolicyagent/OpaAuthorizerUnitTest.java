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

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.json.JsonMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.collect.Streams;
import io.trino.spi.connector.CatalogSchemaName;
import io.trino.spi.connector.CatalogSchemaRoutineName;
import io.trino.spi.connector.CatalogSchemaTableName;
import io.trino.spi.function.FunctionKind;
import io.trino.spi.security.Identity;
import io.trino.spi.security.PrincipalType;
import io.trino.spi.security.Privilege;
import io.trino.spi.security.SystemSecurityContext;
import io.trino.spi.security.TrinoPrincipal;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Named;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpResponse;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.stream.Stream;

import static io.trino.plugin.openpolicyagent.HttpClientUtils.InstrumentedHttpClient;
import static io.trino.plugin.openpolicyagent.RequestTestUtilities.assertJsonRequestsEqual;
import static io.trino.plugin.openpolicyagent.RequestTestUtilities.assertStringRequestsEqual;
import static io.trino.plugin.openpolicyagent.TestHelpers.OK_RESPONSE;
import static io.trino.plugin.openpolicyagent.TestHelpers.createFailingTestCases;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class OpaAuthorizerUnitTest
{
    private static URI opaServerUri = URI.create("http://my-uri/");
    private InstrumentedHttpClient mockClient;
    private OpaAuthorizer authorizer;
    private JsonMapper jsonMapper = new JsonMapper();
    private Identity requestingIdentity;
    private SystemSecurityContext requestingSecurityContext;

    @BeforeEach
    public void setupAuthorizer()
            throws InterruptedException, IOException
    {
        this.mockClient = new InstrumentedHttpClient();
        this.authorizer = new OpaAuthorizer(new OpaConfig().setOpaUri(opaServerUri), this.mockClient.getHttpClient());
        this.requestingIdentity = Identity.ofUser("source-user");
        this.requestingSecurityContext = new SystemSecurityContext(requestingIdentity, Optional.empty());
        this.mockClient.setHandler((request) -> OK_RESPONSE);
    }

    @AfterEach
    public void ensureRequestContextCorrect()
            throws IOException
    {
        for (String request : mockClient.getRequests()) {
            JsonNode parsedRequest = jsonMapper.readTree(request);
            assertEquals(parsedRequest.at("/input/context/identity/user").asText(), requestingIdentity.getUser());
        }
    }

    private static Stream<Arguments> noResourceActionTestCases()
    {
        Stream<BiConsumer<OpaAuthorizer, SystemSecurityContext>> methods =
                Stream.of(
                        OpaAuthorizer::checkCanExecuteQuery,
                        OpaAuthorizer::checkCanReadSystemInformation,
                        OpaAuthorizer::checkCanWriteSystemInformation,
                        OpaAuthorizer::checkCanShowRoles,
                        OpaAuthorizer::checkCanShowRoleAuthorizationDescriptors,
                        OpaAuthorizer::checkCanShowCurrentRoles,
                        OpaAuthorizer::checkCanShowRoleGrants);
        Stream<String> expectedActions = Stream.of(
                "ExecuteQuery",
                "ReadSystemInformation",
                "WriteSystemInformation",
                "ShowRoles",
                "ShowRoleAuthorizationDescriptors",
                "ShowCurrentRoles",
                "ShowRoleGrants");
        return Streams.zip(expectedActions, methods, (action, method) -> Arguments.of(Named.of(action, action), method));
    }

    private static Stream<Arguments> noResourceActionFailureTestCases()
    {
        return createFailingTestCases(noResourceActionTestCases());
    }

    @ParameterizedTest(name = "{index}: {0}")
    @MethodSource("io.trino.plugin.openpolicyagent.OpaAuthorizerUnitTest#noResourceActionTestCases")
    public void testNoResourceAction(String actionName, BiConsumer<OpaAuthorizer, SystemSecurityContext> method)
    {
        method.accept(authorizer, requestingSecurityContext);
        ObjectNode expectedRequest = jsonMapper.createObjectNode().put("operation", actionName);
        assertJsonRequestsEqual(Set.of(expectedRequest), mockClient.getRequests(), "/input/action");
    }

    @ParameterizedTest(name = "{index}: {0} - {2}")
    @MethodSource("io.trino.plugin.openpolicyagent.OpaAuthorizerUnitTest#noResourceActionFailureTestCases")
    public void testNoResourceActionFailure(
            String actionName,
            BiConsumer<OpaAuthorizer, SystemSecurityContext> method,
            HttpResponse<String> failureResponse,
            Class<? extends Throwable> expectedException,
            String expectedErrorMessage)
    {
        mockClient.setHandler((request) -> failureResponse);

        Throwable actualError = assertThrows(expectedException, () -> method.accept(authorizer, requestingSecurityContext));
        ObjectNode expectedRequest = jsonMapper.createObjectNode().put("operation", actionName);
        assertJsonRequestsEqual(Set.of(expectedRequest), mockClient.getRequests(), "/input/action");
        assertTrue(actualError.getMessage().contains(expectedErrorMessage),
                String.format("Error must contain '%s': %s", expectedErrorMessage, actualError.getMessage()));
    }

    private static Stream<Arguments> tableResourceTestCases()
    {
        Stream<FunctionalHelpers.Consumer3<OpaAuthorizer, SystemSecurityContext, CatalogSchemaTableName>> methods = Stream.of(
                OpaAuthorizer::checkCanShowCreateTable,
                OpaAuthorizer::checkCanDropTable,
                OpaAuthorizer::checkCanSetTableComment,
                OpaAuthorizer::checkCanSetColumnComment,
                OpaAuthorizer::checkCanShowColumns,
                OpaAuthorizer::checkCanAddColumn,
                OpaAuthorizer::checkCanDropColumn,
                OpaAuthorizer::checkCanRenameColumn,
                OpaAuthorizer::checkCanInsertIntoTable,
                OpaAuthorizer::checkCanDeleteFromTable,
                OpaAuthorizer::checkCanTruncateTable,
                OpaAuthorizer::checkCanCreateView,
                OpaAuthorizer::checkCanDropView,
                OpaAuthorizer::checkCanRefreshMaterializedView,
                OpaAuthorizer::checkCanDropMaterializedView);
        Stream<FunctionalHelpers.Pair<String, String>> actionAndResource = Stream.of(
                FunctionalHelpers.Pair.of("ShowCreateTable", "table"),
                FunctionalHelpers.Pair.of("DropTable", "table"),
                FunctionalHelpers.Pair.of("SetTableComment", "table"),
                FunctionalHelpers.Pair.of("SetColumnComment", "table"),
                FunctionalHelpers.Pair.of("ShowColumns", "table"),
                FunctionalHelpers.Pair.of("AddColumn", "table"),
                FunctionalHelpers.Pair.of("DropColumn", "table"),
                FunctionalHelpers.Pair.of("RenameColumn", "table"),
                FunctionalHelpers.Pair.of("InsertIntoTable", "table"),
                FunctionalHelpers.Pair.of("DeleteFromTable", "table"),
                FunctionalHelpers.Pair.of("TruncateTable", "table"),
                FunctionalHelpers.Pair.of("CreateView", "view"),
                FunctionalHelpers.Pair.of("DropView", "view"),
                FunctionalHelpers.Pair.of("RefreshMaterializedView", "view"),
                FunctionalHelpers.Pair.of("DropMaterializedView", "view"));
        return Streams.zip(actionAndResource, methods, (action, method) -> Arguments.of(Named.of(action.getFirst(), action.getFirst()), action.getSecond(), method));
    }

    @ParameterizedTest(name = "{index}: {0}")
    @MethodSource("io.trino.plugin.openpolicyagent.OpaAuthorizerUnitTest#tableResourceTestCases")
    public void testTableResourceActions(
            String actionName,
            String resourceName,
            FunctionalHelpers.Consumer3<OpaAuthorizer, SystemSecurityContext, CatalogSchemaTableName> callable)
    {
        callable.accept(authorizer, requestingSecurityContext, new CatalogSchemaTableName("my-catalog", "my-schema", "my-table"));

        String expectedRequest = """
                {
                    "operation": "%s",
                    "resource": {
                        "%s": {
                            "catalogName": "my-catalog",
                            "schemaName": "my-schema",
                            "tableName": "my-table"
                        }
                    }
                }
                """.formatted(actionName, resourceName);
        assertStringRequestsEqual(Set.of(expectedRequest), mockClient.getRequests(), "/input/action");
    }

    private static Stream<Arguments> tableResourceFailureTestCases()
    {
        return createFailingTestCases(tableResourceTestCases());
    }

    @ParameterizedTest(name = "{index}: {0} - {3}")
    @MethodSource("io.trino.plugin.openpolicyagent.OpaAuthorizerUnitTest#tableResourceFailureTestCases")
    public void testTableResourceFailure(
            String actionName,
            String resourceName,
            FunctionalHelpers.Consumer3<OpaAuthorizer, SystemSecurityContext, CatalogSchemaTableName> method,
            HttpResponse<String> failureResponse,
            Class<? extends Throwable> expectedException,
            String expectedErrorMessage)
    {
        mockClient.setHandler((request) -> failureResponse);

        Throwable actualError = assertThrows(expectedException, () -> method.accept(authorizer, requestingSecurityContext, new CatalogSchemaTableName("catalog", "schema", "table")));
        assertTrue(actualError.getMessage().contains(expectedErrorMessage),
                String.format("Error must contain '%s': %s", expectedErrorMessage, actualError.getMessage()));
    }

    private static Stream<Arguments> tableWithPropertiesTestCases()
    {
        Stream<FunctionalHelpers.Consumer4<OpaAuthorizer, SystemSecurityContext, CatalogSchemaTableName, Map>> methods = Stream.of(
                OpaAuthorizer::checkCanSetTableProperties,
                OpaAuthorizer::checkCanSetMaterializedViewProperties,
                OpaAuthorizer::checkCanCreateTable,
                OpaAuthorizer::checkCanCreateMaterializedView);
        Stream<FunctionalHelpers.Pair<String, String>> actionAndResource = Stream.of(
                FunctionalHelpers.Pair.of("SetTableProperties", "table"),
                FunctionalHelpers.Pair.of("SetMaterializedViewProperties", "view"),
                FunctionalHelpers.Pair.of("CreateTable", "table"),
                FunctionalHelpers.Pair.of("CreateMaterializedView", "view"));
        return Streams.zip(actionAndResource, methods, (action, method) -> Arguments.of(Named.of(action.getFirst(), action.getFirst()), action.getSecond(), method));
    }

    @ParameterizedTest(name = "{index}: {0}")
    @MethodSource("io.trino.plugin.openpolicyagent.OpaAuthorizerUnitTest#tableWithPropertiesTestCases")
    public void testTableWithPropertiesActions(
            String actionName,
            String resourceName,
            FunctionalHelpers.Consumer4<OpaAuthorizer, SystemSecurityContext, CatalogSchemaTableName, Map> callable)
    {
        CatalogSchemaTableName table = new CatalogSchemaTableName("my-catalog", "my-schema", "my-table");
        Map<String, Optional<Object>> properties = Map.of(
                "string-item", Optional.of("string-value"),
                "empty-item", Optional.empty(),
                "boxed-number-item", Optional.of(Integer.valueOf(32)));

        callable.accept(authorizer, requestingSecurityContext, table, properties);

        String expectedRequest = """
                {
                    "operation": "%s",
                    "resource": {
                        "%s": {
                            "tableName": "my-table",
                            "catalogName": "my-catalog",
                            "schemaName": "my-schema",
                            "properties": {
                                "string-item": "string-value",
                                "empty-item": null,
                                "boxed-number-item": 32
                            }
                        }
                    }
                }
                """.formatted(actionName, resourceName);
        assertStringRequestsEqual(Set.of(expectedRequest), mockClient.getRequests(), "/input/action");
    }

    private static Stream<Arguments> tableWithPropertiesFailureTestCases()
    {
        return createFailingTestCases(tableWithPropertiesTestCases());
    }

    @ParameterizedTest(name = "{index}: {0} - {3}")
    @MethodSource("io.trino.plugin.openpolicyagent.OpaAuthorizerUnitTest#tableWithPropertiesFailureTestCases")
    public void testTableWithPropertiesActionFailure(
            String actionName,
            String resourceName,
            FunctionalHelpers.Consumer4<OpaAuthorizer, SystemSecurityContext, CatalogSchemaTableName, Map> method,
            HttpResponse<String> failureResponse,
            Class<? extends Throwable> expectedException,
            String expectedErrorMessage)
    {
        mockClient.setHandler((request) -> failureResponse);

        Throwable actualError = assertThrows(expectedException, () -> method.accept(authorizer, requestingSecurityContext, new CatalogSchemaTableName("catalog", "schema", "table"), Map.of()));
        assertTrue(actualError.getMessage().contains(expectedErrorMessage),
                String.format("Error must contain '%s': %s", expectedErrorMessage, actualError.getMessage()));
    }

    private static Stream<Arguments> identityResourceTestCases()
    {
        Stream<FunctionalHelpers.Consumer3<OpaAuthorizer, SystemSecurityContext, Identity>> methods = Stream.of(
                OpaAuthorizer::checkCanViewQueryOwnedBy,
                OpaAuthorizer::checkCanKillQueryOwnedBy);
        Stream<String> actions = Stream.of(
                "ViewQueryOwnedBy",
                "KillQueryOwnedBy");
        return Streams.zip(actions, methods, (action, method) -> Arguments.of(Named.of(action, action), method));
    }

    @ParameterizedTest(name = "{index}: {0}")
    @MethodSource("io.trino.plugin.openpolicyagent.OpaAuthorizerUnitTest#identityResourceTestCases")
    public void testIdentityResourceActions(
            String actionName,
            FunctionalHelpers.Consumer3<OpaAuthorizer, SystemSecurityContext, Identity> callable)
    {
        Identity dummyIdentity = Identity.forUser("dummy-user")
                .withGroups(Set.of("some-group"))
                .withExtraCredentials(Map.of("some-extra-credential", "value"))
                .build();
        callable.accept(authorizer, requestingSecurityContext, dummyIdentity);

        String expectedRequest = """
                {
                    "operation": "%s",
                    "resource": {
                        "user": {
                            "name": "dummy-user",
                            "user": "dummy-user",
                            "groups": ["some-group"],
                            "principal": null,
                            "enabledRoles": [],
                            "catalogRoles": {},
                            "extraCredentials": {"some-extra-credential": "value"},
                            "roles": {}
                        }
                    }
                }
                """.formatted(actionName);
        assertStringRequestsEqual(Set.of(expectedRequest), mockClient.getRequests(), "/input/action");
    }

    private static Stream<Arguments> identityResourceFailureTestCases()
    {
        return createFailingTestCases(identityResourceTestCases());
    }

    @ParameterizedTest(name = "{index}: {0} - {2}")
    @MethodSource("io.trino.plugin.openpolicyagent.OpaAuthorizerUnitTest#identityResourceFailureTestCases")
    public void testIdentityResourceActionsFailure(
            String actionName,
            FunctionalHelpers.Consumer3<OpaAuthorizer, SystemSecurityContext, Identity> method,
            HttpResponse<String> failureResponse,
            Class<? extends Throwable> expectedException,
            String expectedErrorMessage)
    {
        mockClient.setHandler((request) -> failureResponse);

        Throwable actualError = assertThrows(expectedException, () -> method.accept(authorizer, requestingSecurityContext, Identity.ofUser("dummy-user")));
        assertTrue(actualError.getMessage().contains(expectedErrorMessage),
                String.format("Error must contain '%s': %s", expectedErrorMessage, actualError.getMessage()));
    }

    private static Stream<Arguments> stringResourceTestCases()
    {
        Stream<FunctionalHelpers.Consumer3<OpaAuthorizer, SystemSecurityContext, String>> methods = Stream.of(
                OpaAuthorizer::checkCanImpersonateUser,
                OpaAuthorizer::checkCanSetSystemSessionProperty,
                OpaAuthorizer::checkCanAccessCatalog,
                OpaAuthorizer::checkCanShowSchemas,
                OpaAuthorizer::checkCanDropRole,
                OpaAuthorizer::checkCanExecuteFunction);
        Stream<FunctionalHelpers.Pair<String, String>> actionAndResource = Stream.of(
                FunctionalHelpers.Pair.of("ImpersonateUser", "user"),
                FunctionalHelpers.Pair.of("SetSystemSessionProperty", "systemSessionProperty"),
                FunctionalHelpers.Pair.of("AccessCatalog", "catalog"),
                FunctionalHelpers.Pair.of("ShowSchemas", "catalog"),
                FunctionalHelpers.Pair.of("DropRole", "role"),
                FunctionalHelpers.Pair.of("ExecuteFunction", "function"));
        return Streams.zip(actionAndResource, methods, (action, method) -> Arguments.of(Named.of(action.getFirst(), action.getFirst()), action.getSecond(), method));
    }

    @ParameterizedTest(name = "{index}: {0}")
    @MethodSource("io.trino.plugin.openpolicyagent.OpaAuthorizerUnitTest#stringResourceTestCases")
    public void testStringResourceAction(
            String actionName,
            String resourceName,
            FunctionalHelpers.Consumer3<OpaAuthorizer, SystemSecurityContext, String> callable)
    {
        callable.accept(authorizer, requestingSecurityContext, "dummy-name");

        String expectedRequest = """
                {
                    "operation": "%s",
                    "resource": {
                        "%s": {
                            "name": "dummy-name"
                        }
                    }
                }
                """.formatted(actionName, resourceName);
        assertStringRequestsEqual(Set.of(expectedRequest), mockClient.getRequests(), "/input/action");
    }

    public static Stream<Arguments> stringResourceFailureTestCases()
    {
        return createFailingTestCases(stringResourceTestCases());
    }

    @ParameterizedTest(name = "{index}: {0} - {3}")
    @MethodSource("io.trino.plugin.openpolicyagent.OpaAuthorizerUnitTest#stringResourceFailureTestCases")
    public void testStringResourceActionsFailure(
            String actionName,
            String resourceName,
            FunctionalHelpers.Consumer3<OpaAuthorizer, SystemSecurityContext, String> method,
            HttpResponse<String> failureResponse,
            Class<? extends Throwable> expectedException,
            String expectedErrorMessage)
    {
        mockClient.setHandler((request) -> failureResponse);

        Throwable actualError = assertThrows(expectedException, () -> method.accept(authorizer, requestingSecurityContext, "dummy-value"));
        assertTrue(actualError.getMessage().contains(expectedErrorMessage),
                String.format("Error must contain '%s': %s", expectedErrorMessage, actualError.getMessage()));
    }

    private static Stream<Arguments> schemaResourceTestCases()
    {
        Stream<FunctionalHelpers.Consumer3<OpaAuthorizer, SystemSecurityContext, CatalogSchemaName>> methods = Stream.of(
                OpaAuthorizer::checkCanDropSchema,
                OpaAuthorizer::checkCanShowCreateSchema,
                OpaAuthorizer::checkCanShowTables);
        Stream<String> actions = Stream.of(
                "DropSchema",
                "ShowCreateSchema",
                "ShowTables");
        return Streams.zip(actions, methods, (action, method) -> Arguments.of(Named.of(action, action), method));
    }

    @ParameterizedTest(name = "{index}: {0}")
    @MethodSource("io.trino.plugin.openpolicyagent.OpaAuthorizerUnitTest#schemaResourceTestCases")
    public void testSchemaResourceActions(
            String actionName,
            FunctionalHelpers.Consumer3<OpaAuthorizer, SystemSecurityContext, CatalogSchemaName> callable)
    {
        callable.accept(authorizer, requestingSecurityContext, new CatalogSchemaName("my-catalog", "my-schema"));

        String expectedRequest = """
                {
                    "operation": "%s",
                    "resource": {
                        "schema": {
                            "catalogName": "my-catalog",
                            "schemaName": "my-schema"
                        }
                    }
                }
                """.formatted(actionName);
        assertStringRequestsEqual(Set.of(expectedRequest), mockClient.getRequests(), "/input/action");
    }

    public static Stream<Arguments> schemaResourceFailureTestCases()
    {
        return createFailingTestCases(schemaResourceTestCases());
    }

    @ParameterizedTest(name = "{index}: {0} - {2}")
    @MethodSource("io.trino.plugin.openpolicyagent.OpaAuthorizerUnitTest#schemaResourceFailureTestCases")
    public void testSchemaResourceActionsFailure(
            String actionName,
            FunctionalHelpers.Consumer3<OpaAuthorizer, SystemSecurityContext, CatalogSchemaName> method,
            HttpResponse<String> failureResponse,
            Class<? extends Throwable> expectedException,
            String expectedErrorMessage)
    {
        mockClient.setHandler((request) -> failureResponse);

        Throwable actualError = assertThrows(expectedException, () -> method.accept(authorizer, requestingSecurityContext, new CatalogSchemaName("dummy-catalog", "dummy-schema")));
        assertTrue(actualError.getMessage().contains(expectedErrorMessage),
                String.format("Error must contain '%s': %s", expectedErrorMessage, actualError.getMessage()));
    }

    @Test
    public void testCreateSchema()
    {
        CatalogSchemaName schema = new CatalogSchemaName("some-catalog", "some-schema");
        authorizer.checkCanCreateSchema(requestingSecurityContext, schema, Map.of("some-key", "some-value"));
        authorizer.checkCanCreateSchema(requestingSecurityContext, schema, Map.of());

        List<String> expectedRequests = List.of(
                """
                        {
                            "operation": "CreateSchema",
                            "resource": {
                                "schema": {
                                    "catalogName": "some-catalog",
                                    "schemaName": "some-schema",
                                    "properties": {
                                        "some-key": "some-value"
                                    }
                                }
                            }
                        }
                        """,
                """
                        {
                            "operation": "CreateSchema",
                            "resource": {
                                "schema": {
                                    "catalogName": "some-catalog",
                                    "schemaName": "some-schema",
                                    "properties": {}
                                }
                            }
                        }
                        """);
        assertStringRequestsEqual(expectedRequests, mockClient.getRequests(), "/input/action");
    }

    @ParameterizedTest(name = "{index}: {0}")
    @MethodSource("io.trino.plugin.openpolicyagent.TestHelpers#allErrorCasesArgumentProvider")
    public void testCreateSchemaFailure(
            HttpResponse<String> failureResponse,
            Class<? extends Throwable> expectedException,
            String expectedErrorMessage)
    {
        mockClient.setHandler((request) -> failureResponse);

        Throwable actualError = assertThrows(expectedException, () -> authorizer.checkCanCreateSchema(requestingSecurityContext, new CatalogSchemaName("some-catalog", "some-schema"), Map.of()));
        assertTrue(actualError.getMessage().contains(expectedErrorMessage),
                String.format("Error must contain '%s': %s", expectedErrorMessage, actualError.getMessage()));
    }

    @Test
    public void testCanRenameSchema()
    {
        CatalogSchemaName sourceSchema = new CatalogSchemaName("some-catalog", "some-schema");
        authorizer.checkCanRenameSchema(requestingSecurityContext, sourceSchema, "new-name");

        String expectedRequest = """
                {
                    "operation": "RenameSchema",
                    "resource": {
                        "schema": {
                            "catalogName": "some-catalog",
                            "schemaName": "some-schema"
                        }
                    },
                    "targetResource": {
                        "schema": {
                            "catalogName": "some-catalog",
                            "schemaName": "new-name"
                        }
                    }
                }
                """;
        assertStringRequestsEqual(Set.of(expectedRequest), mockClient.getRequests(), "/input/action");
    }

    @ParameterizedTest(name = "{index}: {0}")
    @MethodSource("io.trino.plugin.openpolicyagent.TestHelpers#allErrorCasesArgumentProvider")
    public void testCanRenameSchemaFailure(
            HttpResponse<String> failureResponse,
            Class<? extends Throwable> expectedException,
            String expectedErrorMessage)
    {
        mockClient.setHandler((request) -> failureResponse);

        Throwable actualError = assertThrows(expectedException, () -> authorizer.checkCanRenameSchema(requestingSecurityContext, new CatalogSchemaName("some-catalog", "some-schema"), "new-name"));
        assertTrue(actualError.getMessage().contains(expectedErrorMessage),
                String.format("Error must contain '%s': %s", expectedErrorMessage, actualError.getMessage()));
    }

    private static Stream<Arguments> renameTableTestCases()
    {
        Stream<FunctionalHelpers.Consumer4<OpaAuthorizer, SystemSecurityContext, CatalogSchemaTableName, CatalogSchemaTableName>> methods = Stream.of(
                OpaAuthorizer::checkCanRenameTable,
                OpaAuthorizer::checkCanRenameView,
                OpaAuthorizer::checkCanRenameMaterializedView);
        Stream<FunctionalHelpers.Pair<String, String>> actionAndResource = Stream.of(
                FunctionalHelpers.Pair.of("RenameTable", "table"),
                FunctionalHelpers.Pair.of("RenameView", "view"),
                FunctionalHelpers.Pair.of("RenameMaterializedView", "view"));
        return Streams.zip(actionAndResource, methods, (action, method) -> Arguments.of(Named.of(action.getFirst(), action.getFirst()), action.getSecond(), method));
    }

    @ParameterizedTest(name = "{index}: {0}")
    @MethodSource("io.trino.plugin.openpolicyagent.OpaAuthorizerUnitTest#renameTableTestCases")
    public void testRenameTableActions(
            String actionName,
            String resourceName,
            FunctionalHelpers.Consumer4<OpaAuthorizer, SystemSecurityContext, CatalogSchemaTableName, CatalogSchemaTableName> method)
    {
        CatalogSchemaTableName sourceTable = new CatalogSchemaTableName("some-catalog", "some-schema", "some-table");
        CatalogSchemaTableName targetTable = new CatalogSchemaTableName("another-catalog", "another-schema", "another-table");

        method.accept(authorizer, requestingSecurityContext, sourceTable, targetTable);

        String expectedRequest = """
                {
                    "operation": "%s",
                    "resource": {
                        "%s": {
                            "catalogName": "some-catalog",
                            "schemaName": "some-schema",
                            "tableName": "some-table"
                        }
                    },
                    "targetResource": {
                        "%s": {
                            "catalogName": "another-catalog",
                            "schemaName": "another-schema",
                            "tableName": "another-table"
                        }
                    }
                }
                """.formatted(actionName, resourceName, resourceName);
        assertStringRequestsEqual(Set.of(expectedRequest), mockClient.getRequests(), "/input/action");
    }

    public static Stream<Arguments> renameTableFailureTestCases()
    {
        return createFailingTestCases(renameTableTestCases());
    }

    @ParameterizedTest(name = "{index}: {0} - {3}")
    @MethodSource("io.trino.plugin.openpolicyagent.OpaAuthorizerUnitTest#renameTableFailureTestCases")
    public void testRenameTableFailure(
            String actionName,
            String resourceName,
            FunctionalHelpers.Consumer4<OpaAuthorizer, SystemSecurityContext, CatalogSchemaTableName, CatalogSchemaTableName> method,
            HttpResponse<String> failureResponse,
            Class<? extends Throwable> expectedException,
            String expectedErrorMessage)
    {
        mockClient.setHandler((request) -> failureResponse);

        CatalogSchemaTableName sourceTable = new CatalogSchemaTableName("some-catalog", "some-schema", "some-table");
        CatalogSchemaTableName targetTable = new CatalogSchemaTableName("another-catalog", "another-schema", "another-table");
        Throwable actualError = assertThrows(expectedException, () -> method.accept(authorizer, requestingSecurityContext, sourceTable, targetTable));
        assertTrue(actualError.getMessage().contains(expectedErrorMessage),
                String.format("Error must contain '%s': %s", expectedErrorMessage, actualError.getMessage()));
    }

    @Test
    public void testCanSetSchemaAuthorization()
    {
        CatalogSchemaName schema = new CatalogSchemaName("some-catalog", "some-schema");

        authorizer.checkCanSetSchemaAuthorization(requestingSecurityContext, schema, new TrinoPrincipal(PrincipalType.USER, "some-user"));

        String expectedRequest = """
                {
                    "operation": "SetSchemaAuthorization",
                    "resource": {
                        "schema": {
                            "catalogName": "some-catalog",
                            "schemaName": "some-schema"
                        }
                    },
                    "grantee": {
                        "principals": [
                            {
                                "name": "some-user",
                                "type": "USER"
                            }
                        ]
                    }
                }
                """;
        assertStringRequestsEqual(Set.of(expectedRequest), mockClient.getRequests(), "/input/action");
    }

    @ParameterizedTest(name = "{index}: {0}")
    @MethodSource("io.trino.plugin.openpolicyagent.TestHelpers#allErrorCasesArgumentProvider")
    public void testCanSetSchemaAuthorizationFailure(
            HttpResponse<String> failureResponse,
            Class<? extends Throwable> expectedException,
            String expectedErrorMessage)
    {
        mockClient.setHandler((request) -> failureResponse);

        CatalogSchemaName schema = new CatalogSchemaName("some-catalog", "some-schema");
        Throwable actualError = assertThrows(expectedException, () -> authorizer.checkCanSetSchemaAuthorization(requestingSecurityContext, schema, new TrinoPrincipal(PrincipalType.USER, "some-user")));
        assertTrue(actualError.getMessage().contains(expectedErrorMessage),
                String.format("Error must contain '%s': %s", expectedErrorMessage, actualError.getMessage()));
    }

    private static Stream<Arguments> setTableAuthorizationTestCases()
    {
        Stream<FunctionalHelpers.Consumer4<OpaAuthorizer, SystemSecurityContext, CatalogSchemaTableName, TrinoPrincipal>> methods = Stream.of(
                OpaAuthorizer::checkCanSetTableAuthorization,
                OpaAuthorizer::checkCanSetViewAuthorization);
        Stream<FunctionalHelpers.Pair<String, String>> actionAndResource = Stream.of(
                FunctionalHelpers.Pair.of("SetTableAuthorization", "table"),
                FunctionalHelpers.Pair.of("SetViewAuthorization", "view"));
        return Streams.zip(actionAndResource, methods, (action, method) -> Arguments.of(Named.of(action.getFirst(), action.getFirst()), action.getSecond(), method));
    }

    @ParameterizedTest(name = "{index}: {0}")
    @MethodSource("io.trino.plugin.openpolicyagent.OpaAuthorizerUnitTest#setTableAuthorizationTestCases")
    public void testCanSetTableAuthorization(
            String actionName,
            String resourceName,
            FunctionalHelpers.Consumer4<OpaAuthorizer, SystemSecurityContext, CatalogSchemaTableName, TrinoPrincipal> method)
    {
        CatalogSchemaTableName table = new CatalogSchemaTableName("some-catalog", "some-schema", "some-table");

        method.accept(authorizer, requestingSecurityContext, table, new TrinoPrincipal(PrincipalType.USER, "some-user"));

        String expectedRequest = """
                {
                    "operation": "%s",
                    "resource": {
                        "%s": {
                            "catalogName": "some-catalog",
                            "schemaName": "some-schema",
                            "tableName": "some-table"
                        }
                    },
                    "grantee": {
                        "principals": [
                            {
                                "name": "some-user",
                                "type": "USER"
                            }
                        ]
                    }
                }
                """.formatted(actionName, resourceName);
        assertStringRequestsEqual(Set.of(expectedRequest), mockClient.getRequests(), "/input/action");
    }

    private static Stream<Arguments> setTableAuthorizationFailureTestCases()
    {
        return createFailingTestCases(setTableAuthorizationTestCases());
    }

    @ParameterizedTest(name = "{index}: {0} - {3}")
    @MethodSource("io.trino.plugin.openpolicyagent.OpaAuthorizerUnitTest#setTableAuthorizationFailureTestCases")
    public void testCanSetTableAuthorizationFailure(
            String actionName,
            String resourceName,
            FunctionalHelpers.Consumer4<OpaAuthorizer, SystemSecurityContext, CatalogSchemaTableName, TrinoPrincipal> method,
            HttpResponse<String> failureResponse,
            Class<? extends Throwable> expectedException,
            String expectedErrorMessage)
    {
        mockClient.setHandler((request) -> failureResponse);

        CatalogSchemaTableName table = new CatalogSchemaTableName("some-catalog", "some-schema", "some-table");

        Throwable actualError = assertThrows(
                expectedException,
                () -> method.accept(authorizer, requestingSecurityContext, table, new TrinoPrincipal(PrincipalType.USER, "some-user")));
        assertTrue(actualError.getMessage().contains(expectedErrorMessage),
                String.format("Error must contain '%s': %s", expectedErrorMessage, actualError.getMessage()));
    }

    private static Stream<Arguments> tableColumnOperationTestCases()
    {
        Stream<FunctionalHelpers.Consumer4<OpaAuthorizer, SystemSecurityContext, CatalogSchemaTableName, Set<String>>> methods = Stream.of(
                OpaAuthorizer::checkCanSelectFromColumns,
                OpaAuthorizer::checkCanUpdateTableColumns,
                OpaAuthorizer::checkCanCreateViewWithSelectFromColumns);
        Stream<String> actionAndResource = Stream.of(
                "SelectFromColumns",
                "UpdateTableColumns",
                "CreateViewWithSelectFromColumns");
        return Streams.zip(actionAndResource, methods, (action, method) -> Arguments.of(Named.of(action, action), method));
    }

    @ParameterizedTest(name = "{index}: {0}")
    @MethodSource("io.trino.plugin.openpolicyagent.OpaAuthorizerUnitTest#tableColumnOperationTestCases")
    public void testTableColumnOperations(
            String actionName,
            FunctionalHelpers.Consumer4<OpaAuthorizer, SystemSecurityContext, CatalogSchemaTableName, Set<String>> method)
    {
        CatalogSchemaTableName table = new CatalogSchemaTableName("some-catalog", "some-schema", "some-table");
        Set<String> columns = Set.of("some-column");

        method.accept(authorizer, requestingSecurityContext, table, columns);

        String expectedRequest = """
                {
                    "operation": "%s",
                    "resource": {
                        "table": {
                            "catalogName": "some-catalog",
                            "schemaName": "some-schema",
                            "tableName": "some-table",
                            "columns": ["some-column"]
                        }
                    }
                }
                """.formatted(actionName);
        assertStringRequestsEqual(Set.of(expectedRequest), mockClient.getRequests(), "/input/action");
    }

    private static Stream<Arguments> tableColumnOperationFailureTestCases()
    {
        return createFailingTestCases(tableColumnOperationTestCases());
    }

    @ParameterizedTest(name = "{index}: {0} - {2}")
    @MethodSource("io.trino.plugin.openpolicyagent.OpaAuthorizerUnitTest#tableColumnOperationFailureTestCases")
    public void testTableColumnOperationsFailure(
            String actionName,
            FunctionalHelpers.Consumer4<OpaAuthorizer, SystemSecurityContext, CatalogSchemaTableName, Set<String>> method,
            HttpResponse<String> failureResponse,
            Class<? extends Throwable> expectedException,
            String expectedErrorMessage)
    {
        mockClient.setHandler((request) -> failureResponse);
        CatalogSchemaTableName table = new CatalogSchemaTableName("some-catalog", "some-schema", "some-table");
        Set<String> columns = Set.of("some-column");

        Throwable actualError = assertThrows(
                expectedException,
                () -> method.accept(authorizer, requestingSecurityContext, table, columns));
        assertTrue(
                actualError.getMessage().contains(expectedErrorMessage),
                String.format("Error must contain '%s': %s", expectedErrorMessage, actualError.getMessage()));
    }

    @Test
    public void testCanGrantExecuteFunctionPrivilege()
    {
        authorizer.checkCanGrantExecuteFunctionPrivilege(requestingSecurityContext, "some-function", new TrinoPrincipal(PrincipalType.USER, "some-user"), true);

        String expectedRequest = """
                {
                    "operation": "GrantExecuteFunctionPrivilege",
                    "resource": {
                        "function": {
                            "name": "some-function"
                        }
                    },
                    "grantee": {
                        "principals": [
                            {
                                "name": "some-user",
                                "type": "USER"
                            }
                        ],
                        "grantOption": true
                    }
                }
                """;
        assertStringRequestsEqual(Set.of(expectedRequest), mockClient.getRequests(), "/input/action");
    }

    @ParameterizedTest(name = "{index}: {0}")
    @MethodSource("io.trino.plugin.openpolicyagent.TestHelpers#allErrorCasesArgumentProvider")
    public void testCanGrantExecuteFunctionPrivilegeFailure(
            HttpResponse<String> failureResponse,
            Class<? extends Throwable> expectedException,
            String expectedErrorMessage)
    {
        mockClient.setHandler((request) -> failureResponse);

        Throwable actualError = assertThrows(
                expectedException,
                () -> authorizer.checkCanGrantExecuteFunctionPrivilege(requestingSecurityContext, "some-function", new TrinoPrincipal(PrincipalType.USER, "some-name"), true));
        assertTrue(
                actualError.getMessage().contains(expectedErrorMessage),
                String.format("Error must contain '%s': %s", expectedErrorMessage, actualError.getMessage()));
    }

    @Test
    public void testCanSetCatalogSessionProperty()
    {
        authorizer.checkCanSetCatalogSessionProperty(requestingSecurityContext, "some-catalog", "some-property");

        String expectedRequest = """
                {
                    "operation": "SetCatalogSessionProperty",
                    "resource": {
                        "catalog": {
                            "name": "some-catalog"
                        },
                        "catalogSessionProperty": {
                            "name": "some-property"
                        }
                    }
                }
                """;
        assertStringRequestsEqual(Set.of(expectedRequest), mockClient.getRequests(), "/input/action");
    }

    @ParameterizedTest(name = "{index}: {0}")
    @MethodSource("io.trino.plugin.openpolicyagent.TestHelpers#allErrorCasesArgumentProvider")
    public void testCanSetCatalogSessionPropertyFailure(
            HttpResponse<String> failureResponse,
            Class<? extends Throwable> expectedException,
            String expectedErrorMessage)
    {
        mockClient.setHandler((request) -> failureResponse);

        Throwable actualError = assertThrows(
                expectedException,
                () -> authorizer.checkCanSetCatalogSessionProperty(requestingSecurityContext, "some-catalog", "some-property"));
        assertTrue(
                actualError.getMessage().contains(expectedErrorMessage),
                String.format("Error must contain '%s': %s", expectedErrorMessage, actualError.getMessage()));
    }

    private static Stream<Arguments> schemaPrivilegeTestCases()
    {
        Stream<FunctionalHelpers.Consumer5<OpaAuthorizer, SystemSecurityContext, Privilege, CatalogSchemaName, TrinoPrincipal>> methods = Stream.of(
                OpaAuthorizer::checkCanDenySchemaPrivilege,
                (authorizer, context, privilege, catalog, principal) -> authorizer.checkCanGrantSchemaPrivilege(context, privilege, catalog, principal, true),
                (authorizer, context, privilege, catalog, principal) -> authorizer.checkCanRevokeSchemaPrivilege(context, privilege, catalog, principal, true));
        Stream<String> actions = Stream.of(
                "DenySchemaPrivilege",
                "GrantSchemaPrivilege",
                "RevokeSchemaPrivilege");
        return Streams.zip(actions, methods, (action, method) -> Arguments.of(Named.of(action, action), method));
    }

    @ParameterizedTest(name = "{index}: {0}")
    @MethodSource("io.trino.plugin.openpolicyagent.OpaAuthorizerUnitTest#schemaPrivilegeTestCases")
    public void testSchemaPrivileges(
            String actionName,
            FunctionalHelpers.Consumer5<OpaAuthorizer, SystemSecurityContext, Privilege, CatalogSchemaName, TrinoPrincipal> method)
            throws IOException
    {
        Privilege privilege = Privilege.CREATE;
        method.accept(authorizer, requestingSecurityContext, privilege, new CatalogSchemaName("some-catalog", "some-schema"), new TrinoPrincipal(PrincipalType.USER, "some-user"));

        String expectedRequest = """
                {
                    "operation": "%s",
                    "resource": {
                        "schema": {
                            "catalogName": "some-catalog",
                            "schemaName": "some-schema"
                        }
                    },
                    "grantee": {
                        "principals": [
                            {
                                "name": "some-user",
                                "type": "USER"
                            }
                        ],
                        "privilege": "CREATE",
                        "grantOption": true
                    }
                }
                """.formatted(actionName);
        List<String> actualRequests = mockClient.getRequests();
        assertEquals(actualRequests.size(), 1, "Unexpected number of requests");

        JsonNode actualRequestInput = jsonMapper.readTree(mockClient.getRequests().get(0)).at("/input/action");
        if (!actualRequestInput.at("/grantee").has("grantOption")) {
            // The DenySchemaPrivilege request does not have a grant option, we'll default it to true so we can use the same test
            ((ObjectNode) actualRequestInput.at("/grantee")).put("grantOption", true);
        }
        assertEquals(jsonMapper.readTree(expectedRequest), actualRequestInput);
    }

    private static Stream<Arguments> schemaPrivilegeFailureTestCases()
    {
        return createFailingTestCases(schemaPrivilegeTestCases());
    }

    @ParameterizedTest(name = "{index}: {0} - {2}")
    @MethodSource("io.trino.plugin.openpolicyagent.OpaAuthorizerUnitTest#schemaPrivilegeFailureTestCases")
    public void testSchemaPrivilegesFailure(
            String actionName,
            FunctionalHelpers.Consumer5<OpaAuthorizer, SystemSecurityContext, Privilege, CatalogSchemaName, TrinoPrincipal> method,
            HttpResponse<String> failureResponse,
            Class<? extends Throwable> expectedException,
            String expectedErrorMessage)
    {
        mockClient.setHandler((request) -> failureResponse);

        Privilege privilege = Privilege.CREATE;
        Throwable actualError = assertThrows(
                expectedException,
                () -> method.accept(authorizer, requestingSecurityContext, privilege, new CatalogSchemaName("some-catalog", "some-schema"), new TrinoPrincipal(PrincipalType.USER, "some-user")));
        assertTrue(
                actualError.getMessage().contains(expectedErrorMessage),
                String.format("Error must contain '%s': %s", expectedErrorMessage, actualError.getMessage()));
    }

    private static Stream<Arguments> tablePrivilegeTestCases()
    {
        Stream<FunctionalHelpers.Consumer5<OpaAuthorizer, SystemSecurityContext, Privilege, CatalogSchemaTableName, TrinoPrincipal>> methods = Stream.of(
                OpaAuthorizer::checkCanDenyTablePrivilege,
                (authorizer, context, privilege, catalog, principal) -> authorizer.checkCanGrantTablePrivilege(context, privilege, catalog, principal, true),
                (authorizer, context, privilege, catalog, principal) -> authorizer.checkCanRevokeTablePrivilege(context, privilege, catalog, principal, true));
        Stream<String> actions = Stream.of(
                "DenyTablePrivilege",
                "GrantTablePrivilege",
                "RevokeTablePrivilege");
        return Streams.zip(actions, methods, (action, method) -> Arguments.of(Named.of(action, action), method));
    }

    @ParameterizedTest(name = "{index}: {0}")
    @MethodSource("io.trino.plugin.openpolicyagent.OpaAuthorizerUnitTest#tablePrivilegeTestCases")
    public void testTablePrivileges(
            String actionName,
            FunctionalHelpers.Consumer5<OpaAuthorizer, SystemSecurityContext, Privilege, CatalogSchemaTableName, TrinoPrincipal> method)
            throws IOException
    {
        Privilege privilege = Privilege.CREATE;
        method.accept(authorizer, requestingSecurityContext, privilege, new CatalogSchemaTableName("some-catalog", "some-schema", "some-table"), new TrinoPrincipal(PrincipalType.USER, "some-user"));

        String expectedRequest = """
                {
                    "operation": "%s",
                    "resource": {
                        "table": {
                            "catalogName": "some-catalog",
                            "schemaName": "some-schema",
                            "tableName": "some-table"
                        }
                    },
                    "grantee": {
                        "principals": [
                            {
                                "name": "some-user",
                                "type": "USER"
                            }
                        ],
                        "privilege": "CREATE",
                        "grantOption": true
                    }
                }
                """.formatted(actionName);
        List<String> actualRequests = mockClient.getRequests();
        assertEquals(actualRequests.size(), 1, "Unexpected number of requests");

        JsonNode actualRequestInput = jsonMapper.readTree(mockClient.getRequests().get(0)).at("/input/action");
        if (!actualRequestInput.at("/grantee").has("grantOption")) {
            // The DenySchemaPrivilege request does not have a grant option, we'll default it to true so we can use the same test
            ((ObjectNode) actualRequestInput.at("/grantee")).put("grantOption", true);
        }
        assertEquals(jsonMapper.readTree(expectedRequest), actualRequestInput);
    }

    private static Stream<Arguments> tablePrivilegeFailureTestCases()
    {
        return createFailingTestCases(tablePrivilegeTestCases());
    }

    @ParameterizedTest(name = "{index}: {0} - {2}")
    @MethodSource("io.trino.plugin.openpolicyagent.OpaAuthorizerUnitTest#tablePrivilegeFailureTestCases")
    public void testTablePrivilegesFailure(
            String actionName,
            FunctionalHelpers.Consumer5<OpaAuthorizer, SystemSecurityContext, Privilege, CatalogSchemaTableName, TrinoPrincipal> method,
            HttpResponse<String> failureResponse,
            Class<? extends Throwable> expectedException,
            String expectedErrorMessage)
    {
        mockClient.setHandler((request) -> failureResponse);

        Privilege privilege = Privilege.CREATE;
        Throwable actualError = assertThrows(
                expectedException,
                () -> method.accept(authorizer, requestingSecurityContext, privilege, new CatalogSchemaTableName("some-catalog", "some-schema", "some-table"), new TrinoPrincipal(PrincipalType.USER, "some-user")));
        assertTrue(
                actualError.getMessage().contains(expectedErrorMessage),
                String.format("Error must contain '%s': %s", expectedErrorMessage, actualError.getMessage()));
    }

    @Test
    public void testCanCreateRole()
    {
        authorizer.checkCanCreateRole(requestingSecurityContext, "some-role-without-grantor", Optional.empty());
        TrinoPrincipal grantor = new TrinoPrincipal(PrincipalType.USER, "some-grantor");
        authorizer.checkCanCreateRole(requestingSecurityContext, "some-role-with-grantor", Optional.of(grantor));

        Set<String> expectedRequests = Set.of(
                """
                        {
                            "operation": "CreateRole",
                            "resource": {
                                "role": {
                                    "name": "some-role-without-grantor"
                                }
                            }
                        }""",
                """
                        {
                            "operation": "CreateRole",
                            "resource": {
                                "role": {
                                    "name": "some-role-with-grantor"
                                }
                            },
                            "grantor": {
                                "name": "some-grantor",
                                "type": "USER"
                            }
                        }
                        """);
        assertStringRequestsEqual(expectedRequests, mockClient.getRequests(), "/input/action");
    }

    @ParameterizedTest(name = "{index}: {0}")
    @MethodSource("io.trino.plugin.openpolicyagent.TestHelpers#allErrorCasesArgumentProvider")
    public void testCanCreateRoleFailure(
            HttpResponse<String> failureResponse,
            Class<? extends Throwable> expectedException,
            String expectedErrorMessage)
    {
        mockClient.setHandler((request) -> failureResponse);

        Throwable actualError = assertThrows(
                expectedException,
                () -> authorizer.checkCanCreateRole(requestingSecurityContext, "some-role-without-grantor", Optional.empty()));
        assertTrue(
                actualError.getMessage().contains(expectedErrorMessage),
                String.format("Error must contain '%s': %s", expectedErrorMessage, actualError.getMessage()));
    }

    private static Stream<Arguments> roleGrantingTestCases()
    {
        Stream<FunctionalHelpers.Consumer6<OpaAuthorizer, SystemSecurityContext, Set<String>, Set<TrinoPrincipal>, Boolean, Optional<TrinoPrincipal>>> methods = Stream.of(
                OpaAuthorizer::checkCanGrantRoles,
                OpaAuthorizer::checkCanRevokeRoles);
        Stream<String> actions = Stream.of(
                "GrantRoles",
                "RevokeRoles");
        return Streams.zip(actions, methods, (action, method) -> Arguments.of(Named.of(action, action), method));
    }

    @ParameterizedTest(name = "{index}: {0}")
    @MethodSource("io.trino.plugin.openpolicyagent.OpaAuthorizerUnitTest#roleGrantingTestCases")
    public void testRoleGranting(
            String actionName,
            FunctionalHelpers.Consumer6<OpaAuthorizer, SystemSecurityContext, Set<String>, Set<TrinoPrincipal>, Boolean, Optional<TrinoPrincipal>> method)
            throws IOException
    {
        TrinoPrincipal grantee = new TrinoPrincipal(PrincipalType.ROLE, "some-grantee-role");
        method.accept(authorizer, requestingSecurityContext, Set.of("some-role-without-grantor"), Set.of(grantee), true, Optional.empty());

        TrinoPrincipal grantor = new TrinoPrincipal(PrincipalType.USER, "some-grantor-user");
        method.accept(authorizer, requestingSecurityContext, Set.of("some-role-with-grantor"), Set.of(grantee), false, Optional.of(grantor));

        Set<String> expectedRequests = Set.of(
                """
                        {
                            "operation": "%s",
                            "resource": {
                                "roles": [
                                    {
                                        "name": "some-role-with-grantor"
                                    }
                                ]
                            },
                            "grantor": {
                                "name": "some-grantor-user",
                                "type": "USER"
                            },
                            "grantee": {
                                "principals": [
                                    {
                                        "name": "some-grantee-role",
                                        "type": "ROLE"
                                    }
                                ],
                                "grantOption": false
                            }
                        }""".formatted(actionName),
                """
                        {
                            "operation": "%s",
                            "resource": {
                                "roles": [
                                    {
                                        "name": "some-role-without-grantor"
                                    }
                                ]
                            },
                            "grantee": {
                                "principals": [
                                    {
                                        "name": "some-grantee-role",
                                        "type": "ROLE"
                                    }
                                ],
                                "grantOption": true
                            }
                        }""".formatted(actionName));
        assertStringRequestsEqual(expectedRequests, mockClient.getRequests(), "/input/action");
    }

    private static Stream<Arguments> roleGrantingFailureTestCases()
    {
        return createFailingTestCases(roleGrantingTestCases());
    }

    @ParameterizedTest(name = "{index}: {0} - {2}")
    @MethodSource("io.trino.plugin.openpolicyagent.OpaAuthorizerUnitTest#roleGrantingFailureTestCases")
    public void testRoleGrantingFailure(
            String actionName,
            FunctionalHelpers.Consumer6<OpaAuthorizer, SystemSecurityContext, Set<String>, Set<TrinoPrincipal>, Boolean, Optional<TrinoPrincipal>> method,
            HttpResponse<String> failureResponse,
            Class<? extends Throwable> expectedException,
            String expectedErrorMessage)
    {
        mockClient.setHandler((request) -> failureResponse);

        TrinoPrincipal grantee = new TrinoPrincipal(PrincipalType.ROLE, "some-grantee-role");
        Throwable actualError = assertThrows(
                expectedException,
                () -> method.accept(authorizer, requestingSecurityContext, Set.of("some-role-without-grantor"), Set.of(grantee), true, Optional.empty()));
        assertTrue(
                actualError.getMessage().contains(expectedErrorMessage),
                String.format("Error must contain '%s': %s", expectedErrorMessage, actualError.getMessage()));
    }

    @Test
    public void testCanExecuteProcedure()
    {
        CatalogSchemaRoutineName routine = new CatalogSchemaRoutineName("some-catalog", "some-schema", "some-routine-name");
        authorizer.checkCanExecuteProcedure(requestingSecurityContext, routine);

        String expectedRequest = """
                {
                    "operation": "ExecuteProcedure",
                    "resource": {
                        "schema": {
                            "schemaName": "some-schema",
                            "catalogName": "some-catalog"
                        },
                        "function": {
                            "name": "some-routine-name"
                        }
                    }
                }""";
        assertStringRequestsEqual(Set.of(expectedRequest), mockClient.getRequests(), "/input/action");
    }

    @ParameterizedTest(name = "{index}: {0}")
    @MethodSource("io.trino.plugin.openpolicyagent.TestHelpers#allErrorCasesArgumentProvider")
    public void testCanExecuteProcedureFailure(
            HttpResponse<String> failureResponse,
            Class<? extends Throwable> expectedException,
            String expectedErrorMessage)
    {
        mockClient.setHandler((request) -> failureResponse);

        CatalogSchemaRoutineName routine = new CatalogSchemaRoutineName("some-catalog", "some-schema", "some-routine-name");
        Throwable actualError = assertThrows(
                expectedException,
                () -> authorizer.checkCanExecuteProcedure(requestingSecurityContext, routine));
        assertTrue(
                actualError.getMessage().contains(expectedErrorMessage),
                String.format("Error must contain '%s': %s", expectedErrorMessage, actualError.getMessage()));
    }

    @Test
    public void testCanExecuteTableProcedure()
    {
        CatalogSchemaTableName table = new CatalogSchemaTableName("some-catalog", "some-schema", "some-table");
        authorizer.checkCanExecuteTableProcedure(requestingSecurityContext, table, "some-procedure");

        String expectedRequest = """
                {
                    "operation": "ExecuteTableProcedure",
                    "resource": {
                        "table": {
                            "schemaName": "some-schema",
                            "catalogName": "some-catalog",
                            "tableName": "some-table"
                        },
                        "function": {
                            "name": "some-procedure"
                        }
                    }
                }""";
        assertStringRequestsEqual(Set.of(expectedRequest), mockClient.getRequests(), "/input/action");
    }

    @ParameterizedTest(name = "{index}: {0}")
    @MethodSource("io.trino.plugin.openpolicyagent.TestHelpers#allErrorCasesArgumentProvider")
    public void testCanExecuteTableProcedureFailure(
            HttpResponse<String> failureResponse,
            Class<? extends Throwable> expectedException,
            String expectedErrorMessage)
    {
        mockClient.setHandler((request) -> failureResponse);

        CatalogSchemaTableName table = new CatalogSchemaTableName("some-catalog", "some-schema", "some-table");
        Throwable actualError = assertThrows(
                expectedException,
                () -> authorizer.checkCanExecuteTableProcedure(requestingSecurityContext, table, "some-procedure"));
        assertTrue(
                actualError.getMessage().contains(expectedErrorMessage),
                String.format("Error must contain '%s': %s", expectedErrorMessage, actualError.getMessage()));
    }

    @Test
    public void testCanExecuteFunctionWithFunctionKind()
    {
        CatalogSchemaRoutineName routine = new CatalogSchemaRoutineName("some-catalog", "some-schema", "some-routine");
        authorizer.checkCanExecuteFunction(requestingSecurityContext, FunctionKind.AGGREGATE, routine);

        String expectedRequest = """
                {
                    "operation": "ExecuteFunction",
                    "resource": {
                        "schema": {
                            "schemaName": "some-schema",
                            "catalogName": "some-catalog"
                        },
                        "function": {
                            "name": "some-routine",
                            "functionKind": "AGGREGATE"
                        }
                    }
                }""";
        assertStringRequestsEqual(Set.of(expectedRequest), mockClient.getRequests(), "/input/action");
    }

    @ParameterizedTest(name = "{index}: {0}")
    @MethodSource("io.trino.plugin.openpolicyagent.TestHelpers#allErrorCasesArgumentProvider")
    public void testCanExecuteFunctionWithFunctionKindFailure(
            HttpResponse<String> failureResponse,
            Class<? extends Throwable> expectedException,
            String expectedErrorMessage)
    {
        mockClient.setHandler((request) -> failureResponse);

        CatalogSchemaRoutineName routine = new CatalogSchemaRoutineName("some-catalog", "some-schema", "some-routine");
        Throwable actualError = assertThrows(
                expectedException,
                () -> authorizer.checkCanExecuteFunction(requestingSecurityContext, FunctionKind.AGGREGATE, routine));
        assertTrue(
                actualError.getMessage().contains(expectedErrorMessage),
                String.format("Error must contain '%s': %s", expectedErrorMessage, actualError.getMessage()));
    }
}
