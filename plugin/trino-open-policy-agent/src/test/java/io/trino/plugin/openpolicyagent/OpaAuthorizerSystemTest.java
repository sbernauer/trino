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

import com.google.common.io.Resources;
import io.trino.Session;
import io.trino.execution.QueryIdGenerator;
import io.trino.metadata.SessionPropertyManager;
import io.trino.plugin.memory.MemoryPlugin;
import io.trino.plugin.tpch.TpchPlugin;
import io.trino.server.testing.TestingTrinoServer;
import io.trino.spi.security.Identity;
import io.trino.testing.MaterializedResult;
import io.trino.testing.MaterializedRow;
import io.trino.testing.TestingTrinoClient;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.SocketTimeoutException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class OpaAuthorizerSystemTest
{
    private static URI opaServerUri;
    private static Process opaServer;
    private static TestingTrinoServer trinoServer;
    private static TestingTrinoClient trinoClient;

    /**
     * Get an unused TCP port on a local interface from the system
     * <p>
     * There is a minor race condition here, in that the port is deallocated before it is used
     * again, but this is more or less unavoidable when allocating a port for a subprocess without
     * FD-passing.
     */
    private static InetSocketAddress findAvailableTcpPort()
            throws IOException
    {
        Socket sock = new Socket();
        try {
            sock.bind(new InetSocketAddress("127.0.0.1", 0));
            return new InetSocketAddress(sock.getLocalAddress(), sock.getLocalPort());
        }
        finally {
            sock.close();
        }
    }

    private static void awaitSocketOpen(InetSocketAddress addr, int attempts, int timeoutMs)
            throws IOException, InterruptedException
    {
        for (int i = 0; i < attempts; ++i) {
            Socket socket = new Socket();
            try {
                socket.connect(addr, timeoutMs);
                return;
            }
            catch (SocketTimeoutException e) {
                // e.printStackTrace();
            }
            catch (IOException e) {
                // e.printStackTrace();
                Thread.sleep(timeoutMs);
            }
            finally {
                socket.close();
            }
        }
        throw new SocketTimeoutException("Timed out waiting for addr " + addr + " to be available ("
                + attempts + " attempts made at " + timeoutMs + "ms each)");
    }

    private static TestingTrinoServer getTrinoServer(boolean batched)
    {
        if (batched) {
            return TestingTrinoServer.builder()
                    .setSystemAccessControls(Collections.singletonList(new OpaBatchAuthorizer(new OpaConfig()
                            .setOpaUri(opaServerUri.resolve("v1/data/trino/allow"))
                            .setOpaBatchUri(opaServerUri.resolve("v1/data/trino/extended")))))
                    .build();
        }
        else {
            return TestingTrinoServer.builder()
                    .setSystemAccessControls(Collections.singletonList(new OpaAuthorizer(new OpaConfig()
                            .setOpaUri(opaServerUri.resolve("v1/data/trino/allow")))))
                    .build();
        }
    }

    private static TestingTrinoClient getTrinoClient(String userName)
    {
        QueryIdGenerator idGen = new QueryIdGenerator();
        Identity identity = Identity.forUser(userName).build();
        SessionPropertyManager sessionPropertyManager = new SessionPropertyManager();
        Session session = Session.builder(sessionPropertyManager)
                .setQueryId(idGen.createNextQueryId()).setIdentity(identity).build();
        return new TestingTrinoClient(trinoServer, session);
    }

    @BeforeAll
    public static void setupOpa()
            throws IOException, InterruptedException
    {
        InetSocketAddress opaSocket = findAvailableTcpPort();
        opaServer = new ProcessBuilder(System.getenv().getOrDefault("OPA_BINARY", "opa"), "run", "--server", "--addr",
                opaSocket.getHostString() + ":" + opaSocket.getPort()).inheritIO().start();
        awaitSocketOpen(opaSocket, 100, 200);
        opaServerUri =
                URI.create("http://" + opaSocket.getHostString() + ":" + opaSocket.getPort() + "/");
    }

    @AfterAll
    public static void teardown()
            throws IOException
    {
        try {
            if (opaServer != null) {
                opaServer.destroy();
            }
        }
        finally {
            try {
                if (trinoClient != null) {
                    trinoClient.close();
                }
            }
            finally {
                if (trinoServer != null) {
                    trinoServer.close();
                }
            }
        }
    }

    private String stringOfLines(String... lines)
    {
        StringBuilder out = new StringBuilder();
        for (String line : lines) {
            out.append(line);
            out.append("\r\n");
        }
        return out.toString();
    }

    private void submitPolicy(String... policyLines)
            throws IOException, InterruptedException
    {
        HttpClient httpClient = HttpClient.newHttpClient();
        HttpResponse<String> policyRes =
                httpClient.send(
                        HttpRequest.newBuilder(opaServerUri.resolve("v1/policies/trino"))
                                .PUT(HttpRequest.BodyPublishers
                                        .ofString(stringOfLines(policyLines)))
                                .header("Content-Type", "text/plain").build(),
                        HttpResponse.BodyHandlers.ofString());
        assertEquals(policyRes.statusCode(), 200, "Failed to submit policy: " + policyRes.body());
    }

    @Nested
    @DisplayName("Unbatched Authorizer Tests")
    class UnbatchedAuthorizerTests
    {
        @BeforeAll
        public static void setupTrino()
        {
            trinoServer = getTrinoServer(false);
            trinoClient = getTrinoClient("bob");
        }

        @Test
        public void testShouldAllowQueryIfDirected()
                throws IOException, InterruptedException
        {
            submitPolicy(
                    """
                            package trino
                            import future.keywords.in
                            default allow = false
                            allow {
                              is_bob
                              can_be_accessed_by_bob
                            }
                            is_bob() {
                              input.context.identity.user == "bob"
                            }
                            can_be_accessed_by_bob() {
                              input.action.operation in ["ImpersonateUser", "FilterCatalogs", "AccessCatalog", "ExecuteQuery"]
                            }""");
            List<String> catalogs = new ArrayList<>();
            MaterializedResult result =
                    trinoClient.execute("SHOW CATALOGS").getResult();
            for (MaterializedRow row : result) {
                catalogs.add(row.getField(0).toString());
            }
            assertEquals(Collections.singletonList("system"), catalogs);
        }

        @Test
        public void testShouldDenyQueryIfDirected()
                throws IOException, InterruptedException
        {
            submitPolicy(
                    """
                            package trino
                            import future.keywords.in
                            default allow = false
                            allow {
                              is_bob
                              can_be_accessed_by_bob
                            }
                            is_bob() {
                              input.context.identity.user == "bob"
                            }
                            can_be_accessed_by_bob() {
                              input.action.operation in ["ImpersonateUser", "FilterCatalogs", "AccessCatalog", "ExecuteQuery"]
                            }""");
            RuntimeException error = assertThrows(RuntimeException.class, () -> {
                trinoClient.execute("SHOW SCHEMAS IN system");
            });
            assertTrue(error.getMessage().contains("Access Denied"),
                    "Error must mention 'Access Denied': " + error.getMessage());
        }
    }

    @Nested
    @DisplayName("Batched Authorizer Tests")
    class BatchedAuthorizerTests
    {
        @BeforeAll
        public static void setupTrino()
        {
            trinoServer = getTrinoServer(true);
            trinoClient = getTrinoClient("bob");
        }

        @Test
        public void testFilterOutItems()
                throws IOException, InterruptedException
        {
            submitPolicy(
                    """
                            package trino
                            import future.keywords.in
                            default allow = false

                            allow {
                                input.action.operation in ["AccessCatalog", "ExecuteQuery", "ImpersonateUser", "ShowSchemas", "SelectFromColumns"]
                            }

                            is_bob() {
                                input.context.identity.user == "bob"
                            }

                            extended[i] {
                                some i
                                input.action.operation == "FilterSchemas"
                                input.action.filterResources[i].schema.schemaName in ["jdbc", "metadata"]
                            }

                            extended[i] {
                                some i
                                input.action.operation == "FilterCatalogs"
                                input.action.filterResources[i]
                            }""");
            Set<String> schemas = new HashSet<>();
            trinoClient.execute("SHOW SCHEMAS FROM system").getResult()
                    .iterator()
                    .forEachRemaining((i) -> schemas.add(i.getField(0).toString()));
            assertEquals(Set.of("jdbc", "metadata"), schemas);
        }

        @Test
        public void testDenyUnbatchedQuery()
                throws IOException, InterruptedException
        {
            submitPolicy(
                    """
                            package trino
                            import future.keywords.in
                            default allow = false""");
            RuntimeException error = assertThrows(RuntimeException.class, () -> {
                trinoClient.execute("SELECT version()");
            });
            assertTrue(error.getMessage().contains("Access Denied"),
                    "Error must mention 'Access Denied': " + error.getMessage());
        }

        @Test
        public void testAllowUnbatchedQuery()
                throws IOException, InterruptedException
        {
            submitPolicy(
                    """
                            package trino
                            import future.keywords.in
                            default allow = false
                            allow {
                                input.action.operation in ["ImpersonateUser", "ExecuteFunction", "AccessCatalog", "ExecuteQuery"]
                            }""");
            Set<String> version = new HashSet<>();
            trinoClient.execute("SELECT version()").getResult()
                    .iterator()
                    .forEachRemaining((i) -> version.add(i.getField(0).toString()));
            assertFalse(version.isEmpty());
        }
    }

    @Nested
    @DisplayName("Authorizer Tests testing complex rego rules")
    class ComplexAuthorizerTests
    {
        @BeforeAll
        public static void setupTrino()
        {
            trinoServer = getTrinoServer(true);
            trinoServer.installPlugin(new MemoryPlugin());
            trinoServer.installPlugin(new TpchPlugin());
            trinoServer.createCatalog("tpch", "tpch");
            trinoServer.createCatalog("lakehouse", "memory");
            trinoServer.createCatalog("catalog_without_schemas_acls", "memory");
        }

        @Test
        public void testComplex()
                throws IOException, InterruptedException
        {
            TestingTrinoClient trinoAdminClient = getTrinoClient("admin");
            TestingTrinoClient trinoSupersetClient = getTrinoClient("superset");
            TestingTrinoClient trinoDataAnalyst1Client = getTrinoClient("data-analyst-1");
            TestingTrinoClient trinoDataAnalyst2Client = getTrinoClient("data-analyst-2");
            TestingTrinoClient trinoCustomer1User1Client = getTrinoClient("customer-1-user-1");
            TestingTrinoClient trinoCustomer2User1Client = getTrinoClient("customer-2-user-1");

            String policy = Resources.toString(Resources.getResource("trino.rego"), UTF_8);
            submitPolicy(policy);

            // Setup schema structure for customers
            trinoDataAnalyst1Client.execute("CREATE SCHEMA lakehouse.customer_1");
            trinoDataAnalyst1Client.execute("CREATE SCHEMA lakehouse.customer_2");

            assertQueryReturns(trinoAdminClient, "SHOW CATALOGS", "catalog_without_schemas_acls", "lakehouse", "system", "tpch");
            assertQueryReturns(trinoSupersetClient, "SHOW CATALOGS", "system");
            assertQueryReturns(trinoDataAnalyst1Client, "SHOW CATALOGS", "catalog_without_schemas_acls", "lakehouse", "system", "tpch");
            assertQueryReturns(trinoDataAnalyst2Client, "SHOW CATALOGS", "catalog_without_schemas_acls", "lakehouse", "system", "tpch");
            assertQueryReturns(trinoCustomer1User1Client, "SHOW CATALOGS", "lakehouse", "system");
            assertQueryReturns(trinoCustomer2User1Client, "SHOW CATALOGS", "lakehouse", "system");

            assertQueryReturns(trinoAdminClient, "SELECT * FROM system.jdbc.catalogs", "catalog_without_schemas_acls", "lakehouse", "system", "tpch");
            assertQueryReturns(trinoSupersetClient, "SELECT * FROM system.jdbc.catalogs", "system");
            assertQueryReturns(trinoDataAnalyst1Client, "SELECT * FROM system.jdbc.catalogs", "catalog_without_schemas_acls", "lakehouse", "system", "tpch");
            assertQueryReturns(trinoDataAnalyst2Client, "SELECT * FROM system.jdbc.catalogs", "catalog_without_schemas_acls", "lakehouse", "system", "tpch");
            assertQueryReturns(trinoCustomer1User1Client, "SELECT * FROM system.jdbc.catalogs", "lakehouse", "system");
            assertQueryReturns(trinoCustomer2User1Client, "SELECT * FROM system.jdbc.catalogs", "lakehouse", "system");

            assertSchemaList(trinoAdminClient, "catalog_without_schemas_acls", "default", "information_schema");
            assertSchemaList(trinoAdminClient, "lakehouse", "customer_1", "customer_2", "default", "information_schema");
            assertSchemaList(trinoAdminClient, "system", "information_schema", "jdbc", "metadata", "runtime");
            assertSchemaList(trinoAdminClient, "tpch", "information_schema", "sf1", "sf100", "sf1000", "sf10000", "sf100000", "sf300", "sf3000", "sf30000", "tiny");

            assertAccessDenied(trinoSupersetClient, "SHOW SCHEMAS IN catalog_without_schemas_acls");
            assertAccessDenied(trinoSupersetClient, "SHOW SCHEMAS IN lakehouse");
            assertSchemaList(trinoSupersetClient, "system", "information_schema", "jdbc", "metadata", "runtime");
            assertAccessDenied(trinoSupersetClient, "SHOW SCHEMAS IN tpch");

            assertSchemaList(trinoDataAnalyst1Client, "catalog_without_schemas_acls", "default", "information_schema");
            assertSchemaList(trinoDataAnalyst1Client, "lakehouse", "customer_1", "customer_2", "default", "information_schema");
            assertSchemaList(trinoDataAnalyst1Client, "system", "information_schema", "jdbc", "metadata", "runtime");
            assertSchemaList(trinoDataAnalyst1Client, "tpch", "information_schema", "sf1", "sf100", "sf1000", "sf10000", "sf100000", "sf300", "sf3000", "sf30000", "tiny");

            assertAccessDenied(trinoCustomer1User1Client, "SHOW SCHEMAS IN catalog_without_schemas_acls");
            assertSchemaList(trinoCustomer1User1Client, "lakehouse", "customer_1");
            assertSchemaList(trinoCustomer1User1Client, "system", "information_schema", "jdbc", "metadata", "runtime");
            assertAccessDenied(trinoCustomer1User1Client, "SHOW SCHEMAS IN tpch");

            assertAccessDenied(trinoCustomer2User1Client, "SHOW SCHEMAS IN catalog_without_schemas_acls");
            assertSchemaList(trinoCustomer2User1Client, "lakehouse", "customer_1", "customer_2");
            assertSchemaList(trinoCustomer2User1Client, "system", "information_schema", "jdbc", "metadata", "runtime");
            assertAccessDenied(trinoCustomer2User1Client, "SHOW SCHEMAS IN tpch");

            // Create tables with data for customers
            trinoDataAnalyst1Client.execute("CREATE TABLE lakehouse.customer_1.nation AS SELECT * FROM tpch.tiny.nation");
            trinoDataAnalyst1Client.execute("CREATE TABLE IF NOT EXISTS lakehouse.customer_2.nation AS SELECT * FROM tpch.tiny.nation");
            trinoDataAnalyst1Client.execute("DROP TABLE lakehouse.customer_2.nation");
            trinoDataAnalyst1Client.execute("CREATE TABLE lakehouse.customer_2.tmp AS SELECT * FROM tpch.tiny.nation");
            trinoDataAnalyst1Client.execute("ALTER TABLE lakehouse.customer_2.tmp RENAME TO lakehouse.customer_2.nation");
            assertAccessDenied(trinoDataAnalyst1Client, "CREATE TABLE tpch.tiny.foo AS SELECT * FROM tpch.tiny.nation");
            trinoDataAnalyst1Client.execute("CREATE TABLE lakehouse.customer_1.public_export AS SELECT * FROM tpch.tiny.nation");

            // Also create views
            trinoDataAnalyst1Client.execute("CREATE VIEW lakehouse.customer_1.nation_view AS SELECT * FROM lakehouse.customer_1.nation");
            trinoDataAnalyst1Client.execute("CREATE VIEW lakehouse.customer_1.nation_view_security_invoker SECURITY INVOKER AS SELECT * FROM lakehouse.customer_1.nation");
            trinoDataAnalyst1Client.execute("CREATE OR REPLACE VIEW lakehouse.customer_1.tmp AS SELECT * FROM lakehouse.customer_1.nation");
            trinoDataAnalyst1Client.execute("DROP VIEW lakehouse.customer_1.tmp");
            // Materialized views not supported by Memory connector, something like Iceberg connector would be needed

            // Try to access tables/views again
            assertQueryReturns(trinoDataAnalyst1Client, "SELECT COUNT(*) FROM lakehouse.customer_1.nation", "25");
            assertQueryReturns(trinoDataAnalyst1Client,"SELECT COUNT(*) FROM lakehouse.customer_1.nation_view", "25");
            assertQueryReturns(trinoDataAnalyst1Client,"SELECT COUNT(*) FROM lakehouse.customer_1.nation_view_security_invoker", "25");
            assertQueryReturns(trinoCustomer1User1Client,"SELECT COUNT(*) FROM lakehouse.customer_1.nation", "25");
            assertQueryReturns(trinoCustomer1User1Client,"SELECT COUNT(*) FROM lakehouse.customer_1.nation_view", "25");
            assertQueryReturns(trinoCustomer1User1Client, "SELECT COUNT(*) FROM lakehouse.customer_1.nation_view_security_invoker", "25");
            assertAccessDenied(trinoCustomer2User1Client, "SELECT COUNT(*) FROM lakehouse.customer_1.nation");
            // Besides the view having SECURITY DEFINER (default), customer-2 can not access the view itself
            assertAccessDenied(trinoCustomer2User1Client, "SELECT COUNT(*) FROM lakehouse.customer_1.nation_view");
            assertAccessDenied(trinoCustomer2User1Client, "SELECT COUNT(*) FROM lakehouse.customer_1.nation_view_security_invoker");

            assertQueryReturns(trinoDataAnalyst1Client, "SELECT name FROM lakehouse.customer_1.nation limit 1", "ALGERIA");
            assertQueryReturns(trinoDataAnalyst1Client, "SELECT name FROM lakehouse.customer_1.nation_view limit 1", "ALGERIA");
            assertQueryReturns(trinoDataAnalyst1Client, "SELECT name FROM lakehouse.customer_1.nation_view_security_invoker limit 1", "ALGERIA");
            assertQueryReturns(trinoCustomer1User1Client, "SELECT name FROM lakehouse.customer_1.nation limit 1", "ALGERIA");
            assertQueryReturns(trinoCustomer1User1Client, "SELECT name FROM lakehouse.customer_1.nation_view limit 1", "ALGERIA");
            assertQueryReturns(trinoCustomer1User1Client, "SELECT name FROM lakehouse.customer_1.nation_view_security_invoker limit 1", "ALGERIA");
            assertAccessDenied(trinoCustomer2User1Client, "SELECT name FROM lakehouse.customer_1.nation limit 1");
            assertAccessDenied(trinoCustomer2User1Client, "SELECT name FROM lakehouse.customer_1.nation_view limit 1");
            assertAccessDenied(trinoCustomer2User1Client, "SELECT name FROM lakehouse.customer_1.nation_view_security_invoker limit 1");

            // The table lakehouse.customer_1.public_export is public and can be access by anyone
            assertQueryReturns(trinoAdminClient, "SELECT name FROM lakehouse.customer_1.public_export limit 1", "ALGERIA");
            trinoDataAnalyst1Client.execute("SELECT name FROM lakehouse.customer_1.public_export limit 1");
            trinoCustomer1User1Client.execute("SELECT name FROM lakehouse.customer_1.public_export limit 1");
            trinoCustomer2User1Client.execute("SELECT name FROM lakehouse.customer_1.public_export limit 1");

            // Test SHOW CREATE statements
            assertQueryReturns(trinoAdminClient, "SHOW CREATE SCHEMA lakehouse.customer_1", "CREATE SCHEMA lakehouse.customer_1");
            assertQueryReturns(trinoDataAnalyst1Client, "SHOW CREATE SCHEMA lakehouse.customer_1", "CREATE SCHEMA lakehouse.customer_1");
            assertQueryReturns(trinoCustomer1User1Client, "SHOW CREATE SCHEMA lakehouse.customer_1", "CREATE SCHEMA lakehouse.customer_1");
            assertQueryReturns(trinoCustomer2User1Client, "SHOW CREATE SCHEMA lakehouse.customer_1", "CREATE SCHEMA lakehouse.customer_1");
            assertAccessDenied(trinoCustomer1User1Client, "SHOW CREATE SCHEMA lakehouse.customer_2");

            String createTableStatement = "CREATE TABLE lakehouse.customer_1.nation (\n" +
                    "   nationkey bigint,\n" +
                    "   name varchar(25),\n" +
                    "   regionkey bigint,\n" +
                    "   comment varchar(152)\n" +
                    ")";
            assertQueryReturns(trinoAdminClient, "SHOW CREATE TABLE lakehouse.customer_1.nation", createTableStatement);
            assertQueryReturns(trinoDataAnalyst1Client, "SHOW CREATE TABLE lakehouse.customer_1.nation", createTableStatement);
            assertQueryReturns(trinoCustomer1User1Client, "SHOW CREATE TABLE lakehouse.customer_1.nation", createTableStatement);
            assertAccessDenied(trinoCustomer2User1Client, "SHOW CREATE TABLE lakehouse.customer_1.nation");

            String createViewStatement = "CREATE VIEW lakehouse.customer_1.nation_view SECURITY DEFINER AS\n" +
                    "SELECT *\n" +
                    "FROM\n" +
                    "  lakehouse.customer_1.nation";
            assertQueryReturns(trinoAdminClient, "SHOW CREATE VIEW lakehouse.customer_1.nation_view", createViewStatement);
            assertQueryReturns(trinoDataAnalyst1Client, "SHOW CREATE VIEW lakehouse.customer_1.nation_view", createViewStatement);
            assertQueryReturns(trinoCustomer1User1Client, "SHOW CREATE VIEW lakehouse.customer_1.nation_view", createViewStatement);
            assertAccessDenied(trinoCustomer2User1Client, "SHOW CREATE VIEW lakehouse.customer_1.nation_view");

            createViewStatement = "CREATE VIEW lakehouse.customer_1.nation_view_security_invoker SECURITY INVOKER AS\n" +
                    "SELECT *\n" +
                    "FROM\n" +
                    "  lakehouse.customer_1.nation";
            assertQueryReturns(trinoAdminClient, "SHOW CREATE VIEW lakehouse.customer_1.nation_view_security_invoker", createViewStatement);
            assertQueryReturns(trinoDataAnalyst1Client, "SHOW CREATE VIEW lakehouse.customer_1.nation_view_security_invoker", createViewStatement);
            assertQueryReturns(trinoCustomer1User1Client, "SHOW CREATE VIEW lakehouse.customer_1.nation_view_security_invoker", createViewStatement);
            assertAccessDenied(trinoCustomer2User1Client, "SHOW CREATE VIEW lakehouse.customer_1.nation_view_security_invoker");
        }
    }

    private void assertQueryReturns(TestingTrinoClient trinoClient, String query, String... expected)
    {
        List<String> rows = new ArrayList<>();
        MaterializedResult result = trinoClient.execute(query).getResult();
        for (MaterializedRow row : result) {
            rows.add(row.getField(0).toString());
        }
        assertEquals(new ArrayList<>(Arrays.asList(expected)), rows);
    }

    private void assertSchemaList(TestingTrinoClient trinoClient, String catalog, String... expectedSchemas)
    {
        List<String> schemas = new ArrayList<>();
        MaterializedResult result = trinoClient.execute("SHOW SCHEMAS IN " + catalog).getResult();
        for (MaterializedRow row : result) {
            schemas.add(row.getField(0).toString());
        }
        assertEquals(new ArrayList<>(Arrays.asList(expectedSchemas)), schemas);
    }

    private void assertAccessDenied(TestingTrinoClient trinoClient, String query)
    {
        RuntimeException error = assertThrows(RuntimeException.class, () -> {
            trinoClient.execute(query);
        });
        assertTrue(error.getMessage().contains("Access Denied"), "Error must mention 'Access Denied': " + error.getMessage());
    }
}
