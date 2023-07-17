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

import io.trino.Session;
import io.trino.execution.QueryIdGenerator;
import io.trino.metadata.SessionPropertyManager;
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
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class OpaAccessControlSystemTest
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
        try (Socket sock = new Socket()) {
            sock.bind(new InetSocketAddress("127.0.0.1", 0));
            return new InetSocketAddress(sock.getLocalAddress(), sock.getLocalPort());
        }
    }

    private static void awaitSocketOpen(InetSocketAddress addr, int attempts, int timeoutMs)
            throws IOException, InterruptedException
    {
        for (int i = 0; i < attempts; ++i) {
            try (Socket socket = new Socket()) {
                socket.connect(addr, timeoutMs);
                return;
            }
            catch (SocketTimeoutException e) {
                // ignored
            }
            catch (IOException e) {
                Thread.sleep(timeoutMs);
            }
        }
        throw new SocketTimeoutException("Timed out waiting for addr " + addr + " to be available ("
                + attempts + " attempts made at " + timeoutMs + "ms each)");
    }

    @BeforeAll
    public static void setupOpa()
            throws IOException, InterruptedException
    {
        InetSocketAddress opaSocket = findAvailableTcpPort();
        String opaEndpoint = String.format("%s:%d", opaSocket.getHostString(), opaSocket.getPort());
        System.out.println("OPA has endpoint " + opaEndpoint);
        opaServer = new ProcessBuilder(System.getenv().getOrDefault("OPA_BINARY", "opa"),
                "run",
                "--server",
                "--addr", opaEndpoint,
                "--set", "decision_logs.console=true"
        ).inheritIO().start();
        awaitSocketOpen(opaSocket, 100, 200);
        opaServerUri = URI.create(String.format("http://%s/", opaEndpoint));
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
            QueryIdGenerator idGen = new QueryIdGenerator();
            Identity identity = Identity.forUser("bob").build();
            SessionPropertyManager sessionPropertyManager = new SessionPropertyManager();
            Session session = Session.builder(sessionPropertyManager)
                    .setQueryId(idGen.createNextQueryId()).setIdentity(identity).build();
            trinoServer = TestingTrinoServer.builder()
                    .setSystemAccessControls(
                            Collections.singletonList(
                                    new OpaAccessControlFactory()
                                            .create(
                                                    Map.of("opa.policy.uri", opaServerUri.resolve("v1/data/trino/allow").toString()))))
                    .build();
            trinoClient = new TestingTrinoClient(trinoServer, session);
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
            QueryIdGenerator idGen = new QueryIdGenerator();
            Identity identity = Identity.forUser("bob").build();
            SessionPropertyManager sessionPropertyManager = new SessionPropertyManager();
            Session session = Session.builder(sessionPropertyManager)
                    .setQueryId(idGen.createNextQueryId()).setIdentity(identity).build();
            trinoServer = TestingTrinoServer.builder()
                    .setSystemAccessControls(
                            Collections.singletonList(
                                    new OpaAccessControlFactory()
                                            .create(
                                                    Map.of(
                                                            "opa.policy.uri", opaServerUri.resolve("v1/data/trino/allow").toString(),
                                                            "opa.policy.batched-uri", opaServerUri.resolve("v1/data/trino/extended").toString()))))
                    .build();
            trinoClient = new TestingTrinoClient(trinoServer, session);
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
}
