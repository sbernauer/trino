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
package io.trino.plugin.cassandra;

import io.airlift.json.JsonCodec;
import org.junit.jupiter.api.Test;

import static org.testng.Assert.assertEquals;

public class TestCassandraTableHandle
{
    private final JsonCodec<CassandraTableHandle> codec = JsonCodec.jsonCodec(CassandraTableHandle.class);

    @Test
    public void testRoundTripNamedRelationHandle()
    {
        CassandraTableHandle expected = new CassandraTableHandle(new CassandraNamedRelationHandle("schema", "table"));

        String json = codec.toJson(expected);
        CassandraTableHandle actual = codec.fromJson(json);

        assertEquals(actual.getRequiredNamedRelation().getSchemaTableName(), expected.getRequiredNamedRelation().getSchemaTableName());
    }

    @Test
    public void testRoundTripQueryRelationHandle()
    {
        CassandraTableHandle expected = new CassandraTableHandle(new CassandraQueryRelationHandle("SELECT * FROM tpch.region"));

        String json = codec.toJson(expected);
        CassandraTableHandle actual = codec.fromJson(json);

        assertEquals(actual.getRelationHandle(), new CassandraQueryRelationHandle("SELECT * FROM tpch.region"));
    }
}
