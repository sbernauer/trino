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

import io.airlift.json.JsonCodec;
import io.airlift.json.JsonCodecFactory;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class ResponseTests
{
    private JsonCodec<OpaAccessControl.OpaQueryResult> responseCodec;
    private JsonCodec<OpaBatchAccessControl.OpaBatchQueryResult> batchResponseCodec;

    @BeforeEach
    public void setupParser()
    {
        this.responseCodec = new JsonCodecFactory().jsonCodec(OpaAccessControl.OpaQueryResult.class);
        this.batchResponseCodec = new JsonCodecFactory().jsonCodec(OpaBatchAccessControl.OpaBatchQueryResult.class);
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    public void testCanDeserializeOpaSingleResponse(boolean response)
    {
        OpaAccessControl.OpaQueryResult result = this.responseCodec.fromJson("""
                {
                    "decision_id": "foo",
                    "result": %s
                }""".formatted(String.valueOf(response)));
        assertEquals(response, result.result());
        assertEquals("foo", result.decisionId());
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    public void testCanDeserializeOpaSingleResponseWithNoDecisionId(boolean response)
    {
        OpaAccessControl.OpaQueryResult result = this.responseCodec.fromJson("""
                {
                    "result": %s
                }""".formatted(String.valueOf(response)));
        assertEquals(response, result.result());
        assertNull(result.decisionId());
    }

    @Test
    public void testSingleResponseWithExtraFields()
    {
        OpaAccessControl.OpaQueryResult result = this.responseCodec.fromJson("""
                {
                    "result": true,
                    "someExtraInfo": ["foo"]
                }""");
        assertTrue(result.result());
        assertNull(result.decisionId());
    }

    @Test
    public void testUndefinedDecisionSingleResponse()
    {
        OpaAccessControl.OpaQueryResult result = this.responseCodec.fromJson("{}");
        assertNull(result.result());
        assertNull(result.decisionId());
    }

    @Test
    public void testUndefinedDecisionBatchResponse()
    {
        OpaBatchAccessControl.OpaBatchQueryResult result = this.batchResponseCodec.fromJson("{}");
        assertNull(result.result());
        assertNull(result.decisionId());
    }

    @Test
    public void testBatchResponseEmptyNoDecisionId()
    {
        OpaBatchAccessControl.OpaBatchQueryResult result = this.batchResponseCodec.fromJson("""
                {
                    "result": []
                }""");
        assertEquals(List.of(), result.result());
        assertNull(result.decisionId());
    }

    @Test
    public void testBatchResponseWithItemsNoDecisionId()
    {
        OpaBatchAccessControl.OpaBatchQueryResult result = this.batchResponseCodec.fromJson("""
                {
                    "result": [1, 2, 3]
                }""");
        assertEquals(List.of(1, 2, 3), result.result());
        assertNull(result.decisionId());
    }

    @Test
    public void testBatchResponseWithItemsAndDecisionId()
    {
        OpaBatchAccessControl.OpaBatchQueryResult result = this.batchResponseCodec.fromJson("""
                {
                    "result": [1, 2, 3],
                    "decision_id": "foobar"
                }""");
        assertEquals(List.of(1, 2, 3), result.result());
        assertEquals("foobar", result.decisionId());
    }

    @Test
    public void testBatchResponseWithExtraFields()
    {
        OpaBatchAccessControl.OpaBatchQueryResult result = this.batchResponseCodec.fromJson("""
                {
                    "result": [1, 2, 3],
                    "decision_id": "foobar",
                    "someInfo": "foo",
                    "andAnObject": {}
                }""");
        assertEquals(List.of(1, 2, 3), result.result());
        assertEquals("foobar", result.decisionId());
    }
}
