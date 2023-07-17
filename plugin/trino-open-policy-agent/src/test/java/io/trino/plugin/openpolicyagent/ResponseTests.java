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
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.IOException;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

public class ResponseTests
{
    private ObjectMapper json;

    @BeforeEach
    public void setupParser()
    {
        this.json = new ObjectMapper();
        // do not include null values
        this.json.setSerializationInclusion(JsonInclude.Include.NON_NULL);
        // deal with Optional<T> values
        this.json.registerModule(new Jdk8Module());
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    public void testCanDeserializeOpaSingleResponse(boolean response)
            throws IOException
    {
        OpaAccessControl.OpaQueryResult result = this.json.readValue("""
                {
                    "decision_id": "foo",
                    "result": %s
                }""".formatted(String.valueOf(response)), OpaAccessControl.OpaQueryResult.class);
        assertEquals(response, result.result());
        assertEquals("foo", result.decisionId());
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    public void testCanDeserializeOpaSingleResponseWithNoDecisionId(boolean response)
            throws IOException
    {
        OpaAccessControl.OpaQueryResult result = this.json.readValue("""
                {
                    "result": %s
                }""".formatted(String.valueOf(response)), OpaAccessControl.OpaQueryResult.class);
        assertEquals(response, result.result());
        assertNull(result.decisionId());
    }

    @Test
    public void testUndefinedDecisionSingleResponse()
            throws IOException
    {
        OpaAccessControl.OpaQueryResult result = this.json.readValue(
                "{}",
                OpaAccessControl.OpaQueryResult.class);
        assertNull(result.result());
        assertNull(result.decisionId());
    }

    @Test
    public void testUndefinedDecisionBatchResponse()
            throws IOException
    {
        OpaBatchAccessControl.OpaBatchQueryResult result = this.json.readValue(
                "{}",
                OpaBatchAccessControl.OpaBatchQueryResult.class);
        assertNull(result.result());
        assertNull(result.decisionId());
    }

    @Test
    public void testBatchResponseEmptyNoDecisionId()
            throws IOException
    {
        OpaBatchAccessControl.OpaBatchQueryResult result = this.json.readValue("""
                {
                    "result": []
                }""", OpaBatchAccessControl.OpaBatchQueryResult.class);
        assertEquals(List.of(), result.result());
        assertNull(result.decisionId());
    }

    @Test
    public void testBatchResponseWithItemsNoDecisionId()
            throws IOException
    {
        OpaBatchAccessControl.OpaBatchQueryResult result = this.json.readValue("""
                {
                    "result": [1, 2, 3]
                }""", OpaBatchAccessControl.OpaBatchQueryResult.class);
        assertEquals(List.of(1, 2, 3), result.result());
        assertNull(result.decisionId());
    }

    @Test
    public void testBatchResponseWithItemsAndDecisionId()
            throws IOException
    {
        OpaBatchAccessControl.OpaBatchQueryResult result = this.json.readValue("""
                {
                    "result": [1, 2, 3],
                    "decision_id": "foobar"
                }""", OpaBatchAccessControl.OpaBatchQueryResult.class);
        assertEquals(List.of(1, 2, 3), result.result());
        assertEquals("foobar", result.decisionId());
    }
}
