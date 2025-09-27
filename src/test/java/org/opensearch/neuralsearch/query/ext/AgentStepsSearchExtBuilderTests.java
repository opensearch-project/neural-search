/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.query.ext;

import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.xcontent.ToXContent;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;

import static org.opensearch.common.xcontent.XContentFactory.jsonBuilder;

public class AgentStepsSearchExtBuilderTests extends OpenSearchTestCase {

    public void testConstructor() {
        String agentSteps = "Step 1: Analysis\nStep 2: Execution";
        String memoryId = "test-memory-id";
        String dslQuery = "{\"query\":{\"match\":{\"field\":\"value\"}}}";
        AgentStepsSearchExtBuilder builder = new AgentStepsSearchExtBuilder(agentSteps, memoryId, dslQuery);

        assertEquals(agentSteps, builder.getAgentStepsSummary());
        assertEquals(memoryId, builder.getMemoryId());
        assertEquals(dslQuery, builder.getDslQuery());
        assertEquals(AgentStepsSearchExtBuilder.AGENT_STEPS_FIELD_NAME, builder.getWriteableName());
    }

    public void testConstructorWithEmptySteps() {
        String agentSteps = "";
        String memoryId = null;
        String dslQuery = null;
        AgentStepsSearchExtBuilder builder = new AgentStepsSearchExtBuilder(agentSteps, memoryId, dslQuery);
        assertEquals(agentSteps, builder.getAgentStepsSummary());
        assertNull(builder.getMemoryId());
        assertNull(builder.getDslQuery());
    }

    public void testSerialization() throws IOException {
        String agentSteps = "Step 1: Query parsing\nStep 2: Index search\nStep 3: Result ranking";
        String memoryId = "test-memory-123";
        String dslQuery = "{\"query\":{\"bool\":{\"must\":[]}}}";
        AgentStepsSearchExtBuilder original = new AgentStepsSearchExtBuilder(agentSteps, memoryId, dslQuery);

        // Serialize
        BytesStreamOutput output = new BytesStreamOutput();
        original.writeTo(output);

        // Deserialize
        StreamInput input = output.bytes().streamInput();
        AgentStepsSearchExtBuilder deserialized = new AgentStepsSearchExtBuilder(input);

        assertEquals(original.getAgentStepsSummary(), deserialized.getAgentStepsSummary());
        assertEquals(original.getMemoryId(), deserialized.getMemoryId());
        assertEquals(original.getDslQuery(), deserialized.getDslQuery());
    }

    public void testToXContent() throws IOException {
        String agentSteps = "Step 1: Understanding query\nStep 2: Generating response";
        String memoryId = "memory-456";
        String dslQuery = "{\"query\":{\"term\":{\"status\":\"active\"}}}";
        AgentStepsSearchExtBuilder builder = new AgentStepsSearchExtBuilder(agentSteps, memoryId, dslQuery);

        XContentBuilder xContentBuilder = jsonBuilder();
        xContentBuilder.startObject();
        builder.toXContent(xContentBuilder, ToXContent.EMPTY_PARAMS);
        xContentBuilder.endObject();

        String json = xContentBuilder.toString();
        assertTrue(json.contains("\"agent_steps_summary\""));
        assertTrue(json.contains("\"memory_id\""));
        assertTrue(json.contains("\"dsl_query\""));
        assertTrue(json.contains("Step 1: Understanding query"));
        assertTrue(json.contains("Step 2: Generating response"));
        assertTrue(json.contains("memory-456"));
    }

    public void testToXContentWithNullValues() throws IOException {
        AgentStepsSearchExtBuilder builder = new AgentStepsSearchExtBuilder(null, null, null);

        XContentBuilder xContentBuilder = jsonBuilder();
        xContentBuilder.startObject();
        builder.toXContent(xContentBuilder, ToXContent.EMPTY_PARAMS);
        xContentBuilder.endObject();

        String json = xContentBuilder.toString();
        // Should only contain empty object since all fields are null
        assertEquals("{}", json);
    }

    public void testEquals() {
        String agentSteps = "Step 1: Test";
        String memoryId = "test-mem";
        String dslQuery = "{\"query\":{}}";
        AgentStepsSearchExtBuilder builder1 = new AgentStepsSearchExtBuilder(agentSteps, memoryId, dslQuery);
        AgentStepsSearchExtBuilder builder2 = new AgentStepsSearchExtBuilder(agentSteps, memoryId, dslQuery);
        AgentStepsSearchExtBuilder builder3 = new AgentStepsSearchExtBuilder("Different steps", memoryId, dslQuery);
        AgentStepsSearchExtBuilder builder4 = new AgentStepsSearchExtBuilder(agentSteps, "different-mem", dslQuery);
        AgentStepsSearchExtBuilder builder5 = new AgentStepsSearchExtBuilder(agentSteps, memoryId, "different-query");

        assertEquals(builder1, builder2);
        assertNotEquals(builder1, builder3);
        assertNotEquals(builder1, builder4);
        assertNotEquals(builder1, builder5);
        assertNotEquals(builder1, null);
        assertNotEquals(builder1, "not a builder");
    }

    public void testHashCode() {
        String agentSteps = "Step 1: Test";
        String memoryId = "test-mem";
        String dslQuery = "{\"query\":{}}";
        AgentStepsSearchExtBuilder builder1 = new AgentStepsSearchExtBuilder(agentSteps, memoryId, dslQuery);
        AgentStepsSearchExtBuilder builder2 = new AgentStepsSearchExtBuilder(agentSteps, memoryId, dslQuery);

        assertEquals(builder1.hashCode(), builder2.hashCode());
    }

    public void testSerializationWithNullValues() throws IOException {
        String agentSteps = "Step 1: Test";
        AgentStepsSearchExtBuilder original = new AgentStepsSearchExtBuilder(agentSteps, null, null);

        BytesStreamOutput output = new BytesStreamOutput();
        original.writeTo(output);

        StreamInput input = output.bytes().streamInput();
        AgentStepsSearchExtBuilder deserialized = new AgentStepsSearchExtBuilder(input);

        assertEquals(original.getAgentStepsSummary(), deserialized.getAgentStepsSummary());
        assertNull(deserialized.getMemoryId());
        assertNull(deserialized.getDslQuery());
    }

    public void testToXContentWithSelectiveFields() throws IOException {
        String agentSteps = "Step 1: Test";
        String dslQuery = "{\"query\":{\"match_all\":{}}}";
        AgentStepsSearchExtBuilder builder = new AgentStepsSearchExtBuilder(agentSteps, null, dslQuery);

        XContentBuilder xContentBuilder = jsonBuilder();
        xContentBuilder.startObject();
        builder.toXContent(xContentBuilder, ToXContent.EMPTY_PARAMS);
        xContentBuilder.endObject();

        String json = xContentBuilder.toString();
        assertTrue(json.contains("\"agent_steps_summary\""));
        assertTrue(json.contains("\"dsl_query\""));
        assertFalse(json.contains("\"memory_id\""));
    }
}
