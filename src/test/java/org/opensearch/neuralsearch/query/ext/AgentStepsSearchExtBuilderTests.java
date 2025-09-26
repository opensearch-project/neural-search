/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.query.ext;

import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.common.xcontent.XContentType;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.xcontent.ToXContent;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;

import static org.opensearch.common.xcontent.XContentFactory.jsonBuilder;

public class AgentStepsSearchExtBuilderTests extends OpenSearchTestCase {

    public void testConstructor() {
        String agentSteps = "Step 1: Analysis\nStep 2: Execution";
        String memoryId = "test-memory-id";
        AgentStepsSearchExtBuilder builder = new AgentStepsSearchExtBuilder(agentSteps, memoryId);

        assertEquals(agentSteps, builder.getAgentStepsSummary());
        assertEquals(memoryId, builder.getMemoryId());
        assertEquals(AgentStepsSearchExtBuilder.AGENT_STEPS_FIELD_NAME, builder.getWriteableName());
    }

    public void testConstructorWithEmptySteps() {
        String agentSteps = "";
        String memoryId = null;
        AgentStepsSearchExtBuilder builder = new AgentStepsSearchExtBuilder(agentSteps, memoryId);
        assertEquals(agentSteps, builder.getAgentStepsSummary());
        assertNull(builder.getMemoryId());
    }

    public void testSerialization() throws IOException {
        String agentSteps = "Step 1: Query parsing\nStep 2: Index search\nStep 3: Result ranking";
        String memoryId = "test-memory-123";
        AgentStepsSearchExtBuilder original = new AgentStepsSearchExtBuilder(agentSteps, memoryId);

        // Serialize
        BytesStreamOutput output = new BytesStreamOutput();
        original.writeTo(output);

        // Deserialize
        StreamInput input = output.bytes().streamInput();
        AgentStepsSearchExtBuilder deserialized = new AgentStepsSearchExtBuilder(input);

        assertEquals(original.getAgentStepsSummary(), deserialized.getAgentStepsSummary());
        assertEquals(original.getMemoryId(), deserialized.getMemoryId());
    }

    public void testToXContent() throws IOException {
        String agentSteps = "Step 1: Understanding query\nStep 2: Generating response";
        String memoryId = "memory-456";
        AgentStepsSearchExtBuilder builder = new AgentStepsSearchExtBuilder(agentSteps, memoryId);

        XContentBuilder xContentBuilder = jsonBuilder();
        xContentBuilder.startObject();
        builder.toXContent(xContentBuilder, ToXContent.EMPTY_PARAMS);
        xContentBuilder.endObject();

        String json = xContentBuilder.toString();
        assertTrue(json.contains("\"agent_steps_summary\""));
        assertTrue(json.contains("\"memory_id\""));
        assertTrue(json.contains("Step 1: Understanding query"));
        assertTrue(json.contains("Step 2: Generating response"));
        assertTrue(json.contains("memory-456"));
    }

    public void testToXContentWithEmptySteps() throws IOException {
        AgentStepsSearchExtBuilder builder = new AgentStepsSearchExtBuilder("", null);

        XContentBuilder xContentBuilder = jsonBuilder();
        xContentBuilder.startObject();
        builder.toXContent(xContentBuilder, ToXContent.EMPTY_PARAMS);
        xContentBuilder.endObject();

        String json = xContentBuilder.toString();
        assertTrue(json.contains("\"agent_steps_summary\":\"\""));
        assertTrue(json.contains("\"memory_id\":null"));
    }

    public void testFromXContent() throws IOException {
        String json = "{\"agent_steps_summary\":\"Step 1: Process query\\nStep 2: Return results\",\"memory_id\":\"mem-789\"}";

        XContentParser parser = createParser(XContentType.JSON.xContent(), json);
        parser.nextToken(); // START_OBJECT

        AgentStepsSearchExtBuilder builder = AgentStepsSearchExtBuilder.fromXContent(parser);

        assertEquals("Step 1: Process query\nStep 2: Return results", builder.getAgentStepsSummary());
        assertEquals("mem-789", builder.getMemoryId());
    }

    public void testFromXContentWithEmptyValue() throws IOException {
        String json = "{\"agent_steps_summary\":\"\",\"memory_id\":null}";

        XContentParser parser = createParser(XContentType.JSON.xContent(), json);
        parser.nextToken(); // START_OBJECT

        AgentStepsSearchExtBuilder builder = AgentStepsSearchExtBuilder.fromXContent(parser);

        assertEquals("", builder.getAgentStepsSummary());
        assertNull(builder.getMemoryId());
    }

    public void testEquals() {
        String agentSteps = "Step 1: Test";
        String memoryId = "test-mem";
        AgentStepsSearchExtBuilder builder1 = new AgentStepsSearchExtBuilder(agentSteps, memoryId);
        AgentStepsSearchExtBuilder builder2 = new AgentStepsSearchExtBuilder(agentSteps, memoryId);
        AgentStepsSearchExtBuilder builder3 = new AgentStepsSearchExtBuilder("Different steps", memoryId);
        AgentStepsSearchExtBuilder builder4 = new AgentStepsSearchExtBuilder(agentSteps, "different-mem");

        assertEquals(builder1, builder2);
        assertNotEquals(builder1, builder3);
        assertNotEquals(builder1, builder4);
        assertNotEquals(builder1, null);
        assertNotEquals(builder1, "not a builder");
    }

    public void testHashCode() {
        String agentSteps = "Step 1: Test";
        String memoryId = "test-mem";
        AgentStepsSearchExtBuilder builder1 = new AgentStepsSearchExtBuilder(agentSteps, memoryId);
        AgentStepsSearchExtBuilder builder2 = new AgentStepsSearchExtBuilder(agentSteps, memoryId);

        assertEquals(builder1.hashCode(), builder2.hashCode());
    }

    public void testSerializationWithNullMemoryId() throws IOException {
        String agentSteps = "Step 1: Test";
        AgentStepsSearchExtBuilder original = new AgentStepsSearchExtBuilder(agentSteps, null);

        BytesStreamOutput output = new BytesStreamOutput();
        original.writeTo(output);

        StreamInput input = output.bytes().streamInput();
        AgentStepsSearchExtBuilder deserialized = new AgentStepsSearchExtBuilder(input);

        assertEquals(original.getAgentStepsSummary(), deserialized.getAgentStepsSummary());
        assertNull(deserialized.getMemoryId());
    }

    public void testFromXContentWithOnlyMemoryId() throws IOException {
        String json = "{\"memory_id\":\"only-mem\"}";

        XContentParser parser = createParser(XContentType.JSON.xContent(), json);
        parser.nextToken(); // START_OBJECT

        AgentStepsSearchExtBuilder builder = AgentStepsSearchExtBuilder.fromXContent(parser);

        assertNull(builder.getAgentStepsSummary());
        assertEquals("only-mem", builder.getMemoryId());
    }
}
