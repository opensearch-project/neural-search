/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.query.ext;

import java.io.IOException;
import java.util.Objects;

import lombok.AllArgsConstructor;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.search.SearchExtBuilder;

import lombok.Getter;

/**
 * SearchExtBuilder for agent steps summary in agentic search responses
 */
@AllArgsConstructor
public class AgentStepsSearchExtBuilder extends SearchExtBuilder {

    public final static String PARAM_FIELD_NAME = "agent_steps_summary";
    public final static String MEMORY_ID_FIELD_NAME = "memory_id";
    @Getter
    protected String agentStepsSummary;
    @Getter
    protected String memoryId;

    public AgentStepsSearchExtBuilder(StreamInput in) throws IOException {
        agentStepsSummary = in.readString();
        memoryId = in.readOptionalString();
    }

    @Override
    public String getWriteableName() {
        return PARAM_FIELD_NAME;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(agentStepsSummary);
        out.writeOptionalString(memoryId);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.field(PARAM_FIELD_NAME, agentStepsSummary);
        builder.field(MEMORY_ID_FIELD_NAME, memoryId);
        return builder;
    }

    @Override
    public int hashCode() {
        return Objects.hash(this.getClass(), this.agentStepsSummary, this.memoryId);
    }

    @Override
    public boolean equals(Object obj) {
        return (obj instanceof AgentStepsSearchExtBuilder)
            && Objects.equals(agentStepsSummary, ((AgentStepsSearchExtBuilder) obj).agentStepsSummary)
            && Objects.equals(memoryId, ((AgentStepsSearchExtBuilder) obj).memoryId);
    }

    public static AgentStepsSearchExtBuilder fromXContent(XContentParser parser) throws IOException {
        String agentSteps = null;
        String memoryId = null;

        while (parser.nextToken() != XContentParser.Token.END_OBJECT) {
            if (parser.currentToken() == XContentParser.Token.FIELD_NAME) {
                String fieldName = parser.currentName();
                parser.nextToken();

                if (PARAM_FIELD_NAME.equals(fieldName)) {
                    if (parser.currentToken() == XContentParser.Token.VALUE_STRING) {
                        agentSteps = parser.text();
                    }
                } else if (MEMORY_ID_FIELD_NAME.equals(fieldName)) {
                    if (parser.currentToken() == XContentParser.Token.VALUE_STRING) {
                        memoryId = parser.text();
                    }
                }
            }
        }

        return new AgentStepsSearchExtBuilder(agentSteps, memoryId);
    }
}
