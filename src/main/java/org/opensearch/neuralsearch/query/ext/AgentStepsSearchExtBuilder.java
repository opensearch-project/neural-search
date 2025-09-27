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
import org.opensearch.search.SearchExtBuilder;

import lombok.Getter;

/**
 * SearchExtBuilder for agent steps summary in agentic search responses
 */
@AllArgsConstructor
public class AgentStepsSearchExtBuilder extends SearchExtBuilder {

    public final static String AGENT_STEPS_FIELD_NAME = "agent_steps_summary";
    public final static String MEMORY_ID_FIELD_NAME = "memory_id";
    public final static String DSL_QUERY_FIELD_NAME = "dsl_query";
    @Getter
    protected String agentStepsSummary;
    @Getter
    protected String memoryId;
    @Getter
    protected String dslQuery;

    public AgentStepsSearchExtBuilder(StreamInput in) throws IOException {
        agentStepsSummary = in.readOptionalString();
        memoryId = in.readOptionalString();
        dslQuery = in.readOptionalString();
    }

    @Override
    public String getWriteableName() {
        return AGENT_STEPS_FIELD_NAME;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeOptionalString(agentStepsSummary);
        out.writeOptionalString(memoryId);
        out.writeOptionalString(dslQuery);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        if (agentStepsSummary != null) {
            builder.field(AGENT_STEPS_FIELD_NAME, agentStepsSummary);
        }
        if (memoryId != null) {
            builder.field(MEMORY_ID_FIELD_NAME, memoryId);
        }
        if (dslQuery != null) {
            builder.field(DSL_QUERY_FIELD_NAME, dslQuery);
        }
        return builder;
    }

    @Override
    public int hashCode() {
        return Objects.hash(this.getClass(), this.agentStepsSummary, this.memoryId, this.dslQuery);
    }

    @Override
    public boolean equals(Object obj) {
        return (obj instanceof AgentStepsSearchExtBuilder)
            && Objects.equals(agentStepsSummary, ((AgentStepsSearchExtBuilder) obj).agentStepsSummary)
            && Objects.equals(memoryId, ((AgentStepsSearchExtBuilder) obj).memoryId)
            && Objects.equals(dslQuery, ((AgentStepsSearchExtBuilder) obj).dslQuery);
    }
}
