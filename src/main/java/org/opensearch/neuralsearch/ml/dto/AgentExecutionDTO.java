/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.ml.dto;

import lombok.Getter;

/**
 * DTO for agent execution results containing DSL query and agent steps summary
 */
@Getter
public class AgentExecutionDTO {
    private final String dslQuery;
    private final String agentStepsSummary;
    private final String memoryId;
    private final String selectedIndex;

    public AgentExecutionDTO(String dslQuery, String agentStepsSummary, String memoryId) {
        this(dslQuery, agentStepsSummary, memoryId, null);
    }

    // New constructor with selectedIndex
    public AgentExecutionDTO(String dslQuery, String agentStepsSummary, String memoryId, String selectedIndex) {
        this.dslQuery = dslQuery;
        this.agentStepsSummary = agentStepsSummary;
        this.memoryId = memoryId;
        this.selectedIndex = selectedIndex;
    }
}
