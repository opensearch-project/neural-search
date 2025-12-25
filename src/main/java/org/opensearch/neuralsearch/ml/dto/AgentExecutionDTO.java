/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.ml.dto;

import lombok.AllArgsConstructor;
import lombok.Getter;

/**
 * DTO for agent execution results containing DSL query and agent steps summary
 */
@Getter
@AllArgsConstructor
public class AgentExecutionDTO {
    private final String dslQuery;
    private final String agentStepsSummary;
    private final String memoryId;
    private final String selectedIndex;
}
