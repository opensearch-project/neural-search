/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.ml;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;

/**
 * DTO object to hold data in AgentInfoDTO class
 */
@AllArgsConstructor
@Builder
@Getter
public class AgentInfoDTO {
    private final String type;
    private final boolean hasSystemPrompt;
    private final boolean hasUserPrompt;
}
