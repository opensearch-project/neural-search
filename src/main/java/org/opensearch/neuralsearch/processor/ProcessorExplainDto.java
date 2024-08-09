/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.processor;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import org.apache.lucene.search.Explanation;

import java.util.Map;

@AllArgsConstructor
@Builder
@Getter
public class ProcessorExplainDto {
    Explanation explanation;
    Map<DocIdAtQueryPhase, String> normalizedScoresByDocId;
    Map<DocIdAtQueryPhase, String> combinedScoresByDocId;
}
