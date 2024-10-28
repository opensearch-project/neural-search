/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.processor.explain;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import org.apache.lucene.search.Explanation;
import org.opensearch.neuralsearch.processor.SearchShard;

import java.util.List;
import java.util.Map;

@AllArgsConstructor
@Builder
@Getter
public class ProcessorExplainDto {
    Explanation explanation;
    Map<SearchShard, List<CombinedExplainDetails>> explainDetailsByShard;
}
