/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.search.collector;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import org.apache.lucene.search.FieldDoc;
import org.opensearch.neuralsearch.search.HitsThresholdChecker;
import org.opensearch.search.internal.SearchContext;
import org.opensearch.search.sort.SortAndFormats;

@AllArgsConstructor
@Builder
@Getter
public class HybridCollectorFactoryDTO {
    private final SortAndFormats sortAndFormats;
    private final SearchContext searchContext;
    private final HitsThresholdChecker hitsThresholdChecker;
    private final int numHits;
    private final FieldDoc after;
}
