/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.processor.combination.dto;

import java.util.List;
import lombok.AllArgsConstructor;
import lombok.Getter;
import org.apache.lucene.search.Sort;
import org.opensearch.neuralsearch.processor.CompoundTopDocs;
import org.opensearch.neuralsearch.processor.combination.ScoreCombinationTechnique;

/**
 * CombineScoresDTO handles the request sent for score combination after normalization.
 */
@AllArgsConstructor
public class CombineScoresDTO {
    @Getter
    private List<CompoundTopDocs> queryTopDocs;
    @Getter
    private ScoreCombinationTechnique scoreCombinationTechnique;
    @Getter
    private Sort sort;
}
