/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.fusion.math;

import lombok.AccessLevel;
import lombok.NoArgsConstructor;

/**
 * The search-pipeline processor type names under which fusion configuration has historically been stored. The fused
 * path reads a pipeline's technique configuration by these keys ({@code fusion: "pipeline"}); the classic processors are
 * registered under them. Owned here so that reading the configuration never requires the executing processor class.
 */
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public final class FusionProcessorTypes {

    /** The normalization-processor: score-based normalization followed by a combination technique. */
    public static final String NORMALIZATION_PROCESSOR = "normalization-processor";
    /** The score-ranker-processor: reciprocal rank fusion. */
    public static final String SCORE_RANKER_PROCESSOR = "score-ranker-processor";
    /** The response processor that renders a hybrid query's explanation. */
    public static final String HYBRID_SCORE_EXPLANATION_PROCESSOR = "hybrid_score_explanation";
}
