/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.processor.combination;

import java.util.List;
import java.util.Map;
import java.util.Set;

import org.opensearch.neuralsearch.fusion.math.CombinationWeights;
import org.opensearch.neuralsearch.fusion.math.FusionTechniqueNames;
import org.opensearch.neuralsearch.fusion.math.RrfCombinationCore;
import org.opensearch.neuralsearch.processor.explain.ExplainableTechnique;

import lombok.ToString;

import static org.opensearch.neuralsearch.processor.combination.ScoreCombinationUtil.PARAM_NAME_WEIGHTS;
import static org.opensearch.neuralsearch.processor.explain.ExplanationUtils.describeCombinationTechnique;

/**
 * Abstracts combination of scores for reciprocal rank fusion: the weighted sum of the reciprocal-rank contributions
 *
 * <p>The arithmetic lives in {@link RrfCombinationCore}; this class owns only the processor-config parsing and the explain
 * rendering, so the classic processor and the in-query fusion path cannot compute two different fused scores.
 */
@ToString(onlyExplicitlyIncluded = true)
public class RRFScoreCombinationTechnique implements ScoreCombinationTechnique, ExplainableTechnique {
    @ToString.Include
    public static final String TECHNIQUE_NAME = FusionTechniqueNames.RRF_COMBINATION;
    private static final Set<String> SUPPORTED_PARAMS = Set.of(PARAM_NAME_WEIGHTS);
    @ToString.Include
    private final List<Float> weights;
    private final RrfCombinationCore core;

    public RRFScoreCombinationTechnique(final Map<String, Object> params, final ScoreCombinationUtil combinationUtil) {
        combinationUtil.validateParams(params, SUPPORTED_PARAMS);
        weights = combinationUtil.getWeights(params);
        core = new RrfCombinationCore(CombinationWeights.of(weights));
    }

    @Override
    public float combine(final float[] scores) {
        return core.combine(scores);
    }

    @Override
    public CombinationWeights weights() {
        return core.weights();
    }

    @Override
    public String techniqueName() {
        return TECHNIQUE_NAME;
    }

    @Override
    public String describe() {
        return describeCombinationTechnique(TECHNIQUE_NAME, weights);
    }
}
