/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.processor.combination;

import lombok.ToString;
import lombok.extern.log4j.Log4j2;

@ToString(onlyExplicitlyIncluded = true)
@Log4j2
public class DedupCombinationTechnique implements ScoreCombinationTechnique {

    @ToString.Include
    public static final String TECHNIQUE_NAME = "dedup";

    @Override
    public float combine(float[] scores) {
        if (scores.length < 2 || scores.length > 2) {
            throw new IllegalArgumentException("scores array length during dedup should be equal to 2");
        }
        if (scores[0] > scores[1]) {
            return scores[0];
        }
        return scores[1];
    }
}
