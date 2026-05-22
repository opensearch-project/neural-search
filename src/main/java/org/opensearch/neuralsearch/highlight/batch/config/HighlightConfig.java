/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.highlight.batch.config;

import java.util.Collections;
import java.util.List;

import lombok.Builder;
import lombok.Getter;

/**
 * Per-request semantic highlighting configuration produced by {@link HighlightConfigResolver}.
 * Holds the list of semantic targets (top-level fields plus inner_hits fields) and the
 * query text extracted from the request's main query.
 */
@Getter
@Builder(toBuilder = true)
public class HighlightConfig {

    private final List<SemanticHighlightTarget> targets;

    private final String queryText;

    /** True when at least one semantic target was found. */
    public boolean hasTargets() {
        return targets != null && !targets.isEmpty();
    }

    /** Returns the targets list, or empty list when null. */
    public List<SemanticHighlightTarget> getTargetsOrEmpty() {
        return targets == null ? Collections.emptyList() : targets;
    }

    /** Empty config when the request declares no semantic highlight. */
    public static HighlightConfig empty() {
        return HighlightConfig.builder().targets(Collections.emptyList()).queryText(null).build();
    }
}
