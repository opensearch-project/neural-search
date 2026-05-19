/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.highlight.batch.config;

import java.util.Map;

import lombok.Builder;
import lombok.Getter;

/**
 * One {@code type: semantic} field declaration discovered while walking a search
 * request's highlight DSL. For top-level targets {@link #nestedPath} and
 * {@link #innerHitsBucketName} are null; for fields declared inside an
 * {@code inner_hits} block they are captured from the enclosing
 * {@code NestedQueryBuilder} and {@code InnerHitBuilder}.
 */
@Getter
@Builder
public class SemanticHighlightTarget {

    private final String fieldName;

    private final String nestedPath;

    private final String innerHitsBucketName;

    private final Map<String, Object> options;

    private final String preTag;

    private final String postTag;

    /**
     * @return true when this target was discovered inside an {@code inner_hits} block
     */
    public boolean isNested() {
        return nestedPath != null;
    }
}
