/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.sparse.cache;

import lombok.EqualsAndHashCode;
import lombok.NonNull;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.SegmentInfo;

/**
 * Key for cache sparse vector forward index and clustered posting
 */
@EqualsAndHashCode
public class CacheKey {

    private final SegmentInfo segmentInfo;
    private final String field;

    public CacheKey(@NonNull SegmentInfo segmentInfo, @NonNull FieldInfo fieldInfo) {
        this.segmentInfo = segmentInfo;
        this.field = fieldInfo.name;
    }

    public CacheKey(@NonNull SegmentInfo segmentInfo, @NonNull String fieldName) {
        this.segmentInfo = segmentInfo;
        this.field = fieldName;
    }
}
