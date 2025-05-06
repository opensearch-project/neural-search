/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.sparse.common;

import lombok.EqualsAndHashCode;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.SegmentInfo;

/**
 * Key for in-memory sparse vector forward index
 */
public class InMemoryKey {

    @EqualsAndHashCode
    public static class IndexKey {
        private final SegmentInfo segmentInfo;
        private final String field;

        public IndexKey(SegmentInfo segmentInfo, FieldInfo fieldInfo) {
            this.segmentInfo = segmentInfo;
            this.field = fieldInfo.name;
        }

        public IndexKey(SegmentInfo segmentInfo, String fieldName) {
            this.segmentInfo = segmentInfo;
            this.field = fieldName;
        }
    }
}
