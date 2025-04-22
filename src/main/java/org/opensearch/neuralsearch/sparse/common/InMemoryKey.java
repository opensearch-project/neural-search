/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.sparse.common;

import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.SegmentInfo;

/**
 * Key for in-memory sparse vector forward index
 */
@AllArgsConstructor(access = AccessLevel.PRIVATE)
public class InMemoryKey {
    public static class IndexKey {
        private final int hashCode;

        public IndexKey(SegmentInfo segmentInfo, FieldInfo fieldInfo) {
            this.hashCode = segmentInfo.hashCode() + fieldInfo.name.hashCode();
        }

        @Override
        public int hashCode() {
            return this.hashCode;
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj) return true;
            if (obj == null || getClass() != obj.getClass()) return false;

            return this.hashCode() == obj.hashCode();
        }
    }
}
