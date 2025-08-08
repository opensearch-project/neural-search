/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.sparse.common;

import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.SegmentInfo;

import java.util.function.BiPredicate;

import static org.opensearch.neuralsearch.sparse.common.SparseConstants.APPROXIMATE_THRESHOLD_FIELD;

/**
 * Utility class containing predicates for sparse search operations.
 */
public class PredicateUtils {

    /**
     * Predicate to determine if SEISMIC (Sparse Encoding Index Structure) should run
     * based on segment document count and field threshold.
     */
    public static final BiPredicate<SegmentInfo, FieldInfo> shouldRunSeisPredicate = new BiPredicate<>() {
        /**
         * Tests if the segment has enough documents to warrant running SEISMIC.
         *
         * @param segmentInfo the segment information
         * @param fieldInfo the field information containing threshold attributes
         * @return true if segment document count meets or exceeds the threshold
         */
        @Override
        public boolean test(SegmentInfo segmentInfo, FieldInfo fieldInfo) {
            int clusterUntilDocCountReach = Integer.parseInt(fieldInfo.attributes().get(APPROXIMATE_THRESHOLD_FIELD));
            return segmentInfo.maxDoc() >= clusterUntilDocCountReach;
        }
    };
}
