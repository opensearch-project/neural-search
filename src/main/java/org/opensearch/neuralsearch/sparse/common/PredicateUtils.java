/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.sparse.common;

import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.SegmentInfo;
import org.opensearch.neuralsearch.sparse.mapper.SparseMethodContext;

import java.util.function.BiPredicate;

public class PredicateUtils {

    public static final BiPredicate<SegmentInfo, FieldInfo> shouldRunSeisPredicate = new BiPredicate<>() {
        @Override
        public boolean test(SegmentInfo segmentInfo, FieldInfo fieldInfo) {
            int clusterUntilDocCountReach = Integer.parseInt(fieldInfo.attributes().get(SparseMethodContext.CLUSTER_UNTIL_FIELD));
            return segmentInfo.maxDoc() >= clusterUntilDocCountReach;
        }
    };
}
