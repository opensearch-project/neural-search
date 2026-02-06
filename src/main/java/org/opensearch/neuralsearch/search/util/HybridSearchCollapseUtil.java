/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.search.util;

import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.grouping.CollapseTopFieldDocs;
import org.apache.lucene.util.BytesRef;
import org.opensearch.neuralsearch.processor.CompoundTopDocs;

import java.util.List;

public class HybridSearchCollapseUtil {

    public static Class<?> getCollapseFieldType(final List<CompoundTopDocs> queryTopDocs) {
        /*
           First collapse value will be initialized as null. Later if the field is not present in the CollapseTopFieldDoc then this values stays null and the document will grouped under null group.
        */
        Object collapseValue = null;
        outer: for (CompoundTopDocs queryTopDoc : queryTopDocs) {
            List<TopDocs> topDocsList = queryTopDoc.getTopDocs();
            if (!topDocsList.isEmpty()) {
                for (TopDocs topDocs : topDocsList) {
                    CollapseTopFieldDocs collapseTopFieldDocs = (CollapseTopFieldDocs) topDocs;
                    // In case of multiple subqueries, if the subquery does not have result then it will have CollapseTopFieldDoc with empty
                    // collapseValues.
                    // Therefore, we need to find the collapseValues of a result which has collapseValues.
                    if (collapseTopFieldDocs.collapseValues != null && collapseTopFieldDocs.collapseValues.length > 0) {
                        collapseValue = collapseTopFieldDocs.collapseValues[0];
                        break outer;
                    }
                }
            }
        }
        if (collapseValue instanceof BytesRef) {
            return BytesRef.class;
        } else if (collapseValue instanceof Long) {
            return Long.class;
        }
        return null;
    }
}
