/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.search.util;

import org.apache.lucene.search.SortField;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.grouping.CollapseTopFieldDocs;
import org.apache.lucene.util.BytesRef;
import org.opensearch.neuralsearch.processor.CompoundTopDocs;

import java.util.List;
import java.util.Objects;

/**
 * Utility class for handling collapse field operations in hybrid search scenarios.
 */
public class HybridSearchCollapseUtil {

    /**
     * Determines the data type of collapse field values from hybrid search results.
     *
     * @param queryTopDocs List of {@link CompoundTopDocs} containing results from all segments and sub-queries
     *                     in the hybrid search. Each {@link CompoundTopDocs} represents results from one
     *                     segment/collector, containing multiple {@link TopDocs} (one per sub-query).
     *                     Can be null or contain null entries.
     *
     * @return The {@link Class} type of the collapse field values:
     *         <ul>
     *           <li>{@link BytesRef}.class for keyword/text fields</li>
     *           <li>{@link Long}.class for numeric fields</li>
     *           <li>{@code null} if no collapse values found, unsupported type, or null input</li>
     *         </ul>
     *
     * @throws ClassCastException if any {@link TopDocs} in the input cannot be cast to {@link CollapseTopFieldDocs}.
     *                           This should not happen in normal hybrid search with collapse enabled.
     */
    public static Class<?> getCollapseFieldType(final List<CompoundTopDocs> queryTopDocs) {
        // Handle null input
        if (Objects.isNull(queryTopDocs) || queryTopDocs.isEmpty()) {
            throw new IllegalStateException("queryTopDocs is null or empty");
        }

        /*
           First collapse value will be initialized as null. Later if the field is not present in the CollapseTopFieldDoc then this values stays null and the document will grouped under null group.
        */
        Object collapseValue = null;
        outer: for (CompoundTopDocs queryTopDoc : queryTopDocs) {
            // Handle null CompoundTopDocs entries
            if (queryTopDoc == null) {
                continue;
            }

            List<TopDocs> topDocsList = queryTopDoc.getTopDocs();
            if (topDocsList != null && !topDocsList.isEmpty()) {
                for (TopDocs topDocs : topDocsList) {
                    assert topDocs instanceof CollapseTopFieldDocs;
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
            // Keyword Type
            return BytesRef.class;
        } else if (collapseValue instanceof Long) {
            // Numeric Type
            return Long.class;
        }
        // null will be returned when the field on which collapse is applied is absent from the document.
        return null;
    }

    /**
     * Whether {@code _score} is the primary (first) sort key. Hybrid score handling assumes the score occupies
     * sort slot 0 — the collapse score-slot swap ({@code fields[0] = normalizedScore}) and min-score gating both
     * rely on it — so this is the shared "is this a score-primary sort" predicate.
     *
     * @param sortFields the sort keys (may be {@code null}/empty)
     * @return {@code true} iff the first sort key is {@link SortField.Type#SCORE}
     */
    public static boolean isScorePrimarySort(final SortField[] sortFields) {
        return sortFields != null && sortFields.length > 0 && SortField.Type.SCORE.equals(sortFields[0].getType());
    }

    /**
     * Whether {@code _score} is the primary sort key AND descending. For a SCORE sort field {@code reverse == false}
     * is the natural highest-first (descending) order; OpenSearch maps {@code _score} order:asc to {@code reverse == true}.
     * The hybrid+collapse relaxation accepts only this form: the collapse collector's min-competitive-score pruning
     * ({@code minScoreThresholds} / {@code HybridSubQueryScorer#getMinScores}) assumes score-descending and does not
     * consult the reverse flag, so an ascending {@code _score} could prune competitive docs and yield a wrong group head.
     *
     * @param sortFields the sort keys (may be {@code null}/empty)
     * @return {@code true} iff the first sort key is a descending {@link SortField.Type#SCORE}
     */
    public static boolean isScorePrimaryDescendingSort(final SortField[] sortFields) {
        return isScorePrimarySort(sortFields) && !sortFields[0].getReverse();
    }
}
