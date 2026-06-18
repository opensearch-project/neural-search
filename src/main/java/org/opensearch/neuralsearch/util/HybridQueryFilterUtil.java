/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.util;

import lombok.experimental.UtilityClass;
import org.opensearch.index.query.AbstractQueryBuilder;
import org.opensearch.index.query.NestedQueryBuilder;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.knn.index.query.KNNQueryBuilder;
import org.opensearch.neuralsearch.query.NeuralKNNQueryBuilder;
import org.opensearch.neuralsearch.query.NeuralQueryBuilder;

/**
 * Utility for applying hybrid query filters to sub-queries.
 */
@UtilityClass
public class HybridQueryFilterUtil {

    /**
     * Applies a hybrid filter to a sub-query. For nested queries whose inner query supports efficient
     * vector pre-filtering, the filter is pushed down to the inner query so the kNN engine can pre-filter
     * candidates. Other queries use the default {@link QueryBuilder#filter(QueryBuilder)} behavior.
     *
     * @param query sub-query to apply the filter to
     * @param filter hybrid filter to apply
     * @return query with the filter applied
     */
    public static QueryBuilder applyFilterToSubQuery(QueryBuilder query, QueryBuilder filter) {
        if (AbstractQueryBuilder.validateFilterParams(filter) == false) {
            return query;
        }
        if (query instanceof NestedQueryBuilder nestedQuery && supportsVectorPreFilter(nestedQuery.query())) {
            QueryBuilder filteredInnerQuery = applyFilterToSubQuery(nestedQuery.query(), filter);
            return copyNestedQueryBuilder(nestedQuery, filteredInnerQuery);
        }
        return query.filter(filter);
    }

    private static boolean supportsVectorPreFilter(QueryBuilder query) {
        return query instanceof NeuralQueryBuilder || query instanceof KNNQueryBuilder || query instanceof NeuralKNNQueryBuilder;
    }

    private static NestedQueryBuilder copyNestedQueryBuilder(NestedQueryBuilder nestedQuery, QueryBuilder innerQuery) {
        NestedQueryBuilder copy = new NestedQueryBuilder(nestedQuery.path(), innerQuery, nestedQuery.scoreMode());
        if (nestedQuery.innerHit() != null) {
            copy.innerHit(nestedQuery.innerHit());
        }
        if (nestedQuery.ignoreUnmapped()) {
            copy.ignoreUnmapped(true);
        }
        copy.boost(nestedQuery.boost());
        copy.queryName(nestedQuery.queryName());
        return copy;
    }
}
