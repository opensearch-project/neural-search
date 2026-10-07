/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.grpc.proto.request.search.query;

import lombok.experimental.UtilityClass;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.index.query.InnerHitContextBuilder;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.neuralsearch.query.HybridQueryBuilder;
import org.opensearch.transport.grpc.spi.QueryBuilderProtoConverterRegistry;
import org.opensearch.protobufs.HybridQuery;
import org.opensearch.protobufs.QueryContainer;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import static org.opensearch.neuralsearch.common.MinClusterVersionUtil.isClusterOnOrAfterMinReqVersionForPaginationInHybridQuery;

/**
 * Utility class for converting HybridQuery Protocol Buffers to OpenSearch objects.
 * This class provides methods to transform Protocol Buffer representations of hybrid queries
 * into their corresponding OpenSearch HybridQueryBuilder implementations for search operations.
 */
@UtilityClass
public class HybridQueryBuilderProtoUtils {

    /**
     * Converts a Protocol Buffer HybridQuery to an OpenSearch HybridQueryBuilder.
     * This method follows the exact same pattern as {@link HybridQueryBuilder#fromXContent(XContentParser)}
     * to ensure parsing consistency and compatibility with the REST API side.
     *
     * @param hybridQueryProto The Protocol Buffer HybridQuery to convert
     * @param registry The registry to use for converting nested queries
     * @return A configured HybridQueryBuilder instance
     */
    public QueryBuilder fromProto(HybridQuery hybridQueryProto, QueryBuilderProtoConverterRegistry registry) {
        float boost = HybridQueryBuilder.DEFAULT_BOOST;

        Integer paginationDepth = null;
        final List<QueryBuilder> queries = new ArrayList<>();
        QueryBuilder filter = null;
        String queryName = null;

        for (QueryContainer queryContainer : hybridQueryProto.getQueriesList()) {
            if (queries.size() == HybridQueryBuilder.MAX_NUMBER_OF_SUB_QUERIES) {
                throw new IllegalArgumentException(
                    String.format(Locale.ROOT, HybridQueryBuilder.ERROR_MSG_MAX_QUERIES_EXCEEDED, HybridQueryBuilder.NAME)
                );
            }
            QueryBuilder queryBuilder = parseInnerQueryBuilder(queryContainer, registry);
            if (queryBuilder != null) {
                queries.add(queryBuilder);
            }
        }

        if (hybridQueryProto.hasFilter()) {
            filter = parseInnerQueryBuilder(hybridQueryProto.getFilter(), registry);
        }

        if (hybridQueryProto.hasBoost()) {
            boost = hybridQueryProto.getBoost();
            if (boost != HybridQueryBuilder.DEFAULT_BOOST) {
                throw new IllegalArgumentException(
                    String.format(Locale.ROOT, HybridQueryBuilder.ERROR_MSG_BOOST_NOT_SUPPORTED, HybridQueryBuilder.NAME, "boost")
                );
            }
        }

        if (hybridQueryProto.hasXName()) {
            queryName = hybridQueryProto.getXName();
        }

        if (hybridQueryProto.hasPaginationDepth()) {
            paginationDepth = hybridQueryProto.getPaginationDepth();
        }

        if (queries.isEmpty()) {
            throw new IllegalArgumentException(
                String.format(Locale.ROOT, HybridQueryBuilder.ERROR_MSG_QUERIES_REQUIRED, HybridQueryBuilder.NAME)
            );
        }

        HybridQueryBuilder compoundQueryBuilder = new HybridQueryBuilder();
        compoundQueryBuilder.queryName(queryName);
        compoundQueryBuilder.boost(boost);
        if (isClusterOnOrAfterMinReqVersionForPaginationInHybridQuery()) {
            compoundQueryBuilder.paginationDepth(paginationDepth);
        }

        boolean hasInnerHits = false;
        for (QueryBuilder query : queries) {
            if (filter == null) {
                compoundQueryBuilder.add(query);
            } else {
                compoundQueryBuilder.add(query.filter(filter));
            }

            if (hasInnerHits == false) {
                Map<String, InnerHitContextBuilder> innerHits = new HashMap<>();
                InnerHitContextBuilder.extractInnerHits(query, innerHits);
                hasInnerHits = innerHits.isEmpty() == false;
            }
        }

        boolean hasFilter = filter != null;
        boolean hasPagination = paginationDepth != null;
        // Always false, and correctly so rather than as a stub: the generated `HybridQuery` message has no `fusion` field
        // (protobufs 1.7.0 carries boost, _name, queries, pagination_depth, filter), so resolver mode is not expressible
        // over gRPC at all. The counter therefore reads 0 for gRPC traffic because the feature is unreachable there, not
        // because it is unused -- worth saying wherever the number is presented. Supporting it needs a `fusion` field added
        // to opensearch-protobufs upstream first.
        HybridQueryBuilder.updateQueryStats(hasFilter, hasPagination, hasInnerHits, false);
        return compoundQueryBuilder;
    }

    /**
     * Parses an inner query from a Protocol Buffer QueryContainer.
     * This is the Proto equivalent of AbstractQueryBuilder.parseInnerQueryBuilder(XContentParser).
     *
     * @param queryContainer The Protocol Buffer QueryContainer to parse
     * @param registry The registry to use for converting the query
     * @return The parsed QueryBuilder
     */
    private QueryBuilder parseInnerQueryBuilder(QueryContainer queryContainer, QueryBuilderProtoConverterRegistry registry) {
        return registry.fromProto(queryContainer);
    }
}
