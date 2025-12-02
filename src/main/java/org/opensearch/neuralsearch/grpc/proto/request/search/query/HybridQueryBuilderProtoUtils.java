/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.grpc.proto.request.search.query;

import lombok.experimental.UtilityClass;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.neuralsearch.query.HybridQueryBuilder;
import org.opensearch.transport.grpc.spi.QueryBuilderProtoConverterRegistry;
import org.opensearch.protobufs.HybridQuery;
import org.opensearch.protobufs.QueryContainer;

import java.util.List;
import java.util.Locale;

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
        HybridQueryBuilder hybridQueryBuilder = new HybridQueryBuilder();

        if (hybridQueryProto.getQueriesCount() > 0) {
            List<QueryContainer> queriesProto = hybridQueryProto.getQueriesList();

            if (queriesProto.size() > HybridQueryBuilder.MAX_NUMBER_OF_SUB_QUERIES) {
                throw new IllegalArgumentException(
                    String.format(Locale.ROOT, "Number of sub-queries exceeds maximum supported by [%s] query", HybridQueryBuilder.NAME)
                );
            }

            for (QueryContainer queryContainer : queriesProto) {
                QueryBuilder queryBuilder = registry.fromProto(queryContainer);
                if (queryBuilder != null) {
                    hybridQueryBuilder.add(queryBuilder);
                }
            }
        }

        if (hybridQueryBuilder.queries().isEmpty()) {
            throw new IllegalArgumentException(
                String.format(Locale.ROOT, "[%s] requires 'queries' field with at least one clause", HybridQueryBuilder.NAME)
            );
        }

        if (hybridQueryProto.hasFilter()) {
            QueryBuilder filterBuilder = registry.fromProto(hybridQueryProto.getFilter());
            if (filterBuilder != null) {
                hybridQueryBuilder.filter(filterBuilder);
            }
        }

        if (hybridQueryProto.hasPaginationDepth()) {
            hybridQueryBuilder.paginationDepth(hybridQueryProto.getPaginationDepth());
        }

        if (hybridQueryProto.hasBoost()) {
            float boost = hybridQueryProto.getBoost();
            if (boost != HybridQueryBuilder.DEFAULT_BOOST) {
                throw new IllegalArgumentException(
                    String.format(Locale.ROOT, "[%s] query does not support [%s]", HybridQueryBuilder.NAME, "boost")
                );
            }
            hybridQueryBuilder.boost(boost);
        }

        if (hybridQueryProto.hasXName()) {
            hybridQueryBuilder.queryName(hybridQueryProto.getXName());
        }

        return hybridQueryBuilder;
    }
}
