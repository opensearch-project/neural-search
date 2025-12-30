/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.grpc.proto.request.search.query;

import org.opensearch.index.query.QueryBuilder;
import org.opensearch.transport.grpc.spi.QueryBuilderProtoConverter;
import org.opensearch.transport.grpc.spi.QueryBuilderProtoConverterRegistry;
import org.opensearch.protobufs.QueryContainer;

/**
 * Converter for Hybrid queries.
 * This class implements the QueryBuilderProtoConverter interface to provide Hybrid query support
 * for the gRPC transport plugin from the neural-search plugin.
 */
public class HybridQueryBuilderProtoConverter implements QueryBuilderProtoConverter {

    private QueryBuilderProtoConverterRegistry registry;

    @Override
    public void setRegistry(QueryBuilderProtoConverterRegistry registry) {
        this.registry = registry;
    }

    @Override
    public QueryContainer.QueryContainerCase getHandledQueryCase() {
        return QueryContainer.QueryContainerCase.HYBRID;
    }

    @Override
    public QueryBuilder fromProto(QueryContainer queryContainer) {
        if (queryContainer == null || queryContainer.getQueryContainerCase() != QueryContainer.QueryContainerCase.HYBRID) {
            throw new IllegalArgumentException("QueryContainer does not contain a Hybrid query");
        }

        return HybridQueryBuilderProtoUtils.fromProto(queryContainer.getHybrid(), registry);
    }
}
