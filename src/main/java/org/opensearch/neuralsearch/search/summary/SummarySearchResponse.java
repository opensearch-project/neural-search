/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.neuralsearch.search.summary;

import java.io.IOException;

import lombok.extern.log4j.Log4j2;

import org.opensearch.action.search.SearchResponse;
import org.opensearch.action.search.SearchResponseSections;
import org.opensearch.action.search.ShardSearchFailure;
import org.opensearch.common.io.stream.StreamInput;
import org.opensearch.core.xcontent.XContentBuilder;

@Log4j2
public class SummarySearchResponse extends SearchResponse {

    private final ResultsSummary resultsSummary;

    public SummarySearchResponse(StreamInput in) throws IOException {
        super(in);
        resultsSummary = new ResultsSummary();
    }

    public SummarySearchResponse(
        SearchResponseSections internalResponse,
        String scrollId,
        int totalShards,
        int successfulShards,
        int skippedShards,
        long tookInMillis,
        ShardSearchFailure[] shardFailures,
        Clusters clusters,
        ResultsSummary summary
    ) {
        super(internalResponse, scrollId, totalShards, successfulShards, skippedShards, tookInMillis, shardFailures, clusters);
        resultsSummary = summary;

    }

    public SummarySearchResponse(
        SearchResponseSections internalResponse,
        String scrollId,
        int totalShards,
        int successfulShards,
        int skippedShards,
        long tookInMillis,
        ShardSearchFailure[] shardFailures,
        Clusters clusters,
        String pointInTimeId
    ) {
        super(
            internalResponse,
            scrollId,
            totalShards,
            successfulShards,
            skippedShards,
            tookInMillis,
            shardFailures,
            clusters,
            pointInTimeId
        );
        resultsSummary = new ResultsSummary();
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        innerToXContent(builder, params);
        resultsSummary.toXContent(builder, params);
        builder.endObject();
        return builder;
    }

}
