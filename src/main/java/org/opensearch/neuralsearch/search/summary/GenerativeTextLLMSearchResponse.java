/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.neuralsearch.search.summary;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import lombok.Getter;
import lombok.Setter;
import lombok.extern.log4j.Log4j2;

import org.opensearch.action.search.SearchResponse;
import org.opensearch.action.search.SearchResponseSections;
import org.opensearch.action.search.ShardSearchFailure;
import org.opensearch.common.io.stream.StreamInput;
import org.opensearch.common.io.stream.StreamOutput;
import org.opensearch.core.ParseField;
import org.opensearch.core.xcontent.XContentBuilder;

@Log4j2
public class GenerativeTextLLMSearchResponse extends SearchResponse {

    @Getter
    @Setter
    private List<GeneratedText> generatedTextList;

    private static final ParseField GENERATED_TEXT = new ParseField("generatedText");

    public GenerativeTextLLMSearchResponse(StreamInput in) throws IOException {
        super(in);
        generatedTextList = in.readList(GeneratedText::new);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeList(generatedTextList);
    }

    public GenerativeTextLLMSearchResponse(
        SearchResponseSections internalResponse,
        String scrollId,
        int totalShards,
        int successfulShards,
        int skippedShards,
        long tookInMillis,
        ShardSearchFailure[] shardFailures,
        Clusters clusters,
        List<GeneratedText> generatedTextList
    ) {
        super(internalResponse, scrollId, totalShards, successfulShards, skippedShards, tookInMillis, shardFailures, clusters);
        this.generatedTextList = new ArrayList<>();
        this.generatedTextList.addAll(generatedTextList);
    }

    public GenerativeTextLLMSearchResponse(
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
        this.generatedTextList = new ArrayList<>();
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.startArray(GENERATED_TEXT.getPreferredName());
        for (GeneratedText generatedText : generatedTextList) {
            generatedText.toXContent(builder, params);
        }
        builder.endArray();
        innerToXContent(builder, params);
        builder.endObject();
        return builder;
    }

}
