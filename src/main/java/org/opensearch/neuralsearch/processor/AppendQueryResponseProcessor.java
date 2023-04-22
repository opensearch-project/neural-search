/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.neuralsearch.processor;

import java.util.List;
import java.util.Map;

import lombok.AllArgsConstructor;

import org.opensearch.action.search.SearchRequest;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.common.document.DocumentField;
import org.opensearch.search.SearchHit;
import org.opensearch.search.SearchHits;
import org.opensearch.search.pipeline.Processor;
import org.opensearch.search.pipeline.SearchResponseProcessor;

public class AppendQueryResponseProcessor extends AbstractProcessor implements SearchResponseProcessor {
    public static final String TYPE = "append_query";

    public AppendQueryResponseProcessor(final String tag, final String description) {
        super(description, tag);
    }

    @Override
    public String getType() {
        return TYPE;
    }

    @Override
    public SearchResponse processResponse(SearchRequest request, SearchResponse response) {
        if (response.getHits() != null && response.getHits().getHits().length > 0 && request.source() != null) {
            SearchHits hits = response.getHits();
            SearchHit searchHit = hits.getAt(0);
            DocumentField queryField = new DocumentField("query", List.of(request.source().query().toString()));
            searchHit.setDocumentField("query", queryField);
        }
        return response;
    }

    @AllArgsConstructor
    public static class Factory implements Processor.Factory {

        @Override
        public Processor create(
            Map<String, Processor.Factory> processorFactories,
            String tag,
            String description,
            Map<String, Object> config
        ) throws Exception {
            return new AppendQueryResponseProcessor(tag, description);
        }
    }
}
