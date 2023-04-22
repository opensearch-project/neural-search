/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.neuralsearch.processor;

import java.util.List;
import java.util.Locale;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.extern.log4j.Log4j2;

import org.apache.commons.lang.StringUtils;
import org.opensearch.action.search.SearchRequest;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.neuralsearch.ml.MLCommonsClientAccessor;
import org.opensearch.neuralsearch.search.summary.ResultsSummary;
import org.opensearch.neuralsearch.search.summary.SummarySearchResponse;
import org.opensearch.search.SearchHit;
import org.opensearch.search.pipeline.SearchResponseProcessor;

@Log4j2
public class SummaryProcessor extends AbstractProcessor implements SearchResponseProcessor {

    public static final String TYPE = "summary_processor";
    private final List<String> fields;
    private final MLCommonsClientAccessor clientAccessor;
    private final String modelId;
    private final Prompt prompt;

    public SummaryProcessor(
        final String tag,
        final String description,
        final MLCommonsClientAccessor mlCommonsClientAccessor,
        final List<String> fields,
        final String modelId,
        final String promptType
    ) {
        super(description, tag);
        this.clientAccessor = mlCommonsClientAccessor;
        this.fields = fields;
        this.modelId = modelId;
        if (promptType == null) {
            prompt = Prompt.SUMMARY;
        } else {
            prompt = Prompt.valueOf(promptType.toUpperCase(Locale.ROOT));
        }
    }

    @Override
    public SearchResponse processResponse(SearchRequest searchRequest, SearchResponse searchResponse) {
        final ResultsSummary summary = createSummary(searchRequest, searchResponse);
        return new SummarySearchResponse(
            searchResponse.getInternalResponse(),
            searchResponse.getScrollId(),
            searchResponse.getTotalShards(),
            searchResponse.getSuccessfulShards(),
            searchResponse.getSkippedShards(),
            searchResponse.getTook().millis(),
            searchResponse.getShardFailures(),
            searchResponse.getClusters(),
            summary
        );
    }

    /**
     * Gets the type of processor
     */
    @Override
    public String getType() {
        return TYPE;
    }

    private ResultsSummary createSummary(SearchRequest searchRequest, SearchResponse searchResponse) {
        final String prompt = createPromptForLLM(searchRequest, searchResponse);
        try {
            log.info("Calling the Model {} with a prompt {}", modelId, prompt);
            String summary = clientAccessor.predict(prompt, modelId);
            return new ResultsSummary(summary, StringUtils.EMPTY);
        } catch (Exception e) {
            log.error("Error while calling ML Commons Predict API, ", e);
            return new ResultsSummary(StringUtils.EMPTY, "Error Happened while calling the Summary Response. " + e.getMessage());
        }
    }

    private String createPromptForLLM(SearchRequest searchRequest, SearchResponse searchResponse) {
        final StringBuilder promptBuilder = new StringBuilder();
        createContextForPromptUsingSearchResponse(promptBuilder, searchResponse);
        return prompt.createPrompt(promptBuilder, searchRequest);
    }

    private void createContextForPromptUsingSearchResponse(final StringBuilder promptBuilder, final SearchResponse searchResponse) {
        for (final SearchHit hit : searchResponse.getInternalResponse().hits()) {
            for (String field : fields) {
                if (hit.getSourceAsMap().get(field) != null) {
                    promptBuilder.append(hit.getSourceAsMap().get(field)).append("\\n");
                }
            }
        }
    }

    @AllArgsConstructor
    @Getter
    private enum Prompt {
        SUMMARY("summary", "\\n        Summarize the above input for me. \\n"),
        QUESTION("question", "By considering " + "above input from me, answer the \\n question: ${question}") {
            public String createPrompt(final StringBuilder context, SearchRequest searchRequest) {
                // Find a way in which we can get the query produced by a user
                final String queryString = searchRequest.source().query().toString().replace("\"", "\\\"");
                final String updatedPrompt = getPrompt().replace("${question}", queryString);
                return context.insert(0, "\"").append(updatedPrompt).append("\"").toString();
            }
        };

        private final String name;
        private final String prompt;

        public String createPrompt(final StringBuilder context, SearchRequest searchRequest) {
            return context.insert(0, "\"").append(prompt).append("\"").toString();
        }
    }

}
