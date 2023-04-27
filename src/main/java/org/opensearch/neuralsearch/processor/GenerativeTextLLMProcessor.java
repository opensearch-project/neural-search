/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.neuralsearch.processor;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.extern.log4j.Log4j2;

import org.apache.commons.lang.StringUtils;
import org.opensearch.OpenSearchException;
import org.opensearch.action.search.SearchRequest;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.neuralsearch.ext.QuestionExtBuilder;
import org.opensearch.neuralsearch.ml.MLCommonsClientAccessor;
import org.opensearch.neuralsearch.search.summary.GeneratedText;
import org.opensearch.neuralsearch.search.summary.GenerativeTextLLMSearchResponse;
import org.opensearch.search.SearchExtBuilder;
import org.opensearch.search.SearchHit;
import org.opensearch.search.pipeline.SearchResponseProcessor;

@Log4j2
public class GenerativeTextLLMProcessor extends AbstractProcessor implements SearchResponseProcessor {

    public static final String TYPE = "llm_processor";
    private final List<String> fields;
    private final MLCommonsClientAccessor clientAccessor;
    private final String modelId;
    private final ContextType contextType;

    public GenerativeTextLLMProcessor(
        final String tag,
        final String description,
        final MLCommonsClientAccessor mlCommonsClientAccessor,
        final List<String> fields,
        final String modelId,
        final String usecase
    ) {
        super(description, tag);
        this.clientAccessor = mlCommonsClientAccessor;
        this.fields = fields;
        this.modelId = modelId;
        this.contextType = usecase == null ? ContextType.SUMMARY : ContextType.valueOf(usecase.toUpperCase(Locale.ROOT));
    }

    @Override
    public SearchResponse processResponse(SearchRequest searchRequest, SearchResponse searchResponse) {
        final GeneratedText generatedText = generateTextFromLLM(searchRequest, searchResponse);
        generatedText.setProcessorTag(getTag());
        generatedText.setUsecase(contextType.name);
        List<GeneratedText> generatedTexts = new ArrayList<>();
        if (searchResponse instanceof GenerativeTextLLMSearchResponse) {
            List<GeneratedText> generatedTextList = ((GenerativeTextLLMSearchResponse) searchResponse).getGeneratedTextList();
            generatedTexts.addAll(generatedTextList);
        }
        generatedTexts.add(generatedText);

        return new GenerativeTextLLMSearchResponse(
            searchResponse.getInternalResponse(),
            searchResponse.getScrollId(),
            searchResponse.getTotalShards(),
            searchResponse.getSuccessfulShards(),
            searchResponse.getSkippedShards(),
            searchResponse.getTook().millis(),
            searchResponse.getShardFailures(),
            searchResponse.getClusters(),
            generatedTexts
        );
    }

    /**
     * Gets the type of processor
     */
    @Override
    public String getType() {
        return TYPE;
    }

    private GeneratedText generateTextFromLLM(SearchRequest searchRequest, SearchResponse searchResponse) {
        final String context = createContextForLLM(searchRequest, searchResponse);
        try {
            log.info("Calling the Model {} with a context {}", modelId, context);
            return clientAccessor.predict(context, modelId);
        } catch (Exception e) {
            log.error("Error while calling ML Commons Predict API for context: {}", context, e);
            return new GeneratedText(
                StringUtils.EMPTY,
                String.format(
                    Locale.ROOT,
                    "Error Happened while calling the Predict API for model : %s with context: %s. Error is: %s",
                    modelId,
                    context,
                    e.getMessage()
                )
            );
        }
    }

    private String createContextForLLM(SearchRequest searchRequest, SearchResponse searchResponse) {
        final StringBuilder contextBuilder = new StringBuilder();
        createContextForPromptUsingSearchResponse(contextBuilder, searchResponse);
        return contextType.createContext(contextBuilder, searchRequest);
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
    private enum ContextType {
        SUMMARY("summary", "\\nSummarize the above input for me. \\n"),
        QANDA("QandA", "By considering above input from me, answer the question: ${question}") {
            public String createContext(final StringBuilder contextBuilder, SearchRequest searchRequest) {
                final List<SearchExtBuilder> extBuilders = searchRequest.source().ext();
                String questionString = "";
                for (SearchExtBuilder builder : extBuilders) {
                    if (builder instanceof QuestionExtBuilder) {
                        questionString = ((QuestionExtBuilder) builder).getQuestion();
                    }
                }
                if (StringUtils.isEmpty(questionString)) {
                    throw new OpenSearchException("Not able to get question string from Ext Builder list: " + extBuilders);
                }

                final String updatedPrompt = getContext().replace("${question}", questionString);
                return contextBuilder.insert(0, "\"").append(updatedPrompt).append("\"").toString();
            }
        };

        private final String name;
        private final String context;

        public String createContext(final StringBuilder contextBuilder, SearchRequest searchRequest) {
            return contextBuilder.insert(0, "\"").append(this.context).append("\"").toString();
        }
    }

}
