/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.highlight.batch.processor;

import lombok.extern.log4j.Log4j2;
import org.opensearch.action.search.SearchRequest;
import org.opensearch.neuralsearch.highlight.utils.HighlightExtractorUtils;
import org.opensearch.neuralsearch.ml.MLCommonsClientAccessor;
import org.opensearch.search.builder.SearchSourceBuilder;
import org.opensearch.search.fetch.subphase.highlight.HighlightBuilder;
import org.opensearch.search.pipeline.Processor;
import org.opensearch.search.pipeline.ProcessorGenerationContext;
import org.opensearch.search.pipeline.SearchResponseProcessor;
import org.opensearch.search.pipeline.SystemGeneratedProcessor;

import java.util.Map;

/**
 * Factory for creating system-generated semantic highlighting processors.
 * This factory is automatically invoked when semantic highlighting is detected in search requests.
 * Users cannot manually configure this processor - it only works through automatic detection.
 */
@Log4j2
public class SemanticHighlightingFactory implements SystemGeneratedProcessor.SystemGeneratedFactory<SearchResponseProcessor> {

    private final MLCommonsClientAccessor mlClientAccessor;

    public SemanticHighlightingFactory(MLCommonsClientAccessor mlClientAccessor) {
        this.mlClientAccessor = mlClientAccessor;
    }

    @Override
    public boolean shouldGenerate(ProcessorGenerationContext context) {
        SearchRequest request = context.searchRequest();
        if (request == null || request.source() == null) {
            return false;
        }

        SearchSourceBuilder source = request.source();
        HighlightBuilder highlightBuilder = source.highlighter();

        if (highlightBuilder == null || highlightBuilder.fields() == null) {
            return false;
        }

        // Use utility method to check for semantic highlighting field
        String semanticField = HighlightExtractorUtils.extractSemanticField(highlightBuilder);
        return semanticField != null;
    }

    @Override
    public SearchResponseProcessor create(
        Map<String, Processor.Factory<SearchResponseProcessor>> processorFactories,
        String processorTag,
        String description,
        boolean ignoreFailure,
        Map<String, Object> config,
        Processor.PipelineContext pipelineContext
    ) {
        return new SemanticHighlightingProcessor(ignoreFailure, mlClientAccessor);
    }
}
