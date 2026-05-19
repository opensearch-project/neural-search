/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.highlight.batch.processor;

import java.util.List;
import java.util.Map;

import lombok.extern.log4j.Log4j2;
import org.opensearch.action.search.SearchRequest;
import org.opensearch.neuralsearch.highlight.batch.config.HighlightConfig;
import org.opensearch.neuralsearch.highlight.batch.config.HighlightConfigResolver;
import org.opensearch.neuralsearch.highlight.utils.HighlightExtractorUtils;
import org.opensearch.neuralsearch.ml.MLCommonsClientAccessor;
import org.opensearch.neuralsearch.query.ext.SemanticHighlighterExtBuilder;
import org.opensearch.search.SearchExtBuilder;
import org.opensearch.search.builder.SearchSourceBuilder;
import org.opensearch.search.fetch.subphase.highlight.HighlightBuilder;
import org.opensearch.search.pipeline.Processor;
import org.opensearch.search.pipeline.ProcessorGenerationContext;
import org.opensearch.search.pipeline.SearchResponseProcessor;
import org.opensearch.search.pipeline.SystemGeneratedProcessor;

/**
 * Factory for the system-generated semantic highlighting processor.
 *
 * <p>The factory uses cheap top-level checks to decide whether the request is
 * a candidate for batch semantic highlighting before walking the query tree.
 * The full walk only happens when one of the following is true:
 * <ul>
 *   <li>the request carries the {@code ext.semantic_highlighting_batch: true} block, or</li>
 *   <li>the top-level highlight block declares any {@code type: semantic} field
 *       (legacy path — already covers requests that use {@code batch_inference: true}
 *       on a top-level field).</li>
 * </ul>
 * Requests with neither signal short-circuit on the very first check, keeping
 * the gate cheap for every search that does not use semantic highlighting.
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

        // Cheap candidate check: ext opt-in OR a top-level type: semantic field.
        // If neither is present, we must not pay the cost of walking the query tree.
        boolean extOptedIn = isExtBatchEnabled(source.ext());
        boolean topLevelSemanticPresent = hasTopLevelSemanticField(source.highlighter());
        if (!extOptedIn && !topLevelSemanticPresent) {
            return false;
        }

        // The request is a candidate. Resolve all targets — including those declared
        // inside inner_hits anywhere in the query tree.
        HighlightConfig config = HighlightConfigResolver.resolve(request);
        return config.hasTargets();
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

    private static boolean isExtBatchEnabled(List<SearchExtBuilder> exts) {
        if (exts == null || exts.isEmpty()) {
            return false;
        }
        for (SearchExtBuilder b : exts) {
            if (b instanceof SemanticHighlighterExtBuilder && ((SemanticHighlighterExtBuilder) b).isEnabled()) {
                return true;
            }
        }
        return false;
    }

    private static boolean hasTopLevelSemanticField(HighlightBuilder highlighter) {
        if (highlighter == null) {
            return false;
        }
        return HighlightExtractorUtils.extractSemanticField(highlighter) != null;
    }
}
