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
import org.opensearch.neuralsearch.highlight.batch.config.SemanticHighlightTarget;
import org.opensearch.neuralsearch.highlight.utils.HighlightExtractorUtils;
import org.opensearch.neuralsearch.ml.MLCommonsClientAccessor;
import org.opensearch.neuralsearch.query.ext.SemanticHighlighterExtBuilder;
import org.opensearch.search.SearchExtBuilder;
import org.opensearch.search.builder.SearchSourceBuilder;
import org.opensearch.search.pipeline.Processor;
import org.opensearch.search.pipeline.ProcessorGenerationContext;
import org.opensearch.search.pipeline.SearchResponseProcessor;
import org.opensearch.search.pipeline.SystemGeneratedProcessor;

/**
 * Factory for the system-generated semantic highlighting processor.
 *
 * <p>The factory only generates a processor when the request opts into batch
 * semantic highlighting via one of:
 * <ul>
 *   <li>the request-level {@code ext.semantic_highlighting_batch: true} block, or</li>
 *   <li>any {@code type: semantic} field that declares {@code batch_inference: true}
 *       (legacy field-level signal).</li>
 * </ul>
 * Requests that declare {@code type: semantic} without either signal continue to
 * be handled synchronously by {@code SemanticHighlighter} during the fetch phase,
 * so this gate stays symmetric with the fetch-phase yield condition.
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

        // Short-circuit: if the ext opt-in is absent and the top-level highlight
        // does not declare any type: semantic field, no batch path applies. We must
        // not walk the full query tree on every search.
        boolean extOptedIn = isExtBatchEnabled(source.ext());
        boolean topLevelSemanticPresent = source.highlighter() != null
            && HighlightExtractorUtils.extractSemanticField(source.highlighter()) != null;
        if (!extOptedIn && !topLevelSemanticPresent) {
            return false;
        }

        // Resolve all targets — including those declared inside inner_hits anywhere
        // in the query tree — and only fire when the request actually opts in.
        HighlightConfig config = HighlightConfigResolver.resolve(request);
        if (!config.hasTargets()) {
            return false;
        }

        // Symmetric with SemanticHighlighter's fetch-phase yield: take over only
        // when the request explicitly opts into batch via ext, or when any target
        // declares the legacy field-level batch_inference flag.
        return extOptedIn || anyTargetRequestsBatch(config.getTargetsOrEmpty());
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

    private static boolean anyTargetRequestsBatch(List<SemanticHighlightTarget> targets) {
        if (targets == null || targets.isEmpty()) {
            return false;
        }
        for (SemanticHighlightTarget target : targets) {
            if (HighlightExtractorUtils.extractBatchInferenceFromOptions(target.getOptions())) {
                return true;
            }
        }
        return false;
    }
}
