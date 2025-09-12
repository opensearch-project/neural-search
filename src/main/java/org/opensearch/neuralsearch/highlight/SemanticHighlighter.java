/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.highlight;

import lombok.Setter;
import lombok.extern.log4j.Log4j2;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.common.text.Text;
import org.opensearch.index.mapper.MappedFieldType;
import org.opensearch.neuralsearch.highlight.single.SemanticHighlighterEngine;
import org.opensearch.neuralsearch.highlight.utils.HighlightExtractorUtils;
import org.opensearch.neuralsearch.stats.events.EventStatName;
import org.opensearch.neuralsearch.stats.events.EventStatsManager;
import org.opensearch.neuralsearch.util.NeuralSearchClusterUtil;
import org.opensearch.search.fetch.subphase.highlight.FieldHighlightContext;
import org.opensearch.search.fetch.subphase.highlight.HighlightField;
import org.opensearch.search.fetch.subphase.highlight.Highlighter;
import org.opensearch.search.pipeline.SearchPipelineService;

import java.util.List;
import java.util.Locale;
import java.util.Map;

/**
 * Minimal semantic highlighter that validates "semantic" highlighter type
 * but delegates actual highlighting to SystemGeneratedSemanticHighlightingProcessor
 */
public class SemanticHighlighter implements Highlighter {
    private SemanticHighlighterEngine semanticHighlighterEngine;
    @Setter
    private ClusterService clusterService;

    public void initialize(SemanticHighlighterEngine semanticHighlighterEngine) {
        if (this.semanticHighlighterEngine != null) {
            throw new IllegalStateException(
                "SemanticHighlighterEngine has already been initialized. Multiple initializations are not permitted."
            );
        }
        this.semanticHighlighterEngine = semanticHighlighterEngine;
    }

    @Override
    public boolean canHighlight(MappedFieldType fieldType) {
        return true;
    }

    @Override
    public HighlightField highlight(FieldHighlightContext fieldContext) {
        // Extract batch_inference option from field options
        Map<String, Object> options = fieldContext.field.fieldOptions().options();
        boolean batchInference = extractBatchInference(options);

        if (batchInference) {
            // Check if system processor is enabled
            if (!isSystemProcessorEnabled()) {
                // When batch mode is requested but system processor is not enabled, throw an exception
                String errorMessage = String.format(
                    Locale.ROOT,
                    "Batch inference for semantic highlighting is disabled. Enable it by adding '%s' to the '%s' cluster setting.",
                    SemanticHighlightingConstants.SYSTEM_FACTORY_TYPE,
                    SearchPipelineService.ENABLED_SYSTEM_GENERATED_FACTORIES_SETTING.getKey()
                );
                log.error("[SEMANTIC_HIGHLIGHT] BATCH MODE ERROR - {}", errorMessage);
                throw new IllegalArgumentException(errorMessage);
            }

            // Return null - actual highlighting will be done by SemanticHighlightingProcessor
            // This highlighter only serves to validate the system processor is enabled for batch mode
            return null;
        }

        // Below is the EXACT code from main branch without any changes
        if (semanticHighlighterEngine == null) {
            throw new IllegalStateException(
                "SemanticHighlighter has not been initialized. This can happen when the neural-search plugin "
                    + "is not fully initialized on this node. Please ensure the plugin is properly installed and configured."
            );
        }

        EventStatsManager.increment(EventStatName.SEMANTIC_HIGHLIGHTING_REQUEST_COUNT);

        // Extract field text
        String fieldText = HighlightExtractorUtils.getFieldText(fieldContext);

        // Get model ID
        String modelId = HighlightExtractorUtils.getModelId(fieldContext.field.fieldOptions().options());

        // Try to extract query text
        String originalQueryText = semanticHighlighterEngine.extractOriginalQuery(fieldContext.query, fieldContext.fieldName);

        if (originalQueryText == null || originalQueryText.isEmpty()) {
            log.warn("No query text found for field {}", fieldContext.fieldName);
            return null;
        }

        // The pre- and post- tags are provided by the user or defaulted to <em> and </em>
        String[] preTags = fieldContext.field.fieldOptions().preTags();
        String[] postTags = fieldContext.field.fieldOptions().postTags();

        // Get highlighted text - allow any exceptions from this call to propagate
        String highlightedResponse = semanticHighlighterEngine.getHighlightedSentences(
            modelId,
            originalQueryText,
            fieldText,
            preTags[0],
            postTags[0]
        );

        if (highlightedResponse == null || highlightedResponse.isEmpty()) {
            log.warn("No highlighted text returned for field: {}, returning null", fieldContext.fieldName);
            return null;
        }

        // Create highlight field
        Text[] fragments = new Text[] { new Text(highlightedResponse) };
        return new HighlightField(fieldContext.fieldName, fragments);
    }

    private boolean extractBatchInference(Map<String, Object> options) {
        return HighlightExtractorUtils.extractBatchInferenceFromOptions(options);
    }

    private boolean isSystemProcessorEnabled() {
        if (clusterService == null) {
            clusterService = NeuralSearchClusterUtil.instance().getClusterService();
        }

        Settings clusterSettings = clusterService.state().metadata().settings();
        List<String> enabledFactories = clusterSettings.getAsList(
            SearchPipelineService.ENABLED_SYSTEM_GENERATED_FACTORIES_SETTING.getKey()
        );

        // Check if semantic-highlighter is enabled explicitly or if all factories are enabled with "*"
        return enabledFactories != null
            && (enabledFactories.contains(SemanticHighlightingConstants.SYSTEM_FACTORY_TYPE) || enabledFactories.contains("*"));
    }
}
