/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.highlight;

import java.util.Map;

import lombok.extern.log4j.Log4j2;
import org.opensearch.core.common.text.Text;
import org.opensearch.index.mapper.MappedFieldType;
import org.opensearch.neuralsearch.highlight.single.SemanticHighlighterEngine;
import org.opensearch.neuralsearch.highlight.utils.HighlightExtractorUtils;
import org.opensearch.neuralsearch.query.ext.SemanticHighlighterExtBuilder;
import org.opensearch.neuralsearch.stats.events.EventStatName;
import org.opensearch.neuralsearch.stats.events.EventStatsManager;
import org.opensearch.neuralsearch.util.NeuralSearchClusterUtil;
import org.opensearch.search.SearchExtBuilder;
import org.opensearch.search.fetch.subphase.highlight.FieldHighlightContext;
import org.opensearch.search.fetch.subphase.highlight.HighlightField;
import org.opensearch.search.fetch.subphase.highlight.Highlighter;

/**
 * Semantic highlighter that uses ML models to identify relevant text spans.
 *
 * <p>The fetch-phase highlighter yields (returns null) so the response processor
 * can perform highlighting when either of the following is true:
 * <ul>
 *   <li>The request carries the {@code ext.semantic_highlighting_batch: true} block.</li>
 *   <li>The field options declare {@code batch_inference: true} (legacy signal).</li>
 * </ul>
 * Otherwise, it performs a single inference per field synchronously.
 */
@Log4j2
public class SemanticHighlighter implements Highlighter {

    private SemanticHighlighterEngine semanticHighlighterEngine;

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
        Map<String, Object> options = fieldContext.field.fieldOptions().options();

        // Yield if the request opts into the response processor path: either the
        // request-level ext block (set to true), or the legacy field-level batch_inference flag.
        if (isExtBatchEnabled(fieldContext) || extractBatchInference(options)) {
            if (!isSystemProcessorEnabled()) {
                log.warn(
                    "[SEMANTIC_HIGHLIGHT] Field [{}] is configured for batch semantic highlighting but the system-generated "
                        + "processor is not enabled. Add '{}' to the cluster setting "
                        + "'cluster.search.enabled_system_generated_factories' to enable batch highlighting.",
                    fieldContext.fieldName,
                    SemanticHighlightingConstants.SYSTEM_FACTORY_TYPE
                );
            }
            return null;
        }

        if (semanticHighlighterEngine == null) {
            throw new IllegalStateException(
                "SemanticHighlighter has not been initialized. This can happen when the neural-search plugin "
                    + "is not fully initialized on this node. Please ensure the plugin is properly installed and configured."
            );
        }

        EventStatsManager.increment(EventStatName.SEMANTIC_HIGHLIGHTING_REQUEST_COUNT);

        String fieldText = HighlightExtractorUtils.getFieldText(fieldContext);
        if (fieldText == null) {
            return null;
        }

        String modelId = HighlightExtractorUtils.getModelId(fieldContext.field.fieldOptions().options());
        String originalQueryText = semanticHighlighterEngine.extractOriginalQuery(fieldContext.query, fieldContext.fieldName);
        if (originalQueryText == null || originalQueryText.isEmpty()) {
            log.warn("No query text found for field {}", fieldContext.fieldName);
            return null;
        }

        String[] preTags = fieldContext.field.fieldOptions().preTags();
        String[] postTags = fieldContext.field.fieldOptions().postTags();
        String preTag = preTags[0];
        String postTag = postTags[0];

        String highlightedResponse = semanticHighlighterEngine.getHighlightedSentences(
            modelId,
            originalQueryText,
            fieldText,
            preTag,
            postTag
        );

        if (highlightedResponse == null || highlightedResponse.isEmpty()) {
            // no_match_size: emit first N chars when model returns no spans.
            int noMatchSize = extractNoMatchSize(options);
            if (noMatchSize > 0 && !fieldText.isEmpty()) {
                String snippet = fieldText.length() <= noMatchSize ? fieldText : fieldText.substring(0, noMatchSize);
                if (SemanticHighlightingConstants.ENCODER_HTML.equalsIgnoreCase(extractEncoder(options))) {
                    snippet = htmlEscape(snippet);
                }
                return new HighlightField(fieldContext.fieldName, new Text[] { new Text(snippet) });
            }
            log.warn("No highlighted text returned for field: {}, returning null", fieldContext.fieldName);
            return null;
        }

        if (SemanticHighlightingConstants.ENCODER_HTML.equalsIgnoreCase(extractEncoder(options))) {
            highlightedResponse = htmlEncodePreservingTags(highlightedResponse, preTag, postTag);
        }

        return new HighlightField(fieldContext.fieldName, new Text[] { new Text(highlightedResponse) });
    }

    private static boolean isExtBatchEnabled(FieldHighlightContext fieldContext) {
        if (fieldContext == null || fieldContext.context == null) return false;
        SearchExtBuilder builder = fieldContext.context.getSearchExt(SemanticHighlightingConstants.EXT_NAME);
        return builder instanceof SemanticHighlighterExtBuilder && ((SemanticHighlighterExtBuilder) builder).isEnabled();
    }

    private static boolean extractBatchInference(Map<String, Object> options) {
        return HighlightExtractorUtils.extractBatchInferenceFromOptions(options);
    }

    private boolean isSystemProcessorEnabled() {
        return NeuralSearchClusterUtil.instance().isSystemGeneratedFactoryEnabled(SemanticHighlightingConstants.SYSTEM_FACTORY_TYPE);
    }

    private static int extractNoMatchSize(Map<String, Object> options) {
        if (options == null) return SemanticHighlightingConstants.DEFAULT_NO_MATCH_SIZE;
        Object v = options.get(SemanticHighlightingConstants.NO_MATCH_SIZE);
        if (v instanceof Number) return ((Number) v).intValue();
        if (v instanceof String) {
            try {
                return Integer.parseInt((String) v);
            } catch (NumberFormatException ignored) {}
        }
        return SemanticHighlightingConstants.DEFAULT_NO_MATCH_SIZE;
    }

    private static String extractEncoder(Map<String, Object> options) {
        if (options == null) return SemanticHighlightingConstants.DEFAULT_ENCODER;
        Object v = options.get(SemanticHighlightingConstants.ENCODER);
        if (v instanceof String) return (String) v;
        return SemanticHighlightingConstants.DEFAULT_ENCODER;
    }

    private static String htmlEscape(String text) {
        StringBuilder sb = new StringBuilder(text.length());
        for (int i = 0; i < text.length(); i++) {
            char c = text.charAt(i);
            switch (c) {
                case '&' -> sb.append("&amp;");
                case '<' -> sb.append("&lt;");
                case '>' -> sb.append("&gt;");
                case '"' -> sb.append("&quot;");
                case '\'' -> sb.append("&#39;");
                default -> sb.append(c);
            }
        }
        return sb.toString();
    }

    /** HTML-escape the text portions of a highlighted response while preserving the highlight tags. */
    private static String htmlEncodePreservingTags(String highlighted, String preTag, String postTag) {
        StringBuilder result = new StringBuilder(highlighted.length());
        int cursor = 0;
        while (cursor < highlighted.length()) {
            int preIdx = highlighted.indexOf(preTag, cursor);
            if (preIdx < 0) {
                result.append(htmlEscape(highlighted.substring(cursor)));
                break;
            }
            result.append(htmlEscape(highlighted.substring(cursor, preIdx)));
            result.append(preTag);
            int afterPre = preIdx + preTag.length();
            int postIdx = highlighted.indexOf(postTag, afterPre);
            if (postIdx < 0) {
                result.append(htmlEscape(highlighted.substring(afterPre)));
                break;
            }
            result.append(htmlEscape(highlighted.substring(afterPre, postIdx)));
            result.append(postTag);
            cursor = postIdx + postTag.length();
        }
        return result.toString();
    }
}
