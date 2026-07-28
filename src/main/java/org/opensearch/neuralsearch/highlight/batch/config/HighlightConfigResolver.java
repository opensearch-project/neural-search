/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.highlight.batch.config;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Consumer;

import lombok.extern.log4j.Log4j2;
import org.apache.lucene.search.BooleanClause;
import org.opensearch.action.search.SearchRequest;
import org.opensearch.common.Nullable;
import org.opensearch.index.query.InnerHitBuilder;
import org.opensearch.index.query.NestedQueryBuilder;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.index.query.QueryBuilderVisitor;
import org.opensearch.neuralsearch.highlight.SemanticHighlightingConstants;
import org.opensearch.neuralsearch.processor.util.ProcessorUtils;
import org.opensearch.search.builder.SearchSourceBuilder;
import org.opensearch.search.fetch.subphase.highlight.HighlightBuilder;

/**
 * Scans a search request's highlight DSL and produces one {@link SemanticHighlightTarget}
 * per field declared as {@code type: semantic}, whether at the top level or inside an
 * {@code inner_hits} block. Nested path and bucket name come from the enclosing
 * {@link NestedQueryBuilder} and {@link InnerHitBuilder}.
 */
@Log4j2
public final class HighlightConfigResolver {

    private HighlightConfigResolver() {}

    /**
     * Resolves a per-request configuration. Returns an empty config when the request
     * declares no semantic highlight.
     */
    public static HighlightConfig resolve(SearchRequest request) {
        if (request == null || request.source() == null) {
            return HighlightConfig.empty();
        }
        SearchSourceBuilder source = request.source();
        List<SemanticHighlightTarget> targets = new ArrayList<>();

        collectTopLevelTargets(source.highlighter(), targets);
        collectInnerHitsTargets(source.query(), targets);

        if (targets.isEmpty()) {
            return HighlightConfig.empty();
        }

        return HighlightConfig.builder().targets(targets).queryText(resolveQueryText(request)).build();
    }

    private static void collectTopLevelTargets(HighlightBuilder highlighter, List<SemanticHighlightTarget> sink) {
        if (highlighter == null) return;
        List<HighlightBuilder.Field> fields = Optional.ofNullable(highlighter.fields()).orElse(Collections.emptyList());
        for (HighlightBuilder.Field field : fields) {
            if (!SemanticHighlightingConstants.HIGHLIGHTER_TYPE.equals(field.highlighterType())) {
                continue;
            }
            sink.add(
                SemanticHighlightTarget.builder()
                    .fieldName(field.name())
                    .options(mergeOptions(highlighter.options(), field.options()))
                    .preTag(pickFirstTag(field.preTags(), highlighter.preTags()))
                    .postTag(pickFirstTag(field.postTags(), highlighter.postTags()))
                    .build()
            );
        }
    }

    private static void collectInnerHitsTargets(@Nullable QueryBuilder query, List<SemanticHighlightTarget> sink) {
        if (query == null) return;
        QueryBuilderVisitor visitor = new InnerHitHighlightVisitor(sink::add);
        query.visit(visitor);
    }

    private static String resolveQueryText(SearchRequest request) {
        HighlightBuilder top = request.source().highlighter();
        if (top != null && top.highlightQuery() != null) {
            String text = tryExtract(top.highlightQuery());
            if (text != null) return text;
        }
        QueryBuilder mainQuery = request.source().query();
        if (mainQuery == null) return null;
        return tryExtract(mainQuery);
    }

    /**
     * Best-effort query-text extraction used when building the per-request config.
     * Returns null when the query cannot be unwrapped — callers (the processor) are
     * responsible for surfacing this to the customer at execution time.
     */
    private static String tryExtract(QueryBuilder query) {
        try {
            return ProcessorUtils.extractQueryTextFromBuilder(query);
        } catch (IllegalArgumentException e) {
            log.debug("Could not extract query text: {}", e.getMessage());
            return null;
        }
    }

    private static Map<String, Object> mergeOptions(Map<String, Object> global, Map<String, Object> fieldLevel) {
        if (global == null && fieldLevel == null) return Collections.emptyMap();
        if (global == null) return fieldLevel;
        if (fieldLevel == null) return global;
        Map<String, Object> merged = new LinkedHashMap<>(global);
        merged.putAll(fieldLevel);
        return merged;
    }

    private static String pickFirstTag(String[] fieldLevel, String[] global) {
        if (fieldLevel != null && fieldLevel.length > 0) return fieldLevel[0];
        if (global != null && global.length > 0) return global[0];
        return null;
    }

    private record InnerHitHighlightVisitor(Consumer<SemanticHighlightTarget> consumer) implements QueryBuilderVisitor {
        @Override
        public void accept(QueryBuilder qb) {
            if (qb instanceof NestedQueryBuilder nested) {
                InnerHitBuilder inner = nested.innerHit();
                if (inner == null) {
                    return;
                }
                if (inner.getHighlightBuilder() == null) {
                    return;
                }
                HighlightBuilder innerHighlight = inner.getHighlightBuilder();
                String bucketName = inner.getName() != null ? inner.getName() : nested.path();
                List<HighlightBuilder.Field> fields = Optional.ofNullable(innerHighlight.fields()).orElse(Collections.emptyList());
                for (HighlightBuilder.Field field : fields) {
                    if (!SemanticHighlightingConstants.HIGHLIGHTER_TYPE.equals(field.highlighterType())) {
                        continue;
                    }
                    consumer.accept(
                        SemanticHighlightTarget.builder()
                            .fieldName(field.name())
                            .nestedPath(nested.path())
                            .innerHitsBucketName(bucketName)
                            .options(mergeOptions(innerHighlight.options(), field.options()))
                            .preTag(pickFirstTag(field.preTags(), innerHighlight.preTags()))
                            .postTag(pickFirstTag(field.postTags(), innerHighlight.postTags()))
                            .build()
                    );
                }
            }
        }

        @Override
        public QueryBuilderVisitor getChildVisitor(BooleanClause.Occur occur) {
            return this;
        }
    }
}
