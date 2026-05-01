/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.highlight.utils;

import lombok.AllArgsConstructor;
import lombok.Getter;
import org.opensearch.index.query.BoolQueryBuilder;
import org.opensearch.index.query.InnerHitBuilder;
import org.opensearch.index.query.NestedQueryBuilder;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.neuralsearch.highlight.SemanticHighlightingConstants;
import org.opensearch.neuralsearch.query.HybridQueryBuilder;
import org.opensearch.search.fetch.subphase.highlight.HighlightBuilder;

import java.util.ArrayList;
import java.util.List;

/**
 * Locates semantic highlight configurations attached to nested queries' {@code inner_hits}.
 * Supports unwrapping bool and hybrid queries so nested clauses several levels deep are found.
 */
public final class InnerHitsHighlightLocator {

    private static final int MAX_DEPTH = 20;

    private InnerHitsHighlightLocator() {}

    @Getter
    @AllArgsConstructor
    public static class Location {
        private final HighlightBuilder highlightBuilder;
        private final String innerHitName;
        private final String nestedPath;
    }

    /**
     * Find all semantic highlight configurations attached to nested queries' inner_hits
     * @param query the root query to search, may be null
     * @return every semantic-highlight inner_hits location found in the query tree, in traversal order,
     *         or an empty list if none are found
     */
    public static List<Location> findAll(QueryBuilder query) {
        List<Location> results = new ArrayList<>();
        collect(query, results, 0);
        return results;
    }

    private static void collect(QueryBuilder query, List<Location> out, int depth) {
        if (query == null || depth > MAX_DEPTH) {
            return;
        }
        if (query instanceof NestedQueryBuilder nested) {
            Location fromThis = fromNested(nested);
            if (fromThis != null) {
                out.add(fromThis);
            }
            collect(nested.query(), out, depth + 1);
            return;
        }
        if (query instanceof BoolQueryBuilder bool) {
            bool.must().forEach(q -> collect(q, out, depth + 1));
            bool.should().forEach(q -> collect(q, out, depth + 1));
            bool.filter().forEach(q -> collect(q, out, depth + 1));
            bool.mustNot().forEach(q -> collect(q, out, depth + 1));
            return;
        }
        if (query instanceof HybridQueryBuilder hybrid) {
            hybrid.queries().forEach(q -> collect(q, out, depth + 1));
        }
    }

    private static Location fromNested(NestedQueryBuilder nested) {
        InnerHitBuilder innerHit = nested.innerHit();
        if (innerHit == null) {
            return null;
        }
        HighlightBuilder highlighter = innerHit.getHighlightBuilder();
        if (highlighter == null || !hasSemanticField(highlighter)) {
            return null;
        }
        String name = innerHit.getName() != null ? innerHit.getName() : nested.path();
        return new Location(highlighter, name, nested.path());
    }

    private static boolean hasSemanticField(HighlightBuilder highlighter) {
        if (highlighter.fields() == null) {
            return false;
        }
        return highlighter.fields().stream().anyMatch(f -> SemanticHighlightingConstants.HIGHLIGHTER_TYPE.equals(f.highlighterType()));
    }
}
