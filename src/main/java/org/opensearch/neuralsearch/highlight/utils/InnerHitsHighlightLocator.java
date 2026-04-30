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

/**
 * Locates a semantic highlight configuration attached to a nested query's
 * {@code inner_hits}. Supports unwrapping bool and hybrid queries so the nested
 * clause can be found several levels deep.
 */
public final class InnerHitsHighlightLocator {

    private InnerHitsHighlightLocator() {}

    @Getter
    @AllArgsConstructor
    public static class Location {
        private final HighlightBuilder highlightBuilder;
        private final String innerHitName;
        private final String nestedPath;
    }

    /**
     * Find a semantic highlight configuration attached to a nested query's inner_hits
     * @param query the root query to search, may be null
     * @return the first semantic-highlight inner_hits location found, or null if none
     */
    public static Location find(QueryBuilder query) {
        if (query == null) {
            return null;
        }
        if (query instanceof NestedQueryBuilder nested) {
            Location fromThis = fromNested(nested);
            if (fromThis != null) {
                return fromThis;
            }
            return find(nested.query());
        }
        if (query instanceof BoolQueryBuilder bool) {
            Location located = findFirst(bool.must());
            if (located != null) return located;
            located = findFirst(bool.should());
            if (located != null) return located;
            located = findFirst(bool.filter());
            if (located != null) return located;
            return findFirst(bool.mustNot());
        }
        if (query instanceof HybridQueryBuilder hybrid) {
            return findFirst(hybrid.queries());
        }
        return null;
    }

    private static Location findFirst(Iterable<QueryBuilder> queries) {
        if (queries == null) return null;
        for (QueryBuilder qb : queries) {
            Location located = find(qb);
            if (located != null) {
                return located;
            }
        }
        return null;
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
