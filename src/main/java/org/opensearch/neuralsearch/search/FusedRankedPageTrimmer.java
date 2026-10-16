/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.search;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import org.opensearch.action.search.SearchResponse;
import org.opensearch.search.SearchHit;
import org.opensearch.search.SearchHits;

/**
 * Keeps a fused hybrid's page to the documents fusion ranked.
 *
 * <p>Round 2 of a fused query matches the Tail — every leg's matches, non-scoring — beside the ranked window, so that
 * {@code hits.total}, aggregations and highlighting cover the whole match set. A Tail-only document scores {@code 0} and
 * sorts after every ranked one, so a page that reaches past the window would be filled with them. The page instead ends
 * where the ranked documents end: a page past them comes back short or empty, as classic hybrid's pages end at the
 * candidates it collected and as a Top-only fused request ({@code track_total_hits: false}) pages anyway. So for a
 * request whose page is kept this way, whether the Tail runs — which only totals, aggregations, highlighting, sorting
 * and collapse decide — does not change the page. Totals and aggregations are untouched — only the page is trimmed.
 *
 * <p>Dropping documents keeps every page a contiguous slice of the ranking only when the ranked documents come first,
 * which holds when the request orders by the fused score ({@code HybridQueryBuilder#requestPagesTheFusedRanking}); the
 * rewrite publishes the ranking only then, and only for the request's own query. That includes a fused hybrid that is a
 * direct leg of an enclosing one: its leg sub-search is a request of its own, so the enclosing fusion is handed the nested
 * ranking alone rather than Tail-only documents at score {@code 0} whose order says nothing about relevance.
 *
 * <p>Membership is decided by {@code _index} plus {@code _id}, the key fusion ranked by. Fused mode refuses
 * {@code stored_fields: "_none_"}, the one request shape whose hits carry no {@code _id}; should a hit still arrive
 * without either, membership cannot be decided and the page is returned as round 2 produced it.
 *
 * <p>Search pipeline response processors run before this, on round 2's page: they still see a page past the ranked
 * window filled with Tail-only documents. A chain that reorders and then truncates (an oversampling request processor
 * with a {@code rerank} and {@code truncate_hits} response) can therefore keep Tail-only documents in place of ranked
 * ones and come back short after the trim. A window that covers the oversampled page — {@code fusion.window_size} of at
 * least {@code from + size × sample_factor} — leaves no Tail-only document for such a chain to see. A processor that
 * returns its own {@link SearchResponse} subclass (ml-commons' RAG processor, whose response carries the generated
 * answer) is left alone entirely: the trim rebuilds a plain response, which would drop the subclass's fields.
 *
 * <p>Request-scoped, like {@code FusedHitsMerger}: created by {@link HybridQuerySearchRequestFilter}, handed to the
 * fused hybrid as a consumer, and read once when the response passes back through the filter. Nothing published —
 * the request was not one of those, or the page was assembled from the legs and holds only ranked documents already —
 * returns the response as it came.
 */
public final class FusedRankedPageTrimmer {

    private volatile Map<String, Set<String>> rankedIdsByIndex;

    /** Receives the ranked window as parallel {@code _index}/{@code _id} arrays, in rank order. */
    public interface RankedDocsConsumer {
        void accept(String[] indices, String[] ids);
    }

    public RankedDocsConsumer consumer() {
        return (indices, ids) -> {
            Map<String, Set<String>> byIndex = new HashMap<>();
            for (int i = 0; i < ids.length; i++) {
                byIndex.computeIfAbsent(indices[i], index -> new HashSet<>()).add(ids[i]);
            }
            rankedIdsByIndex = byIndex;
        };
    }

    /**
     * The response with every hit fusion did not rank removed — same total, {@code max_score}, sort fields and collapse
     * field — or the response itself when nothing was published, every hit is ranked, a hit carries no {@code _index}
     * or {@code _id} to decide membership by, or the response is a {@link SearchResponse} subclass: a search-pipeline
     * response processor built it (ml-commons' RAG processor returns one carrying the generated answer), and rebuilding
     * it as a plain response would drop what the subclass holds, so the page stays as that processor left it — see
     * {@link FusedResponseRebuilder#carriesOnlyCoreFields}.
     */
    public SearchResponse getMergedResponse(final SearchResponse response) {
        Map<String, Set<String>> ranked = rankedIdsByIndex;
        SearchHits hits = response.getHits();
        if (Objects.isNull(ranked) || Objects.isNull(hits) || FusedResponseRebuilder.carriesOnlyCoreFields(response) == false) {
            return response;
        }
        SearchHit[] page = hits.getHits();
        for (SearchHit hit : page) {
            if (Objects.isNull(hit.getIndex()) || Objects.isNull(hit.getId())) {
                return response;
            }
        }
        Object[] collapseValues = hits.getCollapseValues();
        // Collapse values are one per returned group, parallel to the hits, so they are trimmed alongside them.
        boolean parallelCollapseValues = Objects.nonNull(collapseValues) && collapseValues.length == page.length;
        List<SearchHit> kept = new ArrayList<>(page.length);
        List<Object> keptCollapseValues = new ArrayList<>(page.length);
        for (int i = 0; i < page.length; i++) {
            Set<String> ids = ranked.get(page[i].getIndex());
            if (Objects.nonNull(ids) && ids.contains(page[i].getId())) {
                kept.add(page[i]);
                if (parallelCollapseValues) {
                    keptCollapseValues.add(collapseValues[i]);
                }
            }
        }
        if (kept.size() == page.length) {
            return response;
        }
        SearchHits trimmed = new SearchHits(
            kept.toArray(new SearchHit[0]),
            hits.getTotalHits(),
            // Kept even when the trim empties the page, as core keeps it for a page past the matches: each shard reports the
            // score of the best document it collected, documents before `from` included, so this is the top ranked score.
            hits.getMaxScore(),
            hits.getSortFields(),
            hits.getCollapseField(),
            parallelCollapseValues ? keptCollapseValues.toArray() : collapseValues
        );
        return FusedResponseRebuilder.rebuild(response, null, response.isTimedOut(), trimmed);
    }
}
