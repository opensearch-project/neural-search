/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.search;

import org.apache.lucene.search.TotalHits;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.action.search.SearchResponseSections;
import org.opensearch.core.common.bytes.BytesArray;
import org.opensearch.search.SearchHit;
import org.opensearch.search.SearchHits;
import org.opensearch.search.internal.InternalSearchResponse;
import org.opensearch.test.OpenSearchTestCase;

/**
 * {@link FusedRescoreScoreNormalizer} — turns the fused rescore guard's internal sentinel back into the score a user
 * should see.
 *
 * <p>The guard has to encode "this document was not ranked by the fusion" as a score, because the score is the only
 * channel a shard has to the coordinator's merge. That encoding must not reach the user: without a rescore those
 * documents come back at exactly {@code 0.0}, so that is what the sentinel maps to, and the response for a fused request
 * with a rescore reports the same unranked scores as the same request without one.
 */
public class FusedRescoreScoreNormalizerTests extends OpenSearchTestCase {

    private static final float SENTINEL = -Float.MAX_VALUE;

    public void testNormalize_replacesEverySentinelScoreWithZero() {
        SearchResponse response = responseOf(2.5f, hit(1, 2.5f), hit(2, SENTINEL), hit(3, SENTINEL));

        SearchResponse normalized = FusedRescoreScoreNormalizer.normalize(response, SENTINEL);

        SearchHit[] hits = normalized.getInternalResponse().hits().getHits();
        assertEquals("a ranked score is untouched", 2.5f, hits[0].getScore(), 0.0f);
        assertEquals(0.0f, hits[1].getScore(), 0.0f);
        assertEquals(0.0f, hits[2].getScore(), 0.0f);
    }

    /**
     * The one field that cannot be corrected in place: {@code SearchHits.maxScore} is {@code final}, so a page whose TOP
     * hit was demoted — a request that ranked nothing — needs the hits rebuilt.
     */
    public void testNormalize_whenTheTopHitWasDemoted_thenMaxScoreIsCorrectedToo() {
        SearchResponse response = responseOf(SENTINEL, hit(1, SENTINEL), hit(2, SENTINEL));

        SearchResponse normalized = FusedRescoreScoreNormalizer.normalize(response, SENTINEL);

        assertEquals("max_score must not report the sentinel", 0.0f, normalized.getInternalResponse().hits().getMaxScore(), 0.0f);
        assertEquals(0.0f, normalized.getInternalResponse().hits().getHits()[0].getScore(), 0.0f);
        assertEquals("the rebuild must not lose the totals", 42, normalized.getInternalResponse().hits().getTotalHits().value());
    }

    /** A response with nothing demoted is handed back untouched — the common case pays nothing. */
    public void testNormalize_whenNothingWasDemoted_thenTheResponseIsTheSameInstance() {
        SearchResponse response = responseOf(9.0f, hit(1, 9.0f), hit(2, 0.25f));

        SearchResponse normalized = FusedRescoreScoreNormalizer.normalize(response, SENTINEL);

        assertSame(response, normalized);
        assertEquals(9.0f, normalized.getInternalResponse().hits().getHits()[0].getScore(), 0.0f);
    }

    /** A score that merely looks small is not the sentinel: the test is exact equality, not a threshold. */
    public void testNormalize_leavesAVeryNegativeButNonSentinelScoreAlone() {
        float almost = Math.nextUp(-Float.MAX_VALUE);
        SearchResponse response = responseOf(almost, hit(1, almost));

        SearchResponse normalized = FusedRescoreScoreNormalizer.normalize(response, SENTINEL);

        assertEquals(almost, normalized.getInternalResponse().hits().getHits()[0].getScore(), 0.0f);
    }

    /** Fails open rather than throwing on the way to the caller's listener. */
    public void testNormalize_whenThereAreNoHits_thenNothingHappens() {
        assertNull(FusedRescoreScoreNormalizer.normalize(null, SENTINEL));

        SearchResponse empty = responseOf(Float.NaN);
        assertSame(empty, FusedRescoreScoreNormalizer.normalize(empty, SENTINEL));
    }

    private SearchHit hit(final int docId, final float score) {
        SearchHit hit = new SearchHit(docId, String.valueOf(docId), null, null);
        hit.score(score);
        hit.sourceRef(new BytesArray("{}"));
        return hit;
    }

    private SearchResponse responseOf(final float maxScore, final SearchHit... hits) {
        SearchHits searchHits = new SearchHits(hits, new TotalHits(42, TotalHits.Relation.EQUAL_TO), maxScore);
        SearchResponseSections sections = new InternalSearchResponse(searchHits, null, null, null, false, null, 1);
        return new SearchResponse(sections, null, 1, 1, 0, 1L, new org.opensearch.action.search.ShardSearchFailure[0], null, null);
    }
}
