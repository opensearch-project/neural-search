/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.search;

import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.List;

import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.TotalHits;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.action.search.SearchResponseSections;
import org.opensearch.common.lucene.search.TopDocsAndMaxScore;
import org.opensearch.core.common.bytes.BytesArray;
import org.opensearch.neuralsearch.query.FusedWindowGuardRescorerBuilder;
import org.opensearch.search.SearchHit;
import org.opensearch.search.SearchHits;
import org.opensearch.search.aggregations.Aggregations;
import org.opensearch.search.aggregations.InternalAggregations;
import org.opensearch.search.aggregations.bucket.MultiBucketsAggregation;
import org.opensearch.search.aggregations.bucket.SingleBucketAggregation;
import org.opensearch.search.aggregations.metrics.InternalTopHits;
import org.opensearch.search.aggregations.metrics.TopHits;
import org.opensearch.search.internal.InternalSearchResponse;
import org.opensearch.test.OpenSearchTestCase;

/**
 * {@link FusedRescoreScoreNormalizer} — turns the fused rescore guard's internal band values back into the scores a user
 * should see.
 *
 * <p>The guard has to encode "this document was not ranked by the fusion" as a score, because the score is the only
 * channel a shard has to the coordinator's merge, and it needs a second value one float above it for a ranked document
 * whose own arithmetic saturated. Neither may reach the user: both are decoded to what the document would have scored
 * with no rescore in the request at all — {@code 0.0} for an unranked document, which matches only the non-scoring Tail,
 * and {@link FusedWindowGuardRescorerBuilder#RANKED_FLOOR_NORMALIZED} for a saturated ranked one.
 */
public class FusedRescoreScoreNormalizerTests extends OpenSearchTestCase {

    private static final float SENTINEL = -Float.MAX_VALUE;
    private static final float RANKED_FLOOR = FusedWindowGuardRescorerBuilder.RANKED_FLOOR;
    private static final float RANKED_FLOOR_NORMALIZED = FusedWindowGuardRescorerBuilder.RANKED_FLOOR_NORMALIZED;

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

    /**
     * The band's OTHER value. {@code RANKED_FLOOR} is where the guard parks a ranked document whose post-rescore score
     * saturated or went NaN, and it is not the sentinel — so decoding only the sentinel would report it verbatim, at
     * {@code -3.4028233E38}, one float away from an internal marker.
     */
    public void testNormalize_mapsTheRankedFloorToTheLowestReportableRankedScore() {
        SearchResponse response = responseOf(RANKED_FLOOR, hit(1, RANKED_FLOOR));

        SearchResponse normalized = FusedRescoreScoreNormalizer.normalize(response, SENTINEL);

        assertEquals(RANKED_FLOOR_NORMALIZED, normalized.getInternalResponse().hits().getHits()[0].getScore(), 0.0f);
        assertTrue("a ranked document must still outrank the normalized unranked band", RANKED_FLOOR_NORMALIZED > 0.0f);
    }

    /**
     * The maxScore half of the same question: the page's top hit can be a ranked document that saturated, and the
     * correction is then NOT {@code 0.0} — reporting that would put the page's maximum below its own ranked hits.
     */
    public void testNormalize_whenTheTopHitIsASaturatedRankedDocument_thenMaxScoreIsTheRankedFloor() {
        SearchResponse response = responseOf(RANKED_FLOOR, hit(1, RANKED_FLOOR), hit(2, SENTINEL));

        SearchResponse normalized = FusedRescoreScoreNormalizer.normalize(response, SENTINEL);

        assertEquals(RANKED_FLOOR_NORMALIZED, normalized.getInternalResponse().hits().getMaxScore(), 0.0f);
    }

    /**
     * Both band values on one page, which is the shape that makes decoding only one of them visibly wrong: the ranked
     * document outranks the unranked ones and must also print above them.
     */
    public void testNormalize_whenBothBandsAreOnThePage_thenTheReportedScoresStillDescend() {
        SearchResponse response = responseOf(RANKED_FLOOR, hit(1, RANKED_FLOOR), hit(2, SENTINEL), hit(3, SENTINEL));

        SearchResponse normalized = FusedRescoreScoreNormalizer.normalize(response, SENTINEL);

        SearchHits hits = normalized.getInternalResponse().hits();
        float previous = hits.getMaxScore();
        for (SearchHit hit : hits.getHits()) {
            assertTrue("max_score must not be below any hit score", hit.getScore() <= hits.getMaxScore());
            assertTrue("reported scores must not ascend down the page", hit.getScore() <= previous);
            previous = hit.getScore();
        }
        assertTrue("the ranked hit must print above the unranked ones", hits.getHits()[0].getScore() > hits.getHits()[1].getScore());
    }

    /**
     * A score outside the band is the user's own rescore arithmetic and stays exactly as core computed it, however
     * negative. The test is exact equality against two constants, not a threshold.
     */
    public void testNormalize_leavesAScoreOutsideTheBandAlone() {
        SearchResponse response = responseOf(-5.0f, hit(1, -5.0f), hit(2, Math.nextUp(RANKED_FLOOR)));

        SearchResponse normalized = FusedRescoreScoreNormalizer.normalize(response, SENTINEL);

        assertSame("nothing in the band, so no rebuild", response, normalized);
        assertEquals(-5.0f, normalized.getInternalResponse().hits().getHits()[0].getScore(), 0.0f);
        assertEquals(Math.nextUp(RANKED_FLOOR), normalized.getInternalResponse().hits().getHits()[1].getScore(), 0.0f);
    }

    /** Fails open rather than throwing on the way to the caller's listener. */
    public void testNormalize_whenThereAreNoHits_thenNothingHappens() {
        assertNull(FusedRescoreScoreNormalizer.normalize(null, SENTINEL));

        SearchResponse empty = responseOf(Float.NaN);
        assertSame(empty, FusedRescoreScoreNormalizer.normalize(empty, SENTINEL));
    }

    /**
     * {@code top_hits} is the second place core applies the request's rescorers, so the guard demotes inside a bucket
     * too — and unlike the page, a bucket asking for more hits than fusion ranked reaches that state with no unusual
     * weights anywhere in the request, because an aggregation is what forces the Tail open.
     */
    public void testNormalize_decodesTopHitsBucketsAsWellAsThePage() {
        SearchHits bucketHits = new SearchHits(
            new SearchHit[] { hit(1, 3.0f), hit(2, RANKED_FLOOR), hit(3, SENTINEL) },
            new TotalHits(3, TotalHits.Relation.EQUAL_TO),
            3.0f
        );
        SearchResponse response = responseWithAggregations(topHitsAggregation(bucketHits), 3.0f, hit(1, 3.0f));

        FusedRescoreScoreNormalizer.normalize(response, SENTINEL);

        assertEquals("a ranked bucket hit is untouched", 3.0f, bucketHits.getHits()[0].getScore(), 0.0f);
        assertEquals(RANKED_FLOOR_NORMALIZED, bucketHits.getHits()[1].getScore(), 0.0f);
        assertEquals(0.0f, bucketHits.getHits()[2].getScore(), 0.0f);
    }

    /** A {@code top_hits} under a single-bucket aggregation — {@code filter}, {@code nested}, {@code global}. */
    public void testDecodeAggregations_descendsIntoSingleBucketAggregations() {
        SearchHits bucketHits = hitsOf(hit(1, SENTINEL));
        // Built before the outer stubbing starts: stubbing one mock inside another's thenReturn argument is an
        // UnfinishedStubbingException.
        Aggregations children = new Aggregations(List.of(topHits(bucketHits)));
        SingleBucketAggregation singleBucket = mock(SingleBucketAggregation.class);
        when(singleBucket.getAggregations()).thenReturn(children);

        FusedRescoreScoreNormalizer.decodeAggregations(new Aggregations(List.of(singleBucket)), SENTINEL);

        assertEquals(0.0f, bucketHits.getHits()[0].getScore(), 0.0f);
    }

    /** And under every bucket of a multi-bucket aggregation — {@code terms}, {@code date_histogram}, {@code range}. */
    public void testDecodeAggregations_descendsIntoEveryBucketOfAMultiBucketAggregation() {
        SearchHits first = hitsOf(hit(1, SENTINEL));
        SearchHits second = hitsOf(hit(2, RANKED_FLOOR));
        MultiBucketsAggregation multiBucket = mock(MultiBucketsAggregation.class);
        // doReturn rather than when(...).thenReturn: getBuckets() is declared List<? extends Bucket>, which no concrete
        // list literal can be inferred against.
        doReturn(List.of(bucketOf(first), bucketOf(second))).when(multiBucket).getBuckets();

        FusedRescoreScoreNormalizer.decodeAggregations(new Aggregations(List.of(multiBucket)), SENTINEL);

        assertEquals(0.0f, first.getHits()[0].getScore(), 0.0f);
        assertEquals(RANKED_FLOOR_NORMALIZED, second.getHits()[0].getScore(), 0.0f);
    }

    /** No aggregations at all is the overwhelming majority of requests and must not throw. */
    public void testDecodeAggregations_whenThereAreNone_thenNothingHappens() {
        FusedRescoreScoreNormalizer.decodeAggregations(null, SENTINEL);
        FusedRescoreScoreNormalizer.decodeAggregations(new Aggregations(List.of()), SENTINEL);
    }

    private MultiBucketsAggregation.Bucket bucketOf(final SearchHits bucketHits) {
        Aggregations children = new Aggregations(List.of(topHits(bucketHits)));
        MultiBucketsAggregation.Bucket bucket = mock(MultiBucketsAggregation.Bucket.class);
        when(bucket.getAggregations()).thenReturn(children);
        return bucket;
    }

    private TopHits topHits(final SearchHits bucketHits) {
        TopHits topHits = mock(TopHits.class);
        when(topHits.getHits()).thenReturn(bucketHits);
        return topHits;
    }

    private InternalTopHits topHitsAggregation(final SearchHits bucketHits) {
        TopDocs topDocs = new TopDocs(bucketHits.getTotalHits(), new ScoreDoc[0]);
        return new InternalTopHits(
            "top",
            0,
            bucketHits.getHits().length,
            new TopDocsAndMaxScore(topDocs, bucketHits.getMaxScore()),
            bucketHits,
            null
        );
    }

    private SearchHits hitsOf(final SearchHit... hits) {
        return new SearchHits(hits, new TotalHits(hits.length, TotalHits.Relation.EQUAL_TO), hits[0].getScore());
    }

    private SearchHit hit(final int docId, final float score) {
        SearchHit hit = new SearchHit(docId, String.valueOf(docId), null, null);
        hit.score(score);
        hit.sourceRef(new BytesArray("{}"));
        return hit;
    }

    private SearchResponse responseOf(final float maxScore, final SearchHit... hits) {
        return responseWithAggregations(null, maxScore, hits);
    }

    private SearchResponse responseWithAggregations(final InternalTopHits aggregation, final float maxScore, final SearchHit... hits) {
        SearchHits searchHits = new SearchHits(hits, new TotalHits(42, TotalHits.Relation.EQUAL_TO), maxScore);
        InternalAggregations aggregations = aggregation == null ? null : InternalAggregations.from(List.of(aggregation));
        SearchResponseSections sections = new InternalSearchResponse(searchHits, aggregations, null, null, false, null, 1);
        return new SearchResponse(sections, null, 1, 1, 0, 1L, new org.opensearch.action.search.ShardSearchFailure[0], null, null);
    }
}
