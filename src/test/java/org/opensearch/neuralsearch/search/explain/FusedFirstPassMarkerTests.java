/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.search.explain;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import org.apache.lucene.search.Explanation;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.TotalHits;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.action.search.ShardSearchFailure;
import org.opensearch.common.lucene.search.TopDocsAndMaxScore;
import org.opensearch.core.common.text.Text;
import org.opensearch.search.SearchHit;
import org.opensearch.search.SearchHits;
import org.opensearch.search.aggregations.Aggregation;
import org.opensearch.search.aggregations.Aggregations;
import org.opensearch.search.aggregations.InternalAggregations;
import org.opensearch.search.aggregations.bucket.MultiBucketsAggregation;
import org.opensearch.search.aggregations.bucket.SingleBucketAggregation;
import org.opensearch.search.aggregations.metrics.InternalTopHits;
import org.opensearch.search.aggregations.metrics.TopHits;
import org.opensearch.search.internal.InternalSearchResponse;
import org.opensearch.search.suggest.Suggest;
import org.opensearch.search.suggest.completion.CompletionSuggestion;
import org.opensearch.test.OpenSearchTestCase;

/**
 * {@link FusedFirstPassMarker}: the node the fused rescore guard puts around a first pass on the shard, found again on the
 * coordinator by its description alone, and removed wherever no fused breakdown takes its place.
 */
public class FusedFirstPassMarkerTests extends OpenSearchTestCase {

    private static final Explanation FIRST_PASS = Explanation.match(0.5f, "first pass", Explanation.match(0.5f, "top clause"));

    /**
     * The literal is pinned on purpose: the description crosses from the shards that write the marker to the coordinator
     * that replaces it, which can run different versions during an upgrade, so changing it in place breaks both
     * directions. See {@link FusedFirstPassMarker#DESCRIPTION} for how a new form has to be introduced.
     */
    public void testDescription_isTheValueShardsAndCoordinatorsAgreeOn() {
        assertEquals("first pass of the fused hybrid query:", FusedFirstPassMarker.DESCRIPTION);
    }

    public void testMark_wrapsAMatchingFirstPassOnceAtItsOwnValue() {
        Explanation marked = FusedFirstPassMarker.mark(FIRST_PASS);

        assertEquals(FusedFirstPassMarker.DESCRIPTION, marked.getDescription());
        assertSame("the value core reads is the first pass's own", FIRST_PASS.getValue(), marked.getValue());
        assertEquals(1, marked.getDetails().length);
        assertSame(FIRST_PASS, marked.getDetails()[0]);
    }

    public void testMark_whenThereIsNothingToMark_thenTheArgumentIsReturned() {
        Explanation noMatch = Explanation.noMatch("first pass did not match");

        assertSame("core computes no value from a first pass that did not match", noMatch, FusedFirstPassMarker.mark(noMatch));
        assertNull(FusedFirstPassMarker.mark(null));
    }

    public void testReplace_whenTheTreeHoldsNoMarker_thenItIsReturnedItself() {
        Explanation tree = layered(FIRST_PASS);

        assertSame(tree, FusedFirstPassMarker.replace(tree, marker -> Explanation.match(9f, "replacement")));
    }

    public void testReplace_rebuildsEveryNodeOnTheWayWithItsOwnValueDescriptionAndOtherChildren() {
        Explanation tree = layered(FusedFirstPassMarker.mark(FIRST_PASS));
        Explanation replacement = Explanation.match(0.5f, "fused breakdown");

        Explanation rebuilt = FusedFirstPassMarker.replace(tree, marker -> replacement);

        assertEquals(tree.getValue(), rebuilt.getValue());
        assertEquals(tree.getDescription(), rebuilt.getDescription());
        assertSame("a sibling off the path is kept as it was", tree.getDetails()[1], rebuilt.getDetails()[1]);
        Explanation layer = rebuilt.getDetails()[0];
        assertEquals(tree.getDetails()[0].getValue(), layer.getValue());
        assertSame(replacement, layer.getDetails()[0]);
        assertSame(tree.getDetails()[0].getDetails()[1], layer.getDetails()[1]);
    }

    public void testReplace_keepsANonMatchingNodeANonMatch() {
        Explanation tree = Explanation.noMatch("an enclosing non-match", FusedFirstPassMarker.mark(FIRST_PASS));

        Explanation rebuilt = FusedFirstPassMarker.replace(tree, marker -> Explanation.match(0.5f, "fused breakdown"));

        assertFalse(rebuilt.isMatch());
        assertEquals("an enclosing non-match", rebuilt.getDescription());
        assertEquals("fused breakdown", rebuilt.getDetails()[0].getDescription());
    }

    public void testReplace_whenTwoChildrenOfOneNodeAreMarked_thenBothAreReplaced() {
        Explanation tree = Explanation.match(1.0f, "sum of:", FusedFirstPassMarker.mark(FIRST_PASS), FusedFirstPassMarker.mark(FIRST_PASS));

        Explanation rebuilt = FusedFirstPassMarker.replace(tree, marker -> Explanation.match(0.5f, "fused breakdown"));

        assertEquals("fused breakdown", rebuilt.getDetails()[0].getDescription());
        assertEquals("fused breakdown", rebuilt.getDetails()[1].getDescription());
    }

    /** Only the exact shape the guard writes is a marker: a match carrying the description over exactly one child. */
    public void testReplace_whenANodeOnlySharesTheDescription_thenItIsNotAMarker() {
        Explanation twoChildren = Explanation.match(0.5f, FusedFirstPassMarker.DESCRIPTION, FIRST_PASS, FIRST_PASS);
        Explanation noMatch = Explanation.noMatch(FusedFirstPassMarker.DESCRIPTION, FIRST_PASS);

        assertSame(twoChildren, FusedFirstPassMarker.replace(twoChildren, marker -> null));
        assertSame(noMatch, FusedFirstPassMarker.replace(noMatch, marker -> null));
    }

    public void testReplace_whenTheReplacementRefuses_thenNull() {
        assertNull(FusedFirstPassMarker.replace(layered(FusedFirstPassMarker.mark(FIRST_PASS)), marker -> null));
    }

    public void testStrip_restoresExactlyTheTreeBuiltAroundTheUnmarkedFirstPass() {
        assertEquals(layered(FIRST_PASS), FusedFirstPassMarker.strip(layered(FusedFirstPassMarker.mark(FIRST_PASS))));
    }

    public void testStripAll_reachesHitsTheirInnerHitsAndTopHitsBuckets() {
        SearchHit hit = hitExplainedAs(layered(FusedFirstPassMarker.mark(FIRST_PASS)));
        SearchHit innerHit = hitExplainedAs(FusedFirstPassMarker.mark(FIRST_PASS));
        hit.setInnerHits(Map.of("leg", hitsOf(innerHit)));
        SearchHit bucketHit = hitExplainedAs(FusedFirstPassMarker.mark(FIRST_PASS));
        SearchHit unmarked = hitExplainedAs(FIRST_PASS);
        SearchHit unexplained = new SearchHit(0, "2", null, null);

        FusedFirstPassMarker.stripAll(response(topHitsAggregation(hitsOf(bucketHit)), hit, unmarked, unexplained));

        assertEquals(layered(FIRST_PASS), hit.getExplanation());
        assertSame("an inner hit folds the same rescorers", FIRST_PASS, innerHit.getExplanation());
        assertSame("and so does a top_hits bucket", FIRST_PASS, bucketHit.getExplanation());
        assertSame("a tree without a marker is left as it is", FIRST_PASS, unmarked.getExplanation());
        assertNull("and a hit that was not explained stays unexplained", unexplained.getExplanation());
    }

    /** The fetch phase explains a completion suggestion's hit along with the page, through the same rescorers. */
    public void testStripAll_reachesCompletionSuggestionHits() {
        SearchHit suggested = hitExplainedAs(FusedFirstPassMarker.mark(FIRST_PASS));
        CompletionSuggestion.Entry.Option option = new CompletionSuggestion.Entry.Option(0, new Text("lamp"), 1.0f, Map.of());
        option.setHit(suggested);
        CompletionSuggestion.Entry entry = new CompletionSuggestion.Entry(new Text("la"), 0, 2);
        entry.addOption(option);
        entry.addOption(new CompletionSuggestion.Entry.Option(1, new Text("lamp desk"), 0.5f, Map.of()));
        CompletionSuggestion suggestion = new CompletionSuggestion("products", 2, false);
        suggestion.addTerm(entry);
        SearchHits noHits = new SearchHits(new SearchHit[0], new TotalHits(0, TotalHits.Relation.EQUAL_TO), Float.NaN);
        // Core sorts the suggestions it is given, so the list has to be mutable.
        InternalSearchResponse internal = new InternalSearchResponse(
            noHits,
            null,
            new Suggest(new ArrayList<>(List.of(suggestion))),
            null,
            false,
            null,
            1
        );

        FusedFirstPassMarker.stripAll(
            new SearchResponse(internal, null, 1, 1, 0, 1L, ShardSearchFailure.EMPTY_ARRAY, SearchResponse.Clusters.EMPTY)
        );

        assertSame(FIRST_PASS, suggested.getExplanation());
    }

    public void testStripAggregations_descendsIntoMultiAndSingleBucketAggregations() {
        SearchHit inMultiBucket = hitExplainedAs(FusedFirstPassMarker.mark(FIRST_PASS));
        SearchHit inSingleBucket = hitExplainedAs(FusedFirstPassMarker.mark(FIRST_PASS));
        // Each inner mock is built before the stubbing that returns it; Mockito does not allow nesting the two.
        Aggregations bucketAggregations = new Aggregations(List.of(topHits(hitsOf(inMultiBucket))));
        MultiBucketsAggregation.Bucket bucket = mock(MultiBucketsAggregation.Bucket.class);
        when(bucket.getAggregations()).thenReturn(bucketAggregations);
        MultiBucketsAggregation terms = mock(MultiBucketsAggregation.class);
        when(terms.getBuckets()).thenAnswer(invocation -> List.of(bucket));
        Aggregations filterAggregations = new Aggregations(List.of(topHits(hitsOf(inSingleBucket))));
        SingleBucketAggregation filter = mock(SingleBucketAggregation.class);
        when(filter.getAggregations()).thenReturn(filterAggregations);

        FusedFirstPassMarker.stripAggregations(new Aggregations(List.<Aggregation>of(terms, filter)));
        FusedFirstPassMarker.stripAggregations(null);

        assertSame(FIRST_PASS, inMultiBucket.getExplanation());
        assertSame(FIRST_PASS, inSingleBucket.getExplanation());
    }

    public void testStripAll_whenThereIsNoResponseOrNoHits_thenNothingFails() {
        FusedFirstPassMarker.stripAll(null);
        FusedFirstPassMarker.stripAll(response(null));
        InternalSearchResponse noHitsSection = new InternalSearchResponse(null, null, null, null, false, null, 1);
        FusedFirstPassMarker.stripAll(
            new SearchResponse(noHitsSection, null, 1, 1, 0, 1L, ShardSearchFailure.EMPTY_ARRAY, SearchResponse.Clusters.EMPTY)
        );
        SearchHit hit = hitExplainedAs(FIRST_PASS);
        hit.setInnerHits(Map.of("leg", new SearchHits(null, new TotalHits(0, TotalHits.Relation.EQUAL_TO), Float.NaN)));
        FusedFirstPassMarker.stripAll(response(null, hit));
        assertSame(FIRST_PASS, hit.getExplanation());
    }

    /** A metric aggregation holds no hits; the walk passes it by. */
    public void testStripAggregations_whenAnAggregationHoldsNoHits_thenItIsSkipped() {
        FusedFirstPassMarker.stripAggregations(new Aggregations(List.of(mock(Aggregation.class))));
    }

    /** A generic two-level tree above the first pass: the walk matches on the marker alone, so only the nesting matters. */
    private static Explanation layered(final Explanation firstPass) {
        Explanation weighted = Explanation.match(0.5f, "product of:", firstPass, Explanation.match(1.0f, "primaryWeight"));
        Explanation rescoreQuery = Explanation.match(2.0f, "product of:", Explanation.match(1.0f, "weight(text:lamp)"));
        return Explanation.match(2.5f, "sum of:", weighted, rescoreQuery);
    }

    private static SearchHit hitExplainedAs(final Explanation explanation) {
        SearchHit hit = new SearchHit(0, "1", null, null);
        hit.explanation(explanation);
        return hit;
    }

    private static SearchHits hitsOf(final SearchHit... hits) {
        return new SearchHits(hits, new TotalHits(hits.length, TotalHits.Relation.EQUAL_TO), 1.0f);
    }

    private static TopHits topHits(final SearchHits hits) {
        TopHits topHits = mock(TopHits.class);
        when(topHits.getHits()).thenReturn(hits);
        return topHits;
    }

    private static InternalTopHits topHitsAggregation(final SearchHits hits) {
        TopDocs topDocs = new TopDocs(hits.getTotalHits(), new ScoreDoc[0]);
        return new InternalTopHits("top", 0, hits.getHits().length, new TopDocsAndMaxScore(topDocs, hits.getMaxScore()), hits, null);
    }

    private static SearchResponse response(final InternalTopHits aggregation, final SearchHit... hits) {
        SearchHits searchHits = new SearchHits(hits, new TotalHits(hits.length, TotalHits.Relation.EQUAL_TO), 1.0f);
        InternalAggregations aggregations = Objects.isNull(aggregation) ? null : InternalAggregations.from(List.of(aggregation));
        InternalSearchResponse internal = new InternalSearchResponse(searchHits, aggregations, null, null, false, null, 1);
        return new SearchResponse(internal, null, 1, 1, 0, 1L, ShardSearchFailure.EMPTY_ARRAY, SearchResponse.Clusters.EMPTY);
    }
}
