/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.search;

import java.util.Map;

import org.apache.lucene.search.TotalHits;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.action.search.SearchResponseSections;
import org.opensearch.action.search.ShardSearchFailure;
import org.opensearch.search.SearchHit;
import org.opensearch.search.SearchHits;
import org.opensearch.test.OpenSearchTestCase;

public class FusedHitsMergerTests extends OpenSearchTestCase {

    private static SearchResponse emptyResponse() {
        SearchHits hits = new SearchHits(new SearchHit[0], new TotalHits(0, TotalHits.Relation.EQUAL_TO), Float.NaN);
        SearchResponseSections sections = new SearchResponseSections(hits, null, null, false, false, null, 1);
        return new SearchResponse(sections, null, 3, 3, 0, 10, ShardSearchFailure.EMPTY_ARRAY, null);
    }

    public void testGetMergedResponse_whenNothingAssembled_thenTheResponseIsReturnedAsIs() {
        FusedHitsMerger merger = new FusedHitsMerger();
        SearchResponse response = emptyResponse();
        assertSame(response, merger.getMergedResponse(response));
        merger.consumer().accept(null);
        assertSame(response, merger.getMergedResponse(response));
    }

    public void testGetMergedResponse_whenAPageWasAssembled_thenItReplacesTheHitsAndNothingElse() {
        FusedHitsMerger merger = new FusedHitsMerger();
        SearchHit hit = new SearchHit(0, "1", Map.of(), Map.of());
        hit.score(0.6f);
        SearchHits page = new SearchHits(new SearchHit[] { hit }, new TotalHits(10_000, TotalHits.Relation.GREATER_THAN_OR_EQUAL_TO), 0.6f);
        merger.consumer().accept(page);
        SearchResponse response = emptyResponse();

        SearchResponse merged = merger.getMergedResponse(response);

        assertSame(page, merged.getHits());
        assertEquals(response.getTook(), merged.getTook());
        assertEquals(response.getTotalShards(), merged.getTotalShards());
        assertEquals(response.getSuccessfulShards(), merged.getSuccessfulShards());
        assertFalse(merged.isTimedOut());
    }

    public void testConsumer_whenAcceptedTwice_thenTheLastPageWins() {
        FusedHitsMerger merger = new FusedHitsMerger();
        SearchHits first = new SearchHits(new SearchHit[0], new TotalHits(0, TotalHits.Relation.EQUAL_TO), Float.NaN);
        SearchHits second = new SearchHits(new SearchHit[0], new TotalHits(1, TotalHits.Relation.EQUAL_TO), Float.NaN);
        merger.consumer().accept(first);
        merger.consumer().accept(second);
        SearchResponse merged = merger.getMergedResponse(emptyResponse());
        assertEquals(1, merged.getHits().getTotalHits().value());
    }
}
