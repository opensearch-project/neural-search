/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.search;

import java.util.Map;

import org.apache.lucene.search.SortField;
import org.apache.lucene.search.TotalHits;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.action.search.SearchResponseSections;
import org.opensearch.action.search.ShardSearchFailure;
import org.opensearch.search.SearchHit;
import org.opensearch.search.SearchHits;
import org.opensearch.test.OpenSearchTestCase;

public class FusedTotalHitsMergerTests extends OpenSearchTestCase {

    private static SearchResponse responseWithOneHit(TotalHits totalHits) {
        SearchHit hit = new SearchHit(0, "1", Map.of(), Map.of());
        hit.score(0.7f);
        SearchHits hits = new SearchHits(
            new SearchHit[] { hit },
            totalHits,
            0.7f,
            new SortField[] { SortField.FIELD_SCORE },
            "grp",
            new Object[] { "g1" }
        );
        SearchResponseSections sections = new SearchResponseSections(hits, null, null, false, false, null, 1);
        return new SearchResponse(sections, null, 1, 1, 0, 10, ShardSearchFailure.EMPTY_ARRAY, null);
    }

    public void testGetMergedResponse_whenNothingDerived_thenTheResponseIsReturnedAsIs() {
        FusedTotalHitsMerger merger = new FusedTotalHitsMerger();
        SearchResponse response = responseWithOneHit(new TotalHits(1, TotalHits.Relation.EQUAL_TO));
        assertSame(response, merger.getMergedResponse(response));
        merger.consumer().accept(null);
        assertSame(response, merger.getMergedResponse(response));
    }

    public void testGetMergedResponse_whenATotalWasDerived_thenOnlyTheTotalChanges() {
        FusedTotalHitsMerger merger = new FusedTotalHitsMerger();
        merger.consumer().accept(new TotalHits(10_000, TotalHits.Relation.GREATER_THAN_OR_EQUAL_TO));
        SearchResponse response = responseWithOneHit(new TotalHits(1, TotalHits.Relation.EQUAL_TO));

        SearchResponse merged = merger.getMergedResponse(response);

        assertEquals(10_000L, merged.getHits().getTotalHits().value());
        assertEquals(TotalHits.Relation.GREATER_THAN_OR_EQUAL_TO, merged.getHits().getTotalHits().relation());
        assertSame("the hit instances ride through", response.getHits().getHits()[0], merged.getHits().getHits()[0]);
        assertEquals(0.7f, merged.getHits().getMaxScore(), 0.0f);
        assertEquals("sort fields are carried", SortField.FIELD_SCORE, merged.getHits().getSortFields()[0]);
        assertEquals("collapse metadata is carried", "grp", merged.getHits().getCollapseField());
        assertEquals(response.getTook(), merged.getTook());
        assertEquals(response.getTotalShards(), merged.getTotalShards());
    }

    public void testGetMergedResponse_whenTheResponseCarriesNoHits_thenItIsReturnedAsIs() {
        FusedTotalHitsMerger merger = new FusedTotalHitsMerger();
        merger.consumer().accept(new TotalHits(10_000, TotalHits.Relation.GREATER_THAN_OR_EQUAL_TO));
        SearchResponse hitless = org.mockito.Mockito.mock(SearchResponse.class);
        org.mockito.Mockito.when(hitless.getHits()).thenReturn(null);
        assertSame(hitless, merger.getMergedResponse(hitless));
    }
}
