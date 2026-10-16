/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.search;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.lucene.search.SortField;
import org.apache.lucene.search.TotalHits;
import org.opensearch.action.OriginalIndices;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.action.search.SearchResponseSections;
import org.opensearch.action.search.ShardSearchFailure;
import org.opensearch.core.index.Index;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.search.SearchHit;
import org.opensearch.search.SearchHits;
import org.opensearch.search.SearchShardTarget;
import org.opensearch.test.OpenSearchTestCase;

public class FusedRankedPageTrimmerTests extends OpenSearchTestCase {

    private static final TotalHits UNION = new TotalHits(55, TotalHits.Relation.EQUAL_TO);

    private static SearchHit hit(String index, String id, float score) {
        SearchHit hit = new SearchHit(0, id, Map.of(), Map.of());
        hit.score(score);
        hit.shard(new SearchShardTarget("node-1", new ShardId(new Index(index, index + "-uuid"), 0), null, OriginalIndices.NONE));
        return hit;
    }

    private static SearchResponse response(SearchHit[] hits, Object[] collapseValues) {
        SearchHits searchHits = new SearchHits(hits, UNION, 0.9f, new SortField[] { SortField.FIELD_SCORE }, "grp", collapseValues);
        SearchResponseSections sections = new SearchResponseSections(searchHits, null, null, false, false, null, 1);
        return new SearchResponse(sections, null, 3, 3, 0, 10, ShardSearchFailure.EMPTY_ARRAY, null);
    }

    private static List<String> ids(SearchResponse response) {
        return Arrays.stream(response.getHits().getHits()).map(SearchHit::getId).collect(Collectors.toList());
    }

    public void testGetMergedResponse_whenNothingPublished_thenTheResponseIsReturnedAsIs() {
        SearchResponse response = response(new SearchHit[] { hit("idx", "a", 0.9f), hit("idx", "x", 0.0f) }, null);
        assertSame(response, new FusedRankedPageTrimmer().getMergedResponse(response));
    }

    public void testGetMergedResponse_whenEveryHitIsRanked_thenTheResponseIsReturnedAsIs() {
        FusedRankedPageTrimmer trimmer = new FusedRankedPageTrimmer();
        trimmer.consumer().accept(new String[] { "idx", "idx" }, new String[] { "a", "b" });
        SearchResponse response = response(new SearchHit[] { hit("idx", "a", 0.9f), hit("idx", "b", 0.5f) }, null);
        assertSame(response, trimmer.getMergedResponse(response));
    }

    public void testGetMergedResponse_whenThePageReachesPastTheRanking_thenOnlyRankedHitsRemain() {
        FusedRankedPageTrimmer trimmer = new FusedRankedPageTrimmer();
        trimmer.consumer().accept(new String[] { "idx", "idx", "other" }, new String[] { "a", "b", "c" });
        SearchHit a = hit("idx", "a", 0.9f);
        SearchHit b = hit("idx", "b", 0.5f);
        // "c" is ranked in another index only, so this "c" is a Tail-only document that happens to share the _id.
        SearchResponse response = response(
            new SearchHit[] { a, b, hit("idx", "x", 0.0f), hit("idx", "c", 0.0f) },
            new Object[] { "g1", "g2", "g3", "g4" }
        );

        SearchResponse trimmed = trimmer.getMergedResponse(response);

        assertEquals(List.of("a", "b"), ids(trimmed));
        assertSame("the hit instances ride through", a, trimmed.getHits().getHits()[0]);
        assertEquals("the total still counts the whole match set", UNION, trimmed.getHits().getTotalHits());
        assertEquals(0.9f, trimmed.getHits().getMaxScore(), 0.0f);
        assertEquals(SortField.FIELD_SCORE, trimmed.getHits().getSortFields()[0]);
        assertEquals("grp", trimmed.getHits().getCollapseField());
        assertArrayEquals("collapse values stay parallel to the hits", new Object[] { "g1", "g2" }, trimmed.getHits().getCollapseValues());
        assertEquals(response.getTotalShards(), trimmed.getTotalShards());
    }

    public void testGetMergedResponse_whenAHitCarriesNoId_thenThePageIsReturnedAsIs() {
        // Fused mode refuses stored_fields: "_none_", which builds hits without an _id; should one arrive anyway, membership
        // cannot be decided, so round 2's page is kept as it is.
        FusedRankedPageTrimmer trimmer = new FusedRankedPageTrimmer();
        trimmer.consumer().accept(new String[] { "idx" }, new String[] { "a" });
        SearchHit idless = new SearchHit(0, null, Map.of(), Map.of());
        idless.shard(new SearchShardTarget("node-1", new ShardId(new Index("idx", "idx-uuid"), 0), null, OriginalIndices.NONE));
        SearchResponse response = response(new SearchHit[] { idless, hit("idx", "x", 0.0f) }, null);
        assertSame(response, trimmer.getMergedResponse(response));
    }

    public void testGetMergedResponse_whenAHitCarriesNoIndex_thenThePageIsReturnedAsIs() {
        FusedRankedPageTrimmer trimmer = new FusedRankedPageTrimmer();
        trimmer.consumer().accept(new String[] { "idx" }, new String[] { "a" });
        SearchResponse response = response(new SearchHit[] { new SearchHit(0, "a", Map.of(), Map.of()), hit("idx", "x", 0.0f) }, null);
        assertSame(response, trimmer.getMergedResponse(response));
    }

    public void testGetMergedResponse_whenTheResponseCarriesNoHits_thenItIsReturnedAsIs() {
        FusedRankedPageTrimmer trimmer = new FusedRankedPageTrimmer();
        trimmer.consumer().accept(new String[] { "idx" }, new String[] { "a" });
        SearchResponse hitless = org.mockito.Mockito.mock(SearchResponse.class);
        org.mockito.Mockito.when(hitless.getHits()).thenReturn(null);
        assertSame(hitless, trimmer.getMergedResponse(hitless));
    }

    public void testGetMergedResponse_whenAHitComesFromAnIndexTheWindowNeverSaw_thenItIsDropped() {
        FusedRankedPageTrimmer trimmer = new FusedRankedPageTrimmer();
        trimmer.consumer().accept(new String[] { "idx" }, new String[] { "a" });
        SearchResponse response = response(new SearchHit[] { hit("idx", "a", 0.9f), hit("elsewhere", "a", 0.0f) }, null);
        assertEquals(List.of("a"), ids(trimmer.getMergedResponse(response)));
        assertEquals("idx", trimmer.getMergedResponse(response).getHits().getHits()[0].getIndex());
    }

    public void testGetMergedResponse_whenCollapseValuesAreNotOnePerHit_thenTheyAreCarriedAsTheyCame() {
        FusedRankedPageTrimmer trimmer = new FusedRankedPageTrimmer();
        trimmer.consumer().accept(new String[] { "idx" }, new String[] { "a" });
        Object[] notParallel = new Object[] { "g1" };
        SearchResponse response = response(new SearchHit[] { hit("idx", "a", 0.9f), hit("idx", "x", 0.0f) }, notParallel);
        assertSame(notParallel, trimmer.getMergedResponse(response).getHits().getCollapseValues());
    }

    public void testGetMergedResponse_whenThePageIsEntirelyPastTheRanking_thenItIsEmpty() {
        FusedRankedPageTrimmer trimmer = new FusedRankedPageTrimmer();
        trimmer.consumer().accept(new String[] { "idx" }, new String[] { "a" });
        SearchResponse response = response(new SearchHit[] { hit("idx", "x", 0.0f), hit("idx", "y", 0.0f) }, null);

        SearchResponse trimmed = trimmer.getMergedResponse(response);

        assertEquals(0, trimmed.getHits().getHits().length);
        assertEquals(UNION, trimmed.getHits().getTotalHits());
        assertEquals(
            "max_score is the best match's, as core reports it for a page past the matches",
            response.getHits().getMaxScore(),
            trimmed.getHits().getMaxScore(),
            0.0f
        );
        assertNull(trimmed.getHits().getCollapseValues());
    }

    /**
     * A search-pipeline response processor may hand back its own {@link SearchResponse} subclass (ml-commons' RAG processor
     * returns one that carries the generated answer in fields of its own). Trimming rebuilds a plain response, which would
     * drop those fields, so such a response is left exactly as it came — Tail-only documents and all.
     */
    public void testGetMergedResponse_whenTheResponseIsASubclass_thenItIsReturnedAsIs() {
        FusedRankedPageTrimmer trimmer = new FusedRankedPageTrimmer();
        trimmer.consumer().accept(new String[] { "idx" }, new String[] { "a" });
        SearchHits searchHits = new SearchHits(new SearchHit[] { hit("idx", "a", 0.9f), hit("idx", "x", 0.0f) }, UNION, 0.9f);
        SearchResponseSections sections = new SearchResponseSections(searchHits, null, null, false, false, null, 1);
        SearchResponse generated = new SearchResponse(sections, null, 3, 3, 0, 10, ShardSearchFailure.EMPTY_ARRAY, null) {
            // Stands in for a processor's subclass, e.g. GenerativeSearchResponse with its answer field.
        };

        assertFalse(FusedResponseRebuilder.carriesOnlyCoreFields(generated));
        assertSame("a subclass is never flattened into a plain response", generated, trimmer.getMergedResponse(generated));
        assertEquals("its page is untouched", 2, generated.getHits().getHits().length);
    }
}
