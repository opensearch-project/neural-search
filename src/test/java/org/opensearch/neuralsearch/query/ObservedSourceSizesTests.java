/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.query;

import java.util.OptionalLong;

import org.apache.lucene.search.TotalHits;
import org.junit.Before;
import org.opensearch.action.OriginalIndices;
import org.opensearch.core.common.bytes.BytesArray;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.search.SearchHit;
import org.opensearch.search.SearchHits;
import org.opensearch.search.SearchShardTarget;
import org.opensearch.search.builder.SearchSourceBuilder;
import org.opensearch.test.OpenSearchTestCase;

public class ObservedSourceSizesTests extends OpenSearchTestCase {

    @Before
    public void reset() {
        ObservedSourceSizes.clear();
    }

    public void testRecord_whenHitsCarrySource_thenTheMeanPerIndexIsLearned() {
        ObservedSourceSizes.record(new SearchSourceBuilder(), hits(hit("a", 100), hit("a", 300), hit("b", 1_000)));

        assertEquals(OptionalLong.of(200), ObservedSourceSizes.bytesPerDocument("a", new SearchSourceBuilder()));
        assertEquals(OptionalLong.of(1_000), ObservedSourceSizes.bytesPerDocument("b", new SearchSourceBuilder()));
        assertEquals("never seen", OptionalLong.empty(), ObservedSourceSizes.bytesPerDocument("c", new SearchSourceBuilder()));
        assertEquals(2, ObservedSourceSizes.size());
    }

    public void testRecord_whenObservedAgain_thenTheEstimateMovesByTheEwmaWeight() {
        ObservedSourceSizes.record(new SearchSourceBuilder(), hits(hit("a", 1_000)));
        ObservedSourceSizes.record(new SearchSourceBuilder(), hits(hit("a", 2_000)));
        // 1000 + 0.3 × (2000 − 1000) = 1300: follows the corpus without being captured by one page
        assertEquals(OptionalLong.of(1_300), ObservedSourceSizes.bytesPerDocument("a", new SearchSourceBuilder()));
    }

    public void testShapeKey_isCanonical() {
        SearchSourceBuilder unfiltered = new SearchSourceBuilder();
        SearchSourceBuilder explicitlyOn = new SearchSourceBuilder().fetchSource(true);
        SearchSourceBuilder ba = new SearchSourceBuilder().fetchSource(new String[] { "b", "a" }, new String[] { "z", "y" });
        SearchSourceBuilder ab = new SearchSourceBuilder().fetchSource(new String[] { "a", "b" }, new String[] { "y", "z" });
        SearchSourceBuilder other = new SearchSourceBuilder().fetchSource(new String[] { "a" }, null);

        assertEquals(ObservedSourceSizes.UNFILTERED, ObservedSourceSizes.shapeKey(null));
        assertEquals(ObservedSourceSizes.UNFILTERED, ObservedSourceSizes.shapeKey(unfiltered));
        assertEquals("_source: true is the unfiltered shape", ObservedSourceSizes.UNFILTERED, ObservedSourceSizes.shapeKey(explicitlyOn));
        assertEquals("order of patterns does not matter", ObservedSourceSizes.shapeKey(ba), ObservedSourceSizes.shapeKey(ab));
        assertNotEquals(ObservedSourceSizes.shapeKey(ab), ObservedSourceSizes.shapeKey(other));
        assertNotEquals(ObservedSourceSizes.shapeKey(ab), ObservedSourceSizes.UNFILTERED);

        // and the table keys by it: an observation under one filter says nothing about another
        ObservedSourceSizes.record(ba, hits(hit("a", 500)));
        assertEquals(OptionalLong.of(500), ObservedSourceSizes.bytesPerDocument("a", ab));
        assertEquals(OptionalLong.empty(), ObservedSourceSizes.bytesPerDocument("a", other));
        assertEquals(OptionalLong.empty(), ObservedSourceSizes.bytesPerDocument("a", unfiltered));
    }

    public void testRecord_whenSourceIsOffOrHitsCarryNone_thenNothingIsLearned() {
        SearchSourceBuilder sourceOff = new SearchSourceBuilder().fetchSource(false);
        ObservedSourceSizes.record(sourceOff, hits(hit("a", 100)));
        assertEquals("_source off: not a _source observation", 0, ObservedSourceSizes.size());
        assertFalse(ObservedSourceSizes.sourceRequested(sourceOff));
        assertTrue(ObservedSourceSizes.sourceRequested(null));

        SearchHit sourceless = new SearchHit(0, "1", null, null);
        sourceless.shard(new SearchShardTarget("node", new ShardId("a", "uuid", 0), null, OriginalIndices.NONE));
        ObservedSourceSizes.record(new SearchSourceBuilder(), hits(sourceless));
        assertEquals("hits without _source teach nothing", 0, ObservedSourceSizes.size());

        ObservedSourceSizes.record(new SearchSourceBuilder(), null);
        ObservedSourceSizes.record(
            new SearchSourceBuilder(),
            new SearchHits(new SearchHit[0], new TotalHits(0, TotalHits.Relation.EQUAL_TO), 0f)
        );
        assertEquals(0, ObservedSourceSizes.size());

        // a hit with source but no shard target (no index) is skipped, the others still count
        SearchHit noIndex = new SearchHit(0, "2", null, null);
        noIndex.sourceRef(new BytesArray("{\"body\":\"x\"}"));
        ObservedSourceSizes.record(new SearchSourceBuilder(), hits(noIndex, hit("a", 100)));
        assertEquals(OptionalLong.of(100), ObservedSourceSizes.bytesPerDocument("a", new SearchSourceBuilder()));
    }

    public void testRecord_whenCapacityIsReached_thenTheTableIsClearedAndRelearns() {
        for (int i = 0; i < ObservedSourceSizes.CAPACITY; i++) {
            ObservedSourceSizes.record(new SearchSourceBuilder(), hits(hit("index-" + i, 100)));
        }
        assertEquals(ObservedSourceSizes.CAPACITY, ObservedSourceSizes.size());
        ObservedSourceSizes.record(new SearchSourceBuilder(), hits(hit("one-more", 700)));
        assertEquals("cleared, then the newest observation stands alone", 1, ObservedSourceSizes.size());
        assertEquals(OptionalLong.of(700), ObservedSourceSizes.bytesPerDocument("one-more", new SearchSourceBuilder()));
        assertEquals(OptionalLong.empty(), ObservedSourceSizes.bytesPerDocument("index-0", new SearchSourceBuilder()));
    }

    private static SearchHit hit(String index, int sourceBytes) {
        SearchHit hit = new SearchHit(0, "1", null, null);
        hit.shard(new SearchShardTarget("node", new ShardId(index, "uuid", 0), null, OriginalIndices.NONE));
        String frame = "{\"body\":\"\"}";
        hit.sourceRef(new BytesArray(frame.substring(0, 9) + "x".repeat(Math.max(0, sourceBytes - frame.length())) + frame.substring(9)));
        return hit;
    }

    private static SearchHits hits(SearchHit... hits) {
        return new SearchHits(hits, new TotalHits(hits.length, TotalHits.Relation.EQUAL_TO), 1.0f);
    }
}
