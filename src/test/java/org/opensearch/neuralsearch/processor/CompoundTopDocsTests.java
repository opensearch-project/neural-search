/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.processor;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.apache.commons.lang3.RandomUtils;
import org.apache.lucene.search.FieldDoc;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.TotalHits;
import org.opensearch.neuralsearch.query.OpenSearchQueryTestCase;

public class CompoundTopDocsTests extends OpenSearchQueryTestCase {
    private static final SearchShard SEARCH_SHARD = new SearchShard("my_index", 0, "12345678");

    public void testBasics_whenCreateWithTopDocsArray_thenSuccessful() {
        TopDocs topDocs1 = new TopDocs(
            new TotalHits(2, TotalHits.Relation.EQUAL_TO),
            new ScoreDoc[] { new ScoreDoc(0, RandomUtils.nextFloat()), new ScoreDoc(1, RandomUtils.nextFloat()) }
        );
        TopDocs topDocs2 = new TopDocs(
            new TotalHits(3, TotalHits.Relation.EQUAL_TO),
            new ScoreDoc[] {
                new ScoreDoc(2, RandomUtils.nextFloat()),
                new ScoreDoc(4, RandomUtils.nextFloat()),
                new ScoreDoc(5, RandomUtils.nextFloat()) }
        );
        List<TopDocs> topDocs = List.of(topDocs1, topDocs2);
        CompoundTopDocs compoundTopDocs = new CompoundTopDocs(new TotalHits(3, TotalHits.Relation.EQUAL_TO), topDocs, false, SEARCH_SHARD);
        assertNotNull(compoundTopDocs);
        assertEquals(topDocs, compoundTopDocs.getTopDocs());
    }

    public void testBasics_whenCreateWithoutTopDocs_thenTopDocsIsNull() {
        CompoundTopDocs hybridQueryScoreTopDocs = new CompoundTopDocs(
            new TotalHits(3, TotalHits.Relation.EQUAL_TO),
            List.of(
                new TopDocs(
                    new TotalHits(3, TotalHits.Relation.EQUAL_TO),
                    new ScoreDoc[] {
                        new ScoreDoc(2, RandomUtils.nextFloat()),
                        new ScoreDoc(4, RandomUtils.nextFloat()),
                        new ScoreDoc(5, RandomUtils.nextFloat()) }
                )
            ),
            false,
            SEARCH_SHARD
        );
        assertNotNull(hybridQueryScoreTopDocs);
        assertNotNull(hybridQueryScoreTopDocs.getScoreDocs());
        assertNotNull(hybridQueryScoreTopDocs.getTopDocs());
    }

    public void testBasics_whenMultipleTopDocsOfDifferentLength_thenReturnTopDocsWithMostHits() {
        TopDocs topDocs1 = new TopDocs(new TotalHits(0, TotalHits.Relation.EQUAL_TO), null);
        TopDocs topDocs2 = new TopDocs(
            new TotalHits(2, TotalHits.Relation.EQUAL_TO),
            new ScoreDoc[] { new ScoreDoc(2, RandomUtils.nextFloat()), new ScoreDoc(4, RandomUtils.nextFloat()) }
        );
        List<TopDocs> topDocs = List.of(topDocs1, topDocs2);
        CompoundTopDocs compoundTopDocs = new CompoundTopDocs(new TotalHits(2, TotalHits.Relation.EQUAL_TO), topDocs, false, SEARCH_SHARD);
        assertNotNull(compoundTopDocs);
        assertNotNull(compoundTopDocs.getScoreDocs());
        assertEquals(2, compoundTopDocs.getScoreDocs().size());
    }

    public void testBasics_whenMultipleTopDocsIsNull_thenScoreDocsIsNull() {
        CompoundTopDocs compoundTopDocs = new CompoundTopDocs(
            new TotalHits(0, TotalHits.Relation.EQUAL_TO),
            (List<TopDocs>) null,
            false,
            SEARCH_SHARD
        );
        assertNotNull(compoundTopDocs);
        assertNull(compoundTopDocs.getScoreDocs());

        CompoundTopDocs compoundTopDocsWithNullArray = new CompoundTopDocs(
            new TotalHits(0, TotalHits.Relation.EQUAL_TO),
            Arrays.asList(null, null),
            false,
            SEARCH_SHARD
        );
        assertNotNull(compoundTopDocsWithNullArray);
        assertNotNull(compoundTopDocsWithNullArray.getScoreDocs());
        assertEquals(0, compoundTopDocsWithNullArray.getScoreDocs().size());
    }

    public void testEqualsWithIdenticalCompoundTopDocs() {
        TopDocs topDocs1 = new TopDocs(new TotalHits(1, TotalHits.Relation.EQUAL_TO), new ScoreDoc[] { new ScoreDoc(1, 1.0f) });
        TopDocs topDocs2 = new TopDocs(new TotalHits(2, TotalHits.Relation.EQUAL_TO), new ScoreDoc[] { new ScoreDoc(2, 2.0f) });
        List<TopDocs> topDocsList = Arrays.asList(topDocs1, topDocs2);

        CompoundTopDocs first = new CompoundTopDocs(new TotalHits(3, TotalHits.Relation.EQUAL_TO), topDocsList, false, SEARCH_SHARD);
        CompoundTopDocs second = new CompoundTopDocs(new TotalHits(3, TotalHits.Relation.EQUAL_TO), topDocsList, false, SEARCH_SHARD);

        assertTrue(first.equals(second));
        assertTrue(second.equals(first));
        assertEquals(first.hashCode(), second.hashCode());
    }

    public void testEqualsWithDifferentScoreDocs() {
        TopDocs topDocs1 = new TopDocs(new TotalHits(1, TotalHits.Relation.EQUAL_TO), new ScoreDoc[] { new ScoreDoc(1, 1.0f) });
        TopDocs topDocs2 = new TopDocs(new TotalHits(1, TotalHits.Relation.EQUAL_TO), new ScoreDoc[] { new ScoreDoc(1, 2.0f) });

        CompoundTopDocs first = new CompoundTopDocs(
            new TotalHits(1, TotalHits.Relation.EQUAL_TO),
            Collections.singletonList(topDocs1),
            false,
            SEARCH_SHARD
        );
        CompoundTopDocs second = new CompoundTopDocs(
            new TotalHits(1, TotalHits.Relation.EQUAL_TO),
            Collections.singletonList(topDocs2),
            false,
            SEARCH_SHARD
        );

        assertFalse(first.equals(second));
        assertFalse(second.equals(first));
    }

    public void testEqualsWithDifferentTotalHits() {
        TopDocs topDocs = new TopDocs(new TotalHits(1, TotalHits.Relation.EQUAL_TO), new ScoreDoc[] { new ScoreDoc(1, 1.0f) });

        CompoundTopDocs first = new CompoundTopDocs(
            new TotalHits(1, TotalHits.Relation.EQUAL_TO),
            Collections.singletonList(topDocs),
            false,
            SEARCH_SHARD
        );
        CompoundTopDocs second = new CompoundTopDocs(
            new TotalHits(2, TotalHits.Relation.EQUAL_TO),
            Collections.singletonList(topDocs),
            false,
            SEARCH_SHARD
        );

        assertFalse(first.equals(second));
        assertFalse(second.equals(first));
    }

    public void testEqualsWithDifferentSortEnabled() {
        Object[] fields = new Object[] { "value1" };
        ScoreDoc scoreDoc = new FieldDoc(1, 1.0f, fields);  // use FieldDoc when sort is enabled
        TopDocs topDocs = new TopDocs(new TotalHits(1, TotalHits.Relation.EQUAL_TO), new ScoreDoc[] { scoreDoc });

        CompoundTopDocs first = new CompoundTopDocs(
            new TotalHits(1, TotalHits.Relation.EQUAL_TO),
            Collections.singletonList(topDocs),
            true,
            SEARCH_SHARD
        );

        // non-sorted case, use regular ScoreDoc
        TopDocs topDocs2 = new TopDocs(new TotalHits(1, TotalHits.Relation.EQUAL_TO), new ScoreDoc[] { new ScoreDoc(1, 1.0f) });

        CompoundTopDocs second = new CompoundTopDocs(
            new TotalHits(1, TotalHits.Relation.EQUAL_TO),
            Collections.singletonList(topDocs2),
            false,
            SEARCH_SHARD
        );

        assertNotEquals(first, second);
        assertNotEquals(second, first);
    }

    public void testEqualsWithDifferentSearchShards() {
        TopDocs topDocs = new TopDocs(new TotalHits(1, TotalHits.Relation.EQUAL_TO), new ScoreDoc[] { new ScoreDoc(1, 1.0f) });

        CompoundTopDocs first = new CompoundTopDocs(
            new TotalHits(1, TotalHits.Relation.EQUAL_TO),
            Collections.singletonList(topDocs),
            false,
            SEARCH_SHARD
        );
        CompoundTopDocs second = new CompoundTopDocs(
            new TotalHits(1, TotalHits.Relation.EQUAL_TO),
            Collections.singletonList(topDocs),
            false,
            new SearchShard("my_index", 1, "23456789")
        );

        assertNotEquals(first, second);
        assertNotEquals(second, first);
    }

    public void testEqualsWithFieldDocs() {
        Object[] fields1 = new Object[] { "value1" };
        Object[] fields2 = new Object[] { "value1" };
        TopDocs topDocs1 = new TopDocs(new TotalHits(1, TotalHits.Relation.EQUAL_TO), new FieldDoc[] { new FieldDoc(1, 1.0f, fields1) });
        TopDocs topDocs2 = new TopDocs(new TotalHits(1, TotalHits.Relation.EQUAL_TO), new FieldDoc[] { new FieldDoc(1, 1.0f, fields2) });

        CompoundTopDocs first = new CompoundTopDocs(
            new TotalHits(1, TotalHits.Relation.EQUAL_TO),
            Collections.singletonList(topDocs1),
            false,
            SEARCH_SHARD
        );
        CompoundTopDocs second = new CompoundTopDocs(
            new TotalHits(1, TotalHits.Relation.EQUAL_TO),
            Collections.singletonList(topDocs2),
            false,
            SEARCH_SHARD
        );

        assertEquals(first, second);
        assertEquals(second, first);
        assertEquals(first.hashCode(), second.hashCode());
    }

    public void testEqualsWithNull() {
        TopDocs topDocs = new TopDocs(new TotalHits(1, TotalHits.Relation.EQUAL_TO), new ScoreDoc[] { new ScoreDoc(1, 1.0f) });
        CompoundTopDocs compoundTopDocs = new CompoundTopDocs(
            new TotalHits(1, TotalHits.Relation.EQUAL_TO),
            Collections.singletonList(topDocs),
            false,
            SEARCH_SHARD
        );

        assertNotEquals(null, compoundTopDocs);
    }

    public void testEqualsWithDifferentClass() {
        TopDocs topDocs = new TopDocs(new TotalHits(1, TotalHits.Relation.EQUAL_TO), new ScoreDoc[] { new ScoreDoc(1, 1.0f) });
        CompoundTopDocs compoundTopDocs = new CompoundTopDocs(
            new TotalHits(1, TotalHits.Relation.EQUAL_TO),
            Collections.singletonList(topDocs),
            false,
            SEARCH_SHARD
        );

        assertNotEquals("not a CompoundTopDocs", compoundTopDocs);
    }
}
