/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.search.util;

import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.SortField;
import org.apache.lucene.search.TotalHits;
import org.apache.lucene.search.grouping.CollapseTopFieldDocs;
import org.apache.lucene.util.BytesRef;
import org.opensearch.neuralsearch.processor.CompoundTopDocs;
import org.opensearch.test.OpenSearchTestCase;

import java.util.Collections;
import java.util.List;

/**
 * Unit tests for {@link HybridSearchCollapseUtil}
 */
public class HybridSearchCollapseUtilTests extends OpenSearchTestCase {

    private static final String COLLAPSE_FIELD = "category";
    private static final SortField[] SORT_FIELDS = new SortField[] { SortField.FIELD_SCORE };

    /**
     * Test getCollapseFieldType with BytesRef collapse values (keyword fields)
     */
    public void testGetCollapseFieldType_whenBytesRefCollapseValues_thenReturnsBytesRefClass() {
        // Create CollapseTopFieldDocs with BytesRef collapse values
        CollapseTopFieldDocs collapseTopFieldDocs = new CollapseTopFieldDocs(
            COLLAPSE_FIELD,
            new TotalHits(2, TotalHits.Relation.EQUAL_TO),
            new ScoreDoc[] { new ScoreDoc(1, 1.0f), new ScoreDoc(2, 0.8f) },
            SORT_FIELDS,
            new Object[] { new BytesRef("electronics"), new BytesRef("books") }
        );

        CompoundTopDocs compoundTopDocs = new CompoundTopDocs(
            new TotalHits(2, TotalHits.Relation.EQUAL_TO),
            List.of(collapseTopFieldDocs),
            null,
            null
        );

        List<CompoundTopDocs> queryTopDocs = List.of(compoundTopDocs);

        Class<?> result = HybridSearchCollapseUtil.getCollapseFieldType(queryTopDocs);

        assertEquals(BytesRef.class, result);
    }

    /**
     * Test getCollapseFieldType with Long collapse values (numeric fields)
     */
    public void testGetCollapseFieldType_whenLongCollapseValues_thenReturnsLongClass() {
        // Create CollapseTopFieldDocs with Long collapse values
        CollapseTopFieldDocs collapseTopFieldDocs = new CollapseTopFieldDocs(
            COLLAPSE_FIELD,
            new TotalHits(3, TotalHits.Relation.EQUAL_TO),
            new ScoreDoc[] { new ScoreDoc(1, 1.0f), new ScoreDoc(2, 0.8f), new ScoreDoc(3, 0.6f) },
            SORT_FIELDS,
            new Object[] { 1L, 2L, 3L }
        );

        CompoundTopDocs compoundTopDocs = new CompoundTopDocs(
            new TotalHits(3, TotalHits.Relation.EQUAL_TO),
            List.of(collapseTopFieldDocs),
            null,
            null
        );

        List<CompoundTopDocs> queryTopDocs = List.of(compoundTopDocs);

        Class<?> result = HybridSearchCollapseUtil.getCollapseFieldType(queryTopDocs);

        assertEquals(Long.class, result);
    }

    /**
     * Test getCollapseFieldType with empty collapse values
     */
    public void testGetCollapseFieldType_whenEmptyCollapseValues_thenReturnsNull() {
        // Create CollapseTopFieldDocs with empty collapse values
        CollapseTopFieldDocs collapseTopFieldDocs = new CollapseTopFieldDocs(
            COLLAPSE_FIELD,
            new TotalHits(0, TotalHits.Relation.EQUAL_TO),
            new ScoreDoc[0],
            SORT_FIELDS,
            new Object[0]  // Empty collapse values
        );

        CompoundTopDocs compoundTopDocs = new CompoundTopDocs(
            new TotalHits(0, TotalHits.Relation.EQUAL_TO),
            List.of(collapseTopFieldDocs),
            null,
            null
        );

        List<CompoundTopDocs> queryTopDocs = List.of(compoundTopDocs);

        Class<?> result = HybridSearchCollapseUtil.getCollapseFieldType(queryTopDocs);

        assertNull(result);
    }

    /**
     * Test getCollapseFieldType with empty CompoundTopDocs list
     */
    public void testGetCollapseFieldType_whenEmptyQueryTopDocs_thenReturnsNull() {
        List<CompoundTopDocs> queryTopDocs = Collections.emptyList();
        assertThrows(IllegalStateException.class, () -> HybridSearchCollapseUtil.getCollapseFieldType(queryTopDocs));
    }

    /**
     * Test getCollapseFieldType with CompoundTopDocs containing empty TopDocs list
     */
    public void testGetCollapseFieldType_whenEmptyTopDocsList_thenReturnsNull() {
        CompoundTopDocs compoundTopDocs = new CompoundTopDocs(
            new TotalHits(0, TotalHits.Relation.EQUAL_TO),
            Collections.emptyList(),  // Empty TopDocs list
            null,
            null
        );

        List<CompoundTopDocs> queryTopDocs = List.of(compoundTopDocs);

        Class<?> result = HybridSearchCollapseUtil.getCollapseFieldType(queryTopDocs);

        assertNull(result);
    }

    /**
     * Test getCollapseFieldType with multiple sub-queries where first has empty results
     * but second has valid collapse values
     */
    public void testGetCollapseFieldType_whenFirstSubQueryEmptyButSecondHasResults_thenReturnsCorrectType() {
        // First sub-query with empty results
        CollapseTopFieldDocs emptyCollapseTopFieldDocs = new CollapseTopFieldDocs(
            COLLAPSE_FIELD,
            new TotalHits(0, TotalHits.Relation.EQUAL_TO),
            new ScoreDoc[0],
            SORT_FIELDS,
            new Object[0]
        );

        // Second sub-query with BytesRef results
        CollapseTopFieldDocs validCollapseTopFieldDocs = new CollapseTopFieldDocs(
            COLLAPSE_FIELD,
            new TotalHits(1, TotalHits.Relation.EQUAL_TO),
            new ScoreDoc[] { new ScoreDoc(1, 1.0f) },
            SORT_FIELDS,
            new Object[] { new BytesRef("category1") }
        );

        CompoundTopDocs compoundTopDocs = new CompoundTopDocs(
            new TotalHits(1, TotalHits.Relation.EQUAL_TO),
            List.of(emptyCollapseTopFieldDocs, validCollapseTopFieldDocs),
            null,
            null
        );

        List<CompoundTopDocs> queryTopDocs = List.of(compoundTopDocs);

        Class<?> result = HybridSearchCollapseUtil.getCollapseFieldType(queryTopDocs);

        assertEquals(BytesRef.class, result);
    }

    /**
     * Test getCollapseFieldType with multiple CompoundTopDocs where first has no results
     * but second has valid collapse values
     */
    public void testGetCollapseFieldType_whenFirstCompoundTopDocsEmptyButSecondHasResults_thenReturnsCorrectType() {
        // First CompoundTopDocs with empty results
        CompoundTopDocs emptyCompoundTopDocs = new CompoundTopDocs(
            new TotalHits(0, TotalHits.Relation.EQUAL_TO),
            Collections.emptyList(),
            null,
            null
        );

        // Second CompoundTopDocs with valid results
        CollapseTopFieldDocs validCollapseTopFieldDocs = new CollapseTopFieldDocs(
            COLLAPSE_FIELD,
            new TotalHits(1, TotalHits.Relation.EQUAL_TO),
            new ScoreDoc[] { new ScoreDoc(1, 1.0f) },
            SORT_FIELDS,
            new Object[] { 42L }
        );

        CompoundTopDocs validCompoundTopDocs = new CompoundTopDocs(
            new TotalHits(1, TotalHits.Relation.EQUAL_TO),
            List.of(validCollapseTopFieldDocs),
            null,
            null
        );

        List<CompoundTopDocs> queryTopDocs = List.of(emptyCompoundTopDocs, validCompoundTopDocs);

        Class<?> result = HybridSearchCollapseUtil.getCollapseFieldType(queryTopDocs);

        assertEquals(Long.class, result);
    }

    /**
     * Test getCollapseFieldType with mixed sub-queries where some have null collapse values
     * and others have valid values
     */
    public void testGetCollapseFieldType_whenMixedNullAndValidCollapseValues_thenReturnsValidType() {
        // First sub-query with null collapse values
        CollapseTopFieldDocs nullCollapseTopFieldDocs = new CollapseTopFieldDocs(
            COLLAPSE_FIELD,
            new TotalHits(0, TotalHits.Relation.EQUAL_TO),
            new ScoreDoc[0],
            SORT_FIELDS,
            null
        );

        // Second sub-query with empty collapse values
        CollapseTopFieldDocs emptyCollapseTopFieldDocs = new CollapseTopFieldDocs(
            COLLAPSE_FIELD,
            new TotalHits(0, TotalHits.Relation.EQUAL_TO),
            new ScoreDoc[0],
            SORT_FIELDS,
            new Object[0]
        );

        // Third sub-query with valid BytesRef collapse values
        CollapseTopFieldDocs validCollapseTopFieldDocs = new CollapseTopFieldDocs(
            COLLAPSE_FIELD,
            new TotalHits(2, TotalHits.Relation.EQUAL_TO),
            new ScoreDoc[] { new ScoreDoc(1, 1.0f), new ScoreDoc(2, 0.8f) },
            SORT_FIELDS,
            new Object[] { new BytesRef("electronics"), new BytesRef("books") }
        );

        CompoundTopDocs compoundTopDocs = new CompoundTopDocs(
            new TotalHits(2, TotalHits.Relation.EQUAL_TO),
            List.of(nullCollapseTopFieldDocs, emptyCollapseTopFieldDocs, validCollapseTopFieldDocs),
            null,
            null
        );

        List<CompoundTopDocs> queryTopDocs = List.of(compoundTopDocs);

        Class<?> result = HybridSearchCollapseUtil.getCollapseFieldType(queryTopDocs);

        assertEquals(BytesRef.class, result);
    }
}
