/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.search.query.util;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.document.SortedDocValuesField;
import org.apache.lucene.document.SortedNumericDocValuesField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.NoMergePolicy;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.Collector;
import org.apache.lucene.search.Explanation;
import org.apache.lucene.search.FieldDoc;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.LeafCollector;
import org.apache.lucene.search.MultiCollector;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.SortField;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.TopFieldDocs;
import org.apache.lucene.search.TotalHitCountCollector;
import org.apache.lucene.search.TotalHits;
import org.apache.lucene.search.grouping.CollapseTopFieldDocs;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.BytesRef;
import org.opensearch.common.Nullable;
import org.opensearch.common.lucene.search.TopDocsAndMaxScore;
import org.opensearch.index.mapper.KeywordFieldMapper;
import org.opensearch.index.mapper.MappedFieldType;
import org.opensearch.index.mapper.NumberFieldMapper;
import org.opensearch.index.query.ParsedQuery;
import org.opensearch.search.DocValueFormat;
import org.opensearch.search.collapse.CollapseContext;
import org.opensearch.neuralsearch.query.HybridSubQueryScorer;
import org.opensearch.neuralsearch.query.OpenSearchQueryTestCase;
import org.opensearch.neuralsearch.search.HitsThresholdChecker;
import org.opensearch.neuralsearch.search.collector.HybridCollapsingTopDocsCollector;
import org.opensearch.neuralsearch.search.collector.HybridCollapsingTopGroupsCollector;
import org.opensearch.neuralsearch.search.collector.HybridSearchCollector;
import org.opensearch.neuralsearch.search.collector.HybridTopFieldDocSortCollector;
import org.opensearch.neuralsearch.search.collector.HybridTopScoreDocCollector;
import org.opensearch.neuralsearch.search.collector.PagingFieldCollector;
import org.opensearch.neuralsearch.search.collector.SimpleFieldCollector;
import org.opensearch.neuralsearch.search.query.HybridCollectorManager;
import org.opensearch.neuralsearch.search.query.HybridCollectorResultsUtilParams;
import org.opensearch.neuralsearch.search.query.exception.HybridSearchRescoreQueryException;
import org.opensearch.search.internal.ContextIndexSearcher;
import org.opensearch.search.internal.SearchContext;
import org.opensearch.search.query.QuerySearchResult;
import org.opensearch.search.rescore.QueryRescoreMode;
import org.opensearch.search.rescore.QueryRescorer;
import org.opensearch.search.rescore.RescoreContext;
import org.opensearch.search.rescore.Rescorer;
import org.opensearch.search.sort.SortAndFormats;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Locale;
import java.util.Objects;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyFloat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.opensearch.neuralsearch.search.util.HybridSearchResultFormatUtil.isHybridQueryDelimiterElement;
import static org.opensearch.neuralsearch.search.util.HybridSearchResultFormatUtil.isHybridQueryScoreDocElement;
import static org.opensearch.neuralsearch.search.util.HybridSearchResultFormatUtil.isHybridQueryStartStopElement;

/**
 * Shard-level reduce of a hybrid query under concurrent segment search
 * (https://github.com/opensearch-project/neural-search/issues/2091). Real {@link HybridTopScoreDocCollector}s and sort collectors,
 * one per simulated slice, are reduced by the real {@link HybridCollectorManager}; every sub-query must keep the hits, order, scores
 * and sort values, and the shard the max score and layout, that one collector over the whole shard produces. Every matching doc
 * reaches the collectors here, so the summed total equals one collector's too.
 */
public class HybridSliceResultsReducerTests extends OpenSearchQueryTestCase {

    private static final int NUM_HITS = 10;
    private static final int NUM_SUB_QUERIES = 4;
    private static final int SEGMENTS = 4;
    private static final int DOCS_PER_SEGMENT = 30;
    private static final int SLICES = 3;
    private static final int RESCORE_WINDOW = 4;
    private static final String TAG_FIELD = "tag";
    private static final String BOOSTED_TAG = "boost";
    private static final String PLAIN_TAG = "plain";
    private static final String PRICE_FIELD = "price";
    private static final String LABEL_FIELD = "label";
    private static final String GROUP_FIELD = "group";
    private static final String BUCKET_FIELD = "bucket";
    private static final Comparator<ScoreDoc> SCORE_DESC_DOC_ASC = Comparator.<ScoreDoc>comparingDouble(hit -> -hit.score)
        .thenComparingInt(hit -> hit.doc);

    /**
     * Scripted sub-query scores over global doc ids 0..119. Sub-query 0: values repeat every 13 docs, so each score is shared by
     * about 9 docs spread over all segments and the cut at NUM_HITS falls inside a tie. Sub-query 1: 11 docs share the top score,
     * so the cut falls inside a tie again. Sub-query 2 matches nothing. Sub-query 3 matches 3 docs, fewer than NUM_HITS, two tied;
     * its doc 47 holds the shard's max score, and only the second slice sees it.
     */
    private static float score(final int subQuery, final int globalDoc) {
        switch (subQuery) {
            case 0:
                return ((globalDoc * 7) % 13) * 0.5f;
            case 1:
                return ((globalDoc * 5) % 11) * 0.25f;
            case 2:
                return 0.0f;
            default:
                return globalDoc == 47 ? 7.0f : (globalDoc == 3 || globalDoc == 88) ? 1.0f : 0.0f;
        }
    }

    /** Numeric sort value with 11 distinct values, each shared by about 11 docs spread over all segments. */
    private static long price(final int globalDoc) {
        return (globalDoc * 7L) % 11;
    }

    /** Keyword sort value with 7 distinct values, each shared by about 17 docs spread over all segments. */
    private static String label(final int globalDoc) {
        return "l" + (globalDoc * 5) % 7;
    }

    /**
     * Scripted sub-query matches for the sorted tests: sub-query 0 matches 3 docs, sub-query 1 every third doc, sub-query 2
     * nothing, sub-query 3 every doc. Each sub-query gives its docs one score, because the sort collectors keep the score of the
     * entry a competitive hit replaces (pre-existing), so varying scores would depend on the collection order. And once a doc is
     * not competitive for a full queue, the sort collectors skip it for the later sub-queries too (pre-existing); here a sub-query
     * that fills its queue matches a subset of the docs of each later sub-query, so a skipped doc is never one a later queue keeps.
     */
    private static float sortedSubQueryScore(final int subQuery, final int globalDoc) {
        switch (subQuery) {
            case 0:
                return (globalDoc == 3 || globalDoc == 47 || globalDoc == 88) ? 1.0f : 0.0f;
            case 1:
                return globalDoc % 3 == 0 ? 2.0f : 0.0f;
            case 2:
                return 0.0f;
            default:
                return 3.0f;
        }
    }

    public void testReduce_whenSlicesWithTiesAtTheCut_thenEachSubQueryEqualsOneCollectorOverTheShard() throws IOException {
        // the fixture is only meaningful if the cut falls inside a score tie, so the doc id tie-break decides which docs stay
        assertCutFallsInsideATie(0);
        assertCutFallsInsideATie(1);
        try (Directory directory = newDirectory()) {
            indexSegments(directory);
            try (DirectoryReader reader = DirectoryReader.open(directory)) {
                final List<LeafReaderContext> leaves = reader.leaves();
                assertEquals(SEGMENTS, leaves.size());

                final TopDocsAndMaxScore oneCollector = reduce(List.of(collect(leaves)));
                final TopDocsAndMaxScore slices = reduce(sliceCollectors(leaves));

                // the reference itself: top NUM_HITS per sub-query, lower doc id first inside a tie
                final List<List<ScoreDoc>> expectedHits = subQueryHits(oneCollector.topDocs.scoreDocs);
                assertEquals(NUM_HITS, expectedHits.get(0).size());
                assertEquals(9, expectedHits.get(0).get(NUM_HITS - 1).doc);
                assertEquals(NUM_HITS, expectedHits.get(1).size());
                assertTrue(expectedHits.get(2).isEmpty());
                assertEquals(List.of(47, 3, 88), expectedHits.get(3).stream().map(hit -> hit.doc).toList());

                assertSameResult(oneCollector, slices);
                assertEquals(119, slices.topDocs.totalHits.value());
                assertEquals(7.0f, slices.maxScore, 0.0f);
                // start, stop, one delimiter per sub-query, then min(NUM_HITS, matches) hits per sub-query
                assertEquals(2 + NUM_SUB_QUERIES + 10 + 10 + 0 + 3, slices.topDocs.scoreDocs.length);
            }
        }
    }

    public void testReduce_whenOneCollector_thenSameEntriesAsThePerCollectorEncoding() throws IOException {
        try (Directory directory = newDirectory()) {
            indexSegments(directory);
            try (DirectoryReader reader = DirectoryReader.open(directory)) {
                final List<LeafReaderContext> leaves = reader.leaves();
                // today's path with concurrent segment search off encodes one collector the same way
                assertSameResult(withoutConcurrentSearch(mock(SearchContext.class), leaves), reduce(List.of(collect(leaves))));
            }
        }
    }

    public void testReduce_whenRescoreWindowIsSmallerThanSliceLists_thenOneWindowPerSubQueryOverTheShard() throws IOException {
        try (Directory directory = newDirectory()) {
            indexSegments(directory);
            try (DirectoryReader reader = DirectoryReader.open(directory)) {
                final List<Integer> rescoredListSizes = new ArrayList<>();
                final TopDocsAndMaxScore slices = reduceSlicesWithRescore(
                    reader,
                    List.of(queryRescoreContext(RESCORE_WINDOW, BOOSTED_TAG, 10.0f, rescoredListSizes)),
                    rescoredListSizes
                );

                // rescore must change the order, or this test proves nothing
                assertNotEquals(
                    subQueryHits(reduce(List.of(collect(reader.leaves()))).topDocs.scoreDocs).get(0).stream().map(hit -> hit.doc).toList(),
                    subQueryHits(slices.topDocs.scoreDocs).get(0).stream().map(hit -> hit.doc).toList()
                );
                // one rescore call per sub-query over the merged shard list; the stock fold made one per slice and sub-query
                assertEquals(List.of(NUM_HITS, NUM_HITS, 0, 3), rescoredListSizes);
            }
        }
    }

    public void testReduce_whenRescoreWindowIsLargerThanNumHits_thenRescoreGetsNumHitsPerSubQuery() throws IOException {
        try (Directory directory = newDirectory()) {
            indexSegments(directory);
            try (DirectoryReader reader = DirectoryReader.open(directory)) {
                final List<Integer> rescoredListSizes = new ArrayList<>();
                reduceSlicesWithRescore(
                    reader,
                    List.of(queryRescoreContext(2 * NUM_HITS, BOOSTED_TAG, 10.0f, rescoredListSizes)),
                    rescoredListSizes
                );

                // the merge cuts at numHits before rescore, as one collector does, whatever the window
                assertEquals(List.of(NUM_HITS, NUM_HITS, 0, 3), rescoredListSizes);
            }
        }
    }

    public void testReduce_whenTwoRescoreContexts_thenEachRescoresTheMergedListsInOrder() throws IOException {
        try (Directory directory = newDirectory()) {
            indexSegments(directory);
            try (DirectoryReader reader = DirectoryReader.open(directory)) {
                final List<Integer> rescoredListSizes = new ArrayList<>();
                reduceSlicesWithRescore(
                    reader,
                    List.of(
                        queryRescoreContext(RESCORE_WINDOW, BOOSTED_TAG, 10.0f, rescoredListSizes),
                        queryRescoreContext(RESCORE_WINDOW + 2, PLAIN_TAG, 2.0f, rescoredListSizes)
                    ),
                    rescoredListSizes
                );

                assertEquals(List.of(NUM_HITS, NUM_HITS, 0, 3, NUM_HITS, NUM_HITS, 0, 3), rescoredListSizes);
            }
        }
    }

    public void testReduce_whenRescorerFails_thenHybridSearchRescoreQueryException() throws IOException {
        try (Directory directory = newDirectory()) {
            indexSegments(directory);
            try (DirectoryReader reader = DirectoryReader.open(directory)) {
                final SearchContext searchContext = searchContextWithRescore(
                    reader,
                    List.of(new RescoreContext(RESCORE_WINDOW, (TestRescorer) (topDocs, searcher, context) -> {
                        throw new IOException("rescore failed");
                    }))
                );

                expectThrows(HybridSearchRescoreQueryException.class, () -> reduce(sliceCollectors(reader.leaves()), searchContext));
            }
        }
    }

    public void testReduce_whenTotalHitsTrackingIsDisabled_thenShardTotalIsALowerBound() throws IOException {
        try (Directory directory = newDirectory()) {
            indexSegments(directory);
            try (DirectoryReader reader = DirectoryReader.open(directory)) {
                final SearchContext searchContext = mock(SearchContext.class);
                when(searchContext.trackTotalHitsUpTo()).thenReturn(SearchContext.TRACK_TOTAL_HITS_DISABLED);

                final TopDocsAndMaxScore slices = reduce(sliceCollectors(reader.leaves()), searchContext);

                assertEquals(new TotalHits(119, TotalHits.Relation.GREATER_THAN_OR_EQUAL_TO), slices.topDocs.totalHits);
                assertSameResult(withoutConcurrentSearch(searchContext, reader.leaves()), slices);
            }
        }
    }

    public void testReduce_whenEveryCollectorIsEmpty_thenNoHitsAndZeroTotal() throws IOException {
        final TopDocsAndMaxScore reduced = reduce(List.of(collect(List.of()), collect(List.of())));

        assertEquals(0, reduced.topDocs.scoreDocs.length);
        assertEquals(new TotalHits(0, TotalHits.Relation.EQUAL_TO), reduced.topDocs.totalHits);
        assertEquals(0.0f, reduced.maxScore, 0.0f);
    }

    public void testReduce_whenCollectorCountedHitsButHasNoCandidateLists_thenTotalAndMaxScoreAreKept() throws IOException {
        final HybridSearchCollector collector = mock(HybridSearchCollector.class);
        when(collector.topDocs()).thenReturn(List.of());
        when(collector.getTotalHits()).thenReturn(7);
        when(collector.getMaxScore()).thenReturn(3.0f);

        final SearchContext searchContext = mock(SearchContext.class);
        final TopDocsAndMaxScore reduced = new HybridSliceResultsReducer(
            new HybridCollectorResultsUtilParams.Builder().searchContext(searchContext).build(),
            NUM_HITS
        ).reduce(List.of(collector));

        assertEquals(0, reduced.topDocs.scoreDocs.length);
        assertEquals(new TotalHits(7, TotalHits.Relation.EQUAL_TO), reduced.topDocs.totalHits);
        assertEquals(3.0f, reduced.maxScore, 0.0f);
    }

    public void testReduce_whenSortedByANumericField_thenEachSubQueryEqualsOneCollectorOverTheShard() throws IOException {
        try (Directory directory = newDirectory()) {
            indexSegments(directory);
            try (DirectoryReader reader = DirectoryReader.open(directory)) {
                final List<List<ScoreDoc>> ascending = assertSortedSlicesLikeOneCollector(
                    reader,
                    new Sort(new SortField(PRICE_FIELD, SortField.Type.LONG)),
                    null
                );
                // the reference itself: 11 docs share the lowest price, the cut keeps the 10 lowest doc ids
                assertEquals(List.of(88, 3, 47), docs(ascending.get(0)));
                assertEquals(List.of(0, 33, 66, 99, 30, 63, 96, 27, 60, 93), docs(ascending.get(1)));
                assertTrue(ascending.get(2).isEmpty());
                assertEquals(List.of(0, 11, 22, 33, 44, 55, 66, 77, 88, 99), docs(ascending.get(3)));

                final List<List<ScoreDoc>> descending = assertSortedSlicesLikeOneCollector(
                    reader,
                    new Sort(new SortField(PRICE_FIELD, SortField.Type.LONG, true)),
                    null
                );
                assertEquals(List.of(3, 14, 25, 36, 47, 58, 69, 80, 91, 102), docs(descending.get(3)));
            }
        }
    }

    public void testReduce_whenSortedByKeywordThenNumericField_thenTiesBreakOnTheSecondFieldThenDocId() throws IOException {
        try (Directory directory = newDirectory()) {
            indexSegments(directory);
            try (DirectoryReader reader = DirectoryReader.open(directory)) {
                final List<List<ScoreDoc>> hits = assertSortedSlicesLikeOneCollector(
                    reader,
                    new Sort(new SortField(LABEL_FIELD, SortField.Type.STRING), new SortField(PRICE_FIELD, SortField.Type.LONG, true)),
                    null
                );

                // every doc of label l0, by price descending, then the lower doc id of each pair that shares a price
                assertEquals(List.of(14, 91, 28, 105, 42, 119, 56, 70, 7, 84), docs(hits.get(3)));
            }
        }
    }

    public void testReduce_whenSearchAfter_thenSlicesGiveWhatOneCollectorGives() throws IOException {
        try (Directory directory = newDirectory()) {
            indexSegments(directory);
            try (DirectoryReader reader = DirectoryReader.open(directory)) {
                // search_after the lowest price, as core builds the after doc from the previous page's sort values
                final List<List<ScoreDoc>> hits = assertSortedSlicesLikeOneCollector(
                    reader,
                    new Sort(new SortField(PRICE_FIELD, SortField.Type.LONG)),
                    new FieldDoc(Integer.MAX_VALUE, 0.0f, new Object[] { 0L })
                );

                // 11 docs share the next price, the cut keeps the 10 lowest doc ids
                assertEquals(List.of(8, 19, 30, 41, 52, 63, 74, 85, 96, 107), docs(hits.get(3)));
            }
        }
    }

    public void testReduce_whenSortedByDocId_thenSlicesGiveWhatOneCollectorGives() throws IOException {
        try (Directory directory = newDirectory()) {
            indexSegments(directory);
            try (DirectoryReader reader = DirectoryReader.open(directory)) {
                final List<List<ScoreDoc>> hits = assertSortedSlicesLikeOneCollector(reader, new Sort(SortField.FIELD_DOC), null);

                assertEquals(List.of(0, 3, 6, 9, 12, 15, 18, 21, 24, 27), docs(hits.get(1)));
                assertEquals(List.of(0, 1, 2, 3, 4, 5, 6, 7, 8, 9), docs(hits.get(3)));
            }
        }
    }

    public void testReduce_whenSortedAndRescoreContextsPresent_thenNotRescored() throws IOException {
        try (Directory directory = newDirectory()) {
            indexSegments(directory);
            try (DirectoryReader reader = DirectoryReader.open(directory)) {
                final Sort sort = new Sort(new SortField(PRICE_FIELD, SortField.Type.LONG));
                final SearchContext searchContext = sortedSearchContext(sort);
                when(searchContext.rescore()).thenReturn(
                    List.of(new RescoreContext(RESCORE_WINDOW, (TestRescorer) (topDocs, searcher, context) -> {
                        throw new AssertionError("sorted hybrid results must not be rescored");
                    }))
                );

                final TopDocsAndMaxScore slices = reduce(
                    sliceCollectors(reader.leaves(), leaves -> collectSorted(leaves, sort, null)),
                    searchContext
                );

                // the same as without rescore contexts, as on the per-collector path
                final TopDocsAndMaxScore withoutRescore = reduce(
                    sliceCollectors(reader.leaves(), leaves -> collectSorted(leaves, sort, null)),
                    sortedSearchContext(sort)
                );
                assertSameResult(withoutRescore, slices);
                assertSameSortValues(withoutRescore.topDocs, slices.topDocs, sort);
            }
        }
    }

    public void testReduce_whenCollapsedByKeyword_thenEachSubQueryEqualsOneCollectorOverTheShard() throws IOException {
        try (Directory directory = newDirectory()) {
            indexSegments(directory);
            try (DirectoryReader reader = DirectoryReader.open(directory)) {
                final CollapseTopFieldDocs oneCollector = assertCollapsedSlicesLikeOneCollector(reader, null, true);

                // the reference itself: default collapse keeps documents, several per group, cut inside the score tie
                final List<List<ScoreDoc>> hits = subQueryHits(oneCollector.scoreDocs);
                assertEquals(NUM_HITS, hits.get(0).size());
                assertEquals(9, hits.get(0).get(NUM_HITS - 1).doc);
                assertEquals(List.of(47, 3, 88), docs(hits.get(3)));
                assertEquals(2 + NUM_SUB_QUERIES + 10 + 10 + 0 + 3, oneCollector.scoreDocs.length);
                // doc 89 has no group value and is in sub-query 0's top hits
                assertTrue(Arrays.asList(oneCollector.collapseValues).contains(null));
            }
        }
    }

    public void testReduce_whenCollapsedByNumericField_thenSlicesGiveWhatOneCollectorGives() throws IOException {
        try (Directory directory = newDirectory()) {
            indexSegments(directory);
            try (DirectoryReader reader = DirectoryReader.open(directory)) {
                final CollapseTopFieldDocs oneCollector = assertCollapsedSlicesLikeOneCollector(reader, null, false);

                assertTrue(Arrays.asList(oneCollector.collapseValues).contains(4L));
            }
        }
    }

    public void testReduce_whenCollapsedAndSortedByScoreThenField_thenSlicesGiveWhatOneCollectorGives() throws IOException {
        try (Directory directory = newDirectory()) {
            indexSegments(directory);
            try (DirectoryReader reader = DirectoryReader.open(directory)) {
                assertCollapsedSlicesLikeOneCollector(
                    reader,
                    new Sort(new SortField(null, SortField.Type.SCORE), new SortField(PRICE_FIELD, SortField.Type.LONG)),
                    true
                );
            }
        }
    }

    public void testReduce_whenCollapsedAndSortedByAscendingScore_thenSlicesGiveWhatOneCollectorGives() throws IOException {
        try (Directory directory = newDirectory()) {
            indexSegments(directory);
            try (DirectoryReader reader = DirectoryReader.open(directory)) {
                final CollapseTopFieldDocs oneCollector = assertCollapsedSlicesLikeOneCollector(
                    reader,
                    new Sort(new SortField(null, SortField.Type.SCORE, true)),
                    true
                );

                // the lowest positive scores first: 11 docs of sub-query 1 share 0.25, the cut keeps the 10 lowest doc ids
                final List<ScoreDoc> subQueryHits = subQueryHits(oneCollector.scoreDocs).get(1);
                assertEquals(List.of(9, 20, 31, 42, 53, 64, 75, 86, 97, 108), docs(subQueryHits));
                assertTrue(subQueryHits.stream().allMatch(hit -> hit.score == 0.25f));
            }
        }
    }

    public void testReduce_whenCollapsedAndSortedByAField_thenSlicesGiveWhatOneCollectorGives() throws IOException {
        try (Directory directory = newDirectory()) {
            indexSegments(directory);
            try (DirectoryReader reader = DirectoryReader.open(directory)) {
                assertCollapsedSlicesLikeOneCollector(reader, new Sort(new SortField(PRICE_FIELD, SortField.Type.LONG, true)), false);
            }
        }
    }

    public void testReduce_whenDistinctGroupsCollapse_thenTodaysPerCollectorFold() throws IOException {
        try (Directory directory = newDirectory()) {
            indexSegments(directory);
            try (DirectoryReader reader = DirectoryReader.open(directory)) {
                final SearchContext searchContext = collapseSearchContext(null, GROUP_FIELD);
                final Sort collapseSort = new Sort(new SortField(null, SortField.Type.SCORE));
                final SliceCollector distinctGroups = leaves -> collect(
                    HybridCollapsingTopGroupsCollector.createKeyword(
                        GROUP_FIELD,
                        collapseFieldType(true),
                        collapseSort,
                        NUM_HITS,
                        new HitsThresholdChecker(Integer.MAX_VALUE)
                    ),
                    leaves,
                    HybridSliceResultsReducerTests::score
                );

                final TopDocsAndMaxScore reduced = reduce(sliceCollectors(reader.leaves(), distinctGroups), searchContext);

                final TopDocsAndMaxScore fold = perCollectorFold(sliceCollectors(reader.leaves(), distinctGroups), searchContext);
                assertSameResult(fold, reduced);
                assertSameCollapseValues((CollapseTopFieldDocs) fold.topDocs, (CollapseTopFieldDocs) reduced.topDocs);
                // the fold keeps every slice's groups: more than numHits for a sub-query
                assertTrue(subQueryHits(reduced.topDocs.scoreDocs).get(0).size() > NUM_HITS);
            }
        }
    }

    public void testReduce_whenDefaultAndDistinctGroupsCollectorsAreMixed_thenTodaysPerCollectorFold() throws IOException {
        try (Directory directory = newDirectory()) {
            indexSegments(directory);
            try (DirectoryReader reader = DirectoryReader.open(directory)) {
                final SearchContext searchContext = collapseSearchContext(null, GROUP_FIELD);
                final Sort collapseSort = new Sort(new SortField(null, SortField.Type.SCORE));
                // the distinct groups setting is dynamic, so the slices of one shard can get different collectors
                final SliceCollector mixed = new SliceCollector() {
                    private int slice = 0;

                    @Override
                    public Collector collect(final List<LeafReaderContext> leaves) throws IOException {
                        final HitsThresholdChecker hitsThresholdChecker = new HitsThresholdChecker(Integer.MAX_VALUE);
                        final HybridSearchCollector collector = slice++ % 2 == 0
                            ? HybridCollapsingTopDocsCollector.createKeyword(
                                GROUP_FIELD,
                                collapseFieldType(true),
                                collapseSort,
                                NUM_HITS,
                                hitsThresholdChecker
                            )
                            : HybridCollapsingTopGroupsCollector.createKeyword(
                                GROUP_FIELD,
                                collapseFieldType(true),
                                collapseSort,
                                NUM_HITS,
                                hitsThresholdChecker
                            );
                        return HybridSliceResultsReducerTests.collect(collector, leaves, HybridSliceResultsReducerTests::score);
                    }
                };

                final TopDocsAndMaxScore reduced = reduce(sliceCollectors(reader.leaves(), mixed), searchContext);

                final TopDocsAndMaxScore fold = perCollectorFold(sliceCollectors(reader.leaves(), mixed), searchContext);
                assertSameResult(fold, reduced);
                assertSameCollapseValues((CollapseTopFieldDocs) fold.topDocs, (CollapseTopFieldDocs) reduced.topDocs);
            }
        }
    }

    public void testReduce_whenADistinctGroupsCollectorReachesTheReducer_thenIllegalState() throws IOException {
        final SearchContext searchContext = collapseSearchContext(null, GROUP_FIELD);
        final HybridSliceResultsReducer reducer = new HybridSliceResultsReducer(
            new HybridCollectorResultsUtilParams.Builder().searchContext(searchContext).build(),
            NUM_HITS
        );
        final HybridSearchCollector distinctGroups = HybridCollapsingTopGroupsCollector.createKeyword(
            GROUP_FIELD,
            collapseFieldType(true),
            collapseSort(null),
            NUM_HITS,
            new HitsThresholdChecker(Integer.MAX_VALUE)
        );

        final IllegalStateException exception = expectThrows(IllegalStateException.class, () -> reducer.reduce(List.of(distinctGroups)));
        assertEquals("only default-mode collapse results of a hybrid query can be merged across slices", exception.getMessage());
    }

    public void testCanMerge_whenCollapseIsOffOrEveryCollectorIsDefaultMode_thenTrueElseFalse() {
        final HitsThresholdChecker hitsThresholdChecker = new HitsThresholdChecker(Integer.MAX_VALUE);
        final HybridSearchCollector defaultMode = HybridCollapsingTopDocsCollector.createKeyword(
            GROUP_FIELD,
            collapseFieldType(true),
            collapseSort(null),
            NUM_HITS,
            hitsThresholdChecker
        );
        final HybridSearchCollector distinctGroups = HybridCollapsingTopGroupsCollector.createKeyword(
            GROUP_FIELD,
            collapseFieldType(true),
            collapseSort(null),
            NUM_HITS,
            hitsThresholdChecker
        );
        final HybridCollectorResultsUtilParams collapse = new HybridCollectorResultsUtilParams.Builder().searchContext(
            collapseSearchContext(null, GROUP_FIELD)
        ).build();
        final HybridCollectorResultsUtilParams noCollapse = new HybridCollectorResultsUtilParams.Builder().searchContext(
            mock(SearchContext.class)
        ).build();

        // no collapse: any collector merges
        assertTrue(HybridSliceResultsReducer.canMerge(noCollapse, List.of(new HybridTopScoreDocCollector(NUM_HITS, hitsThresholdChecker))));
        // default-mode collapse only
        assertTrue(HybridSliceResultsReducer.canMerge(collapse, List.of(defaultMode, defaultMode)));
        // distinct-groups alone, or mixed with default mode after the dynamic setting flipped
        assertFalse(HybridSliceResultsReducer.canMerge(collapse, List.of(distinctGroups)));
        assertFalse(HybridSliceResultsReducer.canMerge(collapse, List.of(defaultMode, distinctGroups)));
    }

    public void testReduce_whenCollapseCollectorsAreWrappedInMultiCollectors_thenTheyAreReduced() throws IOException {
        try (Directory directory = newDirectory()) {
            indexSegments(directory);
            try (DirectoryReader reader = DirectoryReader.open(directory)) {
                final SearchContext searchContext = collapseSearchContext(null, GROUP_FIELD);
                final List<Collector> wrapped = new ArrayList<>();
                for (Collector collector : sliceCollectors(reader.leaves(), leaves -> collectCollapsed(leaves, null, true))) {
                    wrapped.add(MultiCollector.wrap(collector, new TotalHitCountCollector()));
                }

                final TopDocsAndMaxScore reduced = reduce(wrapped, searchContext);

                final TopDocsAndMaxScore unwrapped = reduce(
                    sliceCollectors(reader.leaves(), leaves -> collectCollapsed(leaves, null, true)),
                    searchContext
                );
                assertSameResult(unwrapped, reduced);
                assertSameCollapseValues((CollapseTopFieldDocs) unwrapped.topDocs, (CollapseTopFieldDocs) reduced.topDocs);
            }
        }
    }

    public void testMergeSubQueryTopDocs_whenTiesAcrossSlices_thenLowerDocIdFirstInAnySliceOrder() {
        final List<List<? extends TopDocs>> slices = new ArrayList<>();
        slices.add(List.of(topDocs(hit(5, 2.0f), hit(9, 1.0f))));
        slices.add(List.of(topDocs(hit(2, 2.0f), hit(7, 1.0f))));
        slices.add(List.of(topDocs(hit(3, 1.0f))));

        for (int round = 0; round < 3; round++) {
            Collections.rotate(slices, 1);
            final List<TopDocs> merged = HybridSliceResultsReducer.mergeSubQueryTopDocs(slices, 4, null);
            assertEquals(1, merged.size());
            assertHits(merged.get(0), hit(2, 2.0f), hit(5, 2.0f), hit(3, 1.0f), hit(7, 1.0f));
        }
    }

    public void testMergeSubQueryTopDocs_whenNumHitsIsZero_thenEverySubQueryIsEmpty() {
        final List<TopDocs> merged = HybridSliceResultsReducer.mergeSubQueryTopDocs(
            List.of(List.of(topDocs(hit(1, 1.0f)), topDocs(hit(2, 3.0f))), List.of(topDocs(hit(4, 2.0f)), topDocs())),
            0,
            null
        );

        assertEquals(2, merged.size());
        assertHits(merged.get(0));
        assertHits(merged.get(1));
    }

    public void testMergeSubQueryTopDocs_whenFewerHitsThanNumHits_thenAllHitsInOrder() {
        final List<TopDocs> merged = HybridSliceResultsReducer.mergeSubQueryTopDocs(
            List.of(List.of(topDocs(hit(8, 3.0f), hit(1, 0.5f))), List.of(topDocs(hit(4, 2.0f)))),
            10,
            null
        );

        assertHits(merged.get(0), hit(8, 3.0f), hit(4, 2.0f), hit(1, 0.5f));
    }

    public void testMergeSubQueryTopDocs_whenSliceListIsEmptyOrNull_thenItIsSkipped() {
        final List<? extends TopDocs> withNull = Collections.singletonList(null);
        final List<TopDocs> withNullScoreDocs = List.of(new TopDocs(new TotalHits(0, TotalHits.Relation.EQUAL_TO), null));
        final List<TopDocs> merged = HybridSliceResultsReducer.mergeSubQueryTopDocs(
            List.of(List.of(topDocs(hit(3, 1.0f))), List.of(topDocs()), withNull, withNullScoreDocs),
            2,
            null
        );

        assertHits(merged.get(0), hit(3, 1.0f));
        // no slice list left to merge
        assertHits(HybridSliceResultsReducer.mergeSubQueryTopDocs(List.of(withNullScoreDocs, withNull), 2, null).get(0));
    }

    public void testMergeSubQueryTopDocs_whenOneSlice_thenSameHitObjectsInSameOrder() {
        final TopDocs slice = topDocs(hit(6, 4.0f), hit(2, 1.0f));

        final List<TopDocs> merged = HybridSliceResultsReducer.mergeSubQueryTopDocs(List.of(List.of(slice)), 5, null);

        assertEquals(2, merged.get(0).scoreDocs.length);
        assertSame(slice.scoreDocs[0], merged.get(0).scoreDocs[0]);
        assertSame(slice.scoreDocs[1], merged.get(0).scoreDocs[1]);
    }

    public void testMergeSubQueryTopDocs_whenSlicesDisagreeOnSubQueryCount_thenIllegalState() {
        final IllegalStateException exception = expectThrows(
            IllegalStateException.class,
            () -> HybridSliceResultsReducer.mergeSubQueryTopDocs(
                List.of(List.of(topDocs(hit(1, 1.0f)), topDocs()), List.of(topDocs(hit(2, 1.0f)))),
                5,
                null
            )
        );
        assertEquals("slice collectors of one shard returned results for [2] and [1] sub-queries", exception.getMessage());
    }

    public void testMergeSubQueryTopDocs_whenSortedByALongField_thenBySortValueThenDocIdInAnySliceOrder() {
        final Sort ascending = new Sort(new SortField(PRICE_FIELD, SortField.Type.LONG));
        final List<List<? extends TopDocs>> ascendingSlices = new ArrayList<>();
        ascendingSlices.add(List.of(topFieldDocs(ascending, fieldDoc(5, 1L), fieldDoc(9, 2L))));
        ascendingSlices.add(List.of(topFieldDocs(ascending, fieldDoc(2, 1L), fieldDoc(7, 2L))));
        ascendingSlices.add(List.of(topFieldDocs(ascending, fieldDoc(3, 2L))));
        final Sort descending = new Sort(new SortField(PRICE_FIELD, SortField.Type.LONG, true));
        final List<List<? extends TopDocs>> descendingSlices = new ArrayList<>();
        descendingSlices.add(List.of(topFieldDocs(descending, fieldDoc(9, 2L), fieldDoc(5, 1L))));
        descendingSlices.add(List.of(topFieldDocs(descending, fieldDoc(7, 2L), fieldDoc(2, 1L))));
        descendingSlices.add(List.of(topFieldDocs(descending, fieldDoc(3, 2L))));

        for (int round = 0; round < 3; round++) {
            Collections.rotate(ascendingSlices, 1);
            Collections.rotate(descendingSlices, 1);
            assertEquals(List.of(2, 5, 3, 7), docs(HybridSliceResultsReducer.mergeSubQueryTopDocs(ascendingSlices, 4, ascending).get(0)));
            assertEquals(List.of(3, 7, 9, 2), docs(HybridSliceResultsReducer.mergeSubQueryTopDocs(descendingSlices, 4, descending).get(0)));
        }
    }

    public void testMergeSubQueryTopDocs_whenSortedByKeywordWithMissingValues_thenByBytesThenDocId() {
        final SortField labelField = new SortField(LABEL_FIELD, SortField.Type.STRING);
        labelField.setMissingValue(SortField.STRING_LAST);
        final Sort sort = new Sort(labelField);

        final List<TopDocs> merged = HybridSliceResultsReducer.mergeSubQueryTopDocs(
            List.of(
                List.of(topFieldDocs(sort, fieldDoc(6, new BytesRef("a")), fieldDoc(8, new BytesRef("c")))),
                List.of(topFieldDocs(sort, fieldDoc(4, new BytesRef("b")), fieldDoc(1, (Object) null))),
                List.of(topFieldDocs(sort, fieldDoc(2, new BytesRef("b"))))
            ),
            4,
            sort
        );

        assertEquals(List.of(6, 2, 4, 8), docs(merged.get(0)));
        assertTrue(merged.get(0) instanceof TopFieldDocs);
    }

    public void testMergeSubQueryTopDocs_whenSortedByTwoFieldsWithLeadingTies_thenSecondFieldThenDocId() {
        final Sort sort = new Sort(
            new SortField(LABEL_FIELD, SortField.Type.STRING),
            new SortField(PRICE_FIELD, SortField.Type.LONG, true)
        );

        final List<TopDocs> merged = HybridSliceResultsReducer.mergeSubQueryTopDocs(
            List.of(
                List.of(topFieldDocs(sort, fieldDoc(2, new BytesRef("a"), 5L), fieldDoc(4, new BytesRef("a"), 3L))),
                List.of(topFieldDocs(sort, fieldDoc(1, new BytesRef("a"), 5L), fieldDoc(7, new BytesRef("b"), 9L)))
            ),
            3,
            sort
        );

        assertEquals(List.of(1, 2, 4), docs(merged.get(0)));
    }

    public void testMergeSubQueryTopDocs_whenSortedAndASliceListIsNotSorted_thenIllegalState() {
        final Sort sort = new Sort(new SortField(PRICE_FIELD, SortField.Type.LONG));

        final IllegalStateException exception = expectThrows(
            IllegalStateException.class,
            () -> HybridSliceResultsReducer.mergeSubQueryTopDocs(
                List.of(List.of(topFieldDocs(sort, fieldDoc(1, 1L))), List.of(topDocs(hit(2, 1.0f)))),
                5,
                sort
            )
        );
        assertEquals("slice collectors of a sorted hybrid query must return sorted results per sub-query", exception.getMessage());
    }

    public void testMergeCollapseSubQueryTopDocs_whenSlicesShareCollapseValues_thenEachHitKeepsItsOwnValue() {
        final Sort sort = new Sort(new SortField(PRICE_FIELD, SortField.Type.LONG));
        // the same collapse value in several slices, a null value, and value objects that are equal but not the same
        final List<List<? extends TopDocs>> slices = List.of(
            List.of(collapseTopDocs(sort, new FieldDoc[] { fieldDoc(4, 1L), fieldDoc(9, 3L) }, new BytesRef("a"), null)),
            List.of(collapseTopDocs(sort, new FieldDoc[] { fieldDoc(2, 1L), fieldDoc(7, 2L) }, new BytesRef("a"), new BytesRef("b")))
        );

        final List<CollapseTopFieldDocs> merged = HybridSliceResultsReducer.mergeCollapseSubQueryTopDocs(slices, 3, sort, GROUP_FIELD);

        assertEquals(1, merged.size());
        assertEquals(List.of(2, 4, 7), docs(merged.get(0)));
        assertArrayEquals(new Object[] { new BytesRef("a"), new BytesRef("a"), new BytesRef("b") }, merged.get(0).collapseValues);
        // each kept hit carries the value its own slice reported, found by doc id
        assertSame(((CollapseTopFieldDocs) slices.get(1).get(0)).collapseValues[0], merged.get(0).collapseValues[0]);
        assertSame(((CollapseTopFieldDocs) slices.get(0).get(0)).collapseValues[0], merged.get(0).collapseValues[1]);
        assertEquals(GROUP_FIELD, merged.get(0).field);
        assertArrayEquals(sort.getSort(), merged.get(0).fields);
    }

    public void testMergeCollapseSubQueryTopDocs_whenTheCutKeepsANullValue_thenItStaysNull() {
        final Sort sort = new Sort(new SortField(PRICE_FIELD, SortField.Type.LONG));

        final List<CollapseTopFieldDocs> merged = HybridSliceResultsReducer.mergeCollapseSubQueryTopDocs(
            List.of(
                List.of(collapseTopDocs(sort, new FieldDoc[] { fieldDoc(5, 1L) }, (Object) null)),
                Collections.singletonList(null),
                List.of(collapseTopDocs(sort, new FieldDoc[] { fieldDoc(3, 2L) }, 7L))
            ),
            10,
            sort,
            BUCKET_FIELD
        );

        assertEquals(List.of(5, 3), docs(merged.get(0)));
        assertArrayEquals(new Object[] { null, 7L }, merged.get(0).collapseValues);
    }

    public void testMergeCollapseSubQueryTopDocs_whenNumHitsIsZero_thenEmptyListsWithTheirField() {
        final Sort sort = new Sort(new SortField(PRICE_FIELD, SortField.Type.LONG));

        final List<CollapseTopFieldDocs> merged = HybridSliceResultsReducer.mergeCollapseSubQueryTopDocs(
            List.of(List.of(collapseTopDocs(sort, new FieldDoc[] { fieldDoc(5, 1L) }, 1L), collapseTopDocs(sort, new FieldDoc[0]))),
            0,
            sort,
            BUCKET_FIELD
        );

        assertEquals(2, merged.size());
        for (CollapseTopFieldDocs subQuery : merged) {
            assertEquals(0, subQuery.scoreDocs.length);
            assertEquals(0, subQuery.collapseValues.length);
            assertEquals(BUCKET_FIELD, subQuery.field);
        }
    }

    public void testMergeCollapseSubQueryTopDocs_whenValuesAndHitsAreMisaligned_thenIllegalState() {
        final Sort sort = new Sort(new SortField(PRICE_FIELD, SortField.Type.LONG));

        final IllegalStateException exception = expectThrows(
            IllegalStateException.class,
            () -> HybridSliceResultsReducer.mergeCollapseSubQueryTopDocs(
                List.of(List.of(collapseTopDocs(sort, new FieldDoc[] { fieldDoc(5, 1L), fieldDoc(6, 2L) }, 1L))),
                10,
                sort,
                BUCKET_FIELD
            )
        );
        assertEquals("collapse results of a sub-query have [2] hits and [1] collapse values", exception.getMessage());
    }

    public void testMergeCollapseSubQueryTopDocs_whenASliceListIsNotACollapseResult_thenIllegalState() {
        final Sort sort = new Sort(new SortField(PRICE_FIELD, SortField.Type.LONG));

        final IllegalStateException exception = expectThrows(
            IllegalStateException.class,
            () -> HybridSliceResultsReducer.mergeCollapseSubQueryTopDocs(
                List.of(List.of(topFieldDocs(sort, fieldDoc(5, 1L)))),
                10,
                sort,
                BUCKET_FIELD
            )
        );
        assertEquals("slice collectors of a collapsed hybrid query must return collapse results per sub-query", exception.getMessage());
    }

    public void testMergeCollapseSubQueryTopDocs_whenTwoSlicesReturnTheSameDoc_thenIllegalState() {
        // slices partition a shard's segments, so one doc id can come from one slice only; two lists with the same doc would make
        // the collapse value ambiguous, and the merge refuses instead of picking one
        final Sort sort = new Sort(new SortField(PRICE_FIELD, SortField.Type.LONG));

        final IllegalStateException exception = expectThrows(
            IllegalStateException.class,
            () -> HybridSliceResultsReducer.mergeCollapseSubQueryTopDocs(
                List.of(
                    List.of(collapseTopDocs(sort, new FieldDoc[] { fieldDoc(5, 1L) }, 1L)),
                    List.of(collapseTopDocs(sort, new FieldDoc[] { fieldDoc(5, 2L) }, 2L))
                ),
                10,
                sort,
                BUCKET_FIELD
            )
        );
        assertEquals("doc [5] appears in the collapse results of more than one slice", exception.getMessage());
    }

    private static void assertCutFallsInsideATie(final int subQuery) {
        final List<ScoreDoc> matches = new ArrayList<>();
        for (int doc = 0; doc < SEGMENTS * DOCS_PER_SEGMENT; doc++) {
            final float subQueryScore = score(subQuery, doc);
            if (subQueryScore > 0.0f) {
                matches.add(new ScoreDoc(doc, subQueryScore));
            }
        }
        matches.sort(SCORE_DESC_DOC_ASC);
        assertEquals(matches.get(NUM_HITS - 1).score, matches.get(NUM_HITS).score, 0.0f);
    }

    private static void indexSegments(final Directory directory) throws IOException {
        try (IndexWriter writer = new IndexWriter(directory, new IndexWriterConfig().setMergePolicy(NoMergePolicy.INSTANCE))) {
            for (int segment = 0; segment < SEGMENTS; segment++) {
                for (int doc = 0; doc < DOCS_PER_SEGMENT; doc++) {
                    final Document document = new Document();
                    final int globalDoc = segment * DOCS_PER_SEGMENT + doc;
                    document.add(new StringField("id", Integer.toString(globalDoc), Field.Store.NO));
                    // every fourth doc matches the rescore query, so rescore reorders hits inside each window
                    document.add(new StringField(TAG_FIELD, globalDoc % 4 == 1 ? BOOSTED_TAG : PLAIN_TAG, Field.Store.NO));
                    document.add(new NumericDocValuesField(PRICE_FIELD, price(globalDoc)));
                    document.add(new SortedDocValuesField(LABEL_FIELD, new BytesRef(label(globalDoc))));
                    // collapse fields: five groups each, some docs without a value
                    if (globalDoc % 10 != 9) {
                        document.add(new SortedDocValuesField(GROUP_FIELD, new BytesRef("g" + globalDoc % 5)));
                    }
                    if (globalDoc % 10 != 8) {
                        document.add(new SortedNumericDocValuesField(BUCKET_FIELD, globalDoc % 5));
                    }
                    writer.addDocument(document);
                }
                // one commit per segment and no merges: SEGMENTS leaves with doc bases 0, 30, 60, 90
                writer.commit();
            }
        }
    }

    private static List<Collector> sliceCollectors(final List<LeafReaderContext> leaves) throws IOException {
        return sliceCollectors(leaves, HybridSliceResultsReducerTests::collect);
    }

    /**
     * Fresh collectors, because topDocs() drains the queues. Slice i gets the leaves with ord % SLICES == i, visited in ascending
     * doc base order as a Lucene slice visits its segments. A last collector sees no leaf, like an empty slice.
     */
    private static List<Collector> sliceCollectors(final List<LeafReaderContext> leaves, final SliceCollector sliceCollector)
        throws IOException {
        final List<List<LeafReaderContext>> slices = new ArrayList<>();
        for (int i = 0; i < SLICES; i++) {
            slices.add(new ArrayList<>());
        }
        for (LeafReaderContext leaf : leaves) {
            slices.get(leaf.ord % SLICES).add(leaf);
        }
        final List<Collector> collectors = new ArrayList<>();
        for (List<LeafReaderContext> slice : slices) {
            collectors.add(sliceCollector.collect(slice));
        }
        collectors.add(sliceCollector.collect(List.of()));
        return collectors;
    }

    private static HybridTopScoreDocCollector collect(final List<LeafReaderContext> leaves) throws IOException {
        return collect(
            new HybridTopScoreDocCollector(NUM_HITS, new HitsThresholdChecker(NUM_HITS)),
            leaves,
            HybridSliceResultsReducerTests::score
        );
    }

    /** Sort collector as the collector factory creates it, with the default total hits threshold; search_after when after is set. */
    private static HybridTopFieldDocSortCollector collectSorted(
        final List<LeafReaderContext> leaves,
        final Sort sort,
        @Nullable final FieldDoc after
    ) throws IOException {
        final HitsThresholdChecker hitsThresholdChecker = new HitsThresholdChecker(SearchContext.DEFAULT_TRACK_TOTAL_HITS_UP_TO);
        final HybridTopFieldDocSortCollector collector = Objects.isNull(after)
            ? new SimpleFieldCollector(NUM_HITS, hitsThresholdChecker, sort)
            : new PagingFieldCollector(NUM_HITS, hitsThresholdChecker, sort, after);
        return collect(collector, leaves, HybridSliceResultsReducerTests::sortedSubQueryScore);
    }

    private static <T extends HybridSearchCollector> T collect(
        final T collector,
        final List<LeafReaderContext> leaves,
        final SubQueryScores subQueryScores
    ) throws IOException {
        for (LeafReaderContext leaf : leaves) {
            final LeafCollector leafCollector = collector.getLeafCollector(leaf);
            final HybridSubQueryScorer scorer = new HybridSubQueryScorer(NUM_SUB_QUERIES);
            leafCollector.setScorer(scorer);
            for (int doc = 0; doc < leaf.reader().maxDoc(); doc++) {
                boolean matches = false;
                for (int subQuery = 0; subQuery < NUM_SUB_QUERIES; subQuery++) {
                    final float subQueryScore = subQueryScores.score(subQuery, leaf.docBase + doc);
                    scorer.getSubQueryScores()[subQuery] = subQueryScore;
                    matches |= subQueryScore > 0.0f;
                }
                // the hybrid bulk scorer calls collect only for a doc that matches at least one sub-query; from its second 4,096-doc
                // window on it also skips docs at or below the queue's min competitive score, which these small segments never reach
                if (matches) {
                    leafCollector.collect(doc);
                }
                scorer.resetScores();
            }
            leafCollector.finish();
        }
        return collector;
    }

    @FunctionalInterface
    private interface SubQueryScores {
        float score(int subQuery, int globalDoc);
    }

    @FunctionalInterface
    private interface SliceCollector {
        Collector collect(List<LeafReaderContext> leaves) throws IOException;
    }

    private static TopDocsAndMaxScore reduce(final List<? extends Collector> collectors) throws IOException {
        return reduce(collectors, mock(SearchContext.class));
    }

    private static TopDocsAndMaxScore reduce(final List<? extends Collector> collectors, final SearchContext searchContext)
        throws IOException {
        final HybridCollectorManager manager = new HybridCollectorManager(
            NUM_HITS,
            new HitsThresholdChecker(NUM_HITS),
            null,
            null,
            searchContext
        );
        final QuerySearchResult result = new QuerySearchResult();
        manager.reduce(new ArrayList<>(collectors)).reduce(result);
        return result.topDocs();
    }

    /**
     * Search context with the given rescore contexts, whose searcher runs Lucene's real rescore over the test index (rewrite,
     * createWeight and the reader go to a plain IndexSearcher).
     */
    private static SearchContext searchContextWithRescore(final DirectoryReader reader, final List<RescoreContext> rescoreContexts)
        throws IOException {
        final IndexSearcher indexSearcher = new IndexSearcher(reader);
        final ContextIndexSearcher contextIndexSearcher = mock(ContextIndexSearcher.class);
        when(contextIndexSearcher.getIndexReader()).thenReturn(reader);
        when(contextIndexSearcher.rewrite(any(Query.class))).thenAnswer(
            invocation -> indexSearcher.rewrite((Query) invocation.getArgument(0))
        );
        when(contextIndexSearcher.createWeight(any(Query.class), any(ScoreMode.class), anyFloat())).thenAnswer(
            invocation -> indexSearcher.createWeight(invocation.getArgument(0), invocation.getArgument(1), invocation.getArgument(2))
        );
        final SearchContext searchContext = mock(SearchContext.class);
        when(searchContext.searcher()).thenReturn(contextIndexSearcher);
        when(searchContext.rescore()).thenReturn(rescoreContexts);
        return searchContext;
    }

    /**
     * Core's query rescore of the top window hits: first-pass score plus rescoreQueryWeight times the tag query's score. Records
     * the length of every list it gets.
     */
    private static RescoreContext queryRescoreContext(
        final int window,
        final String tag,
        final float rescoreQueryWeight,
        final List<Integer> rescoredListSizes
    ) {
        final QueryRescorer.QueryRescoreContext context = new QueryRescorer.QueryRescoreContext(window);
        context.setParsedQuery(new ParsedQuery(new TermQuery(new Term(TAG_FIELD, tag))));
        context.setQueryWeight(1.0f);
        context.setRescoreQueryWeight(rescoreQueryWeight);
        context.setScoreMode(QueryRescoreMode.Total);
        return new RescoreContext(window, (TestRescorer) (topDocs, searcher, ignored) -> {
            rescoredListSizes.add(topDocs.scoreDocs.length);
            return QueryRescorer.INSTANCE.rescore(topDocs, searcher, context);
        });
    }

    /**
     * Reduces the slice collectors with the given rescore contexts and asserts the result equals today's path with concurrent
     * segment search off. The recorded list sizes are cleared after that reference runs, so they hold the slice reduce's calls.
     */
    private static TopDocsAndMaxScore reduceSlicesWithRescore(
        final DirectoryReader reader,
        final List<RescoreContext> rescoreContexts,
        final List<Integer> rescoredListSizes
    ) throws IOException {
        final SearchContext searchContext = searchContextWithRescore(reader, rescoreContexts);
        final TopDocsAndMaxScore oneCollector = withoutConcurrentSearch(searchContext, reader.leaves());
        rescoredListSizes.clear();
        final TopDocsAndMaxScore slices = reduce(sliceCollectors(reader.leaves()), searchContext);
        assertSameResult(oneCollector, slices);
        return slices;
    }

    /**
     * Collects with the sort, and search_after when after is set, over the slices and over the whole shard, and asserts the
     * reduced slices equal today's path with concurrent segment search off; returns that reference's hits per sub-query.
     */
    private static List<List<ScoreDoc>> assertSortedSlicesLikeOneCollector(
        final DirectoryReader reader,
        final Sort sort,
        @Nullable final FieldDoc after
    ) throws IOException {
        final SearchContext searchContext = sortedSearchContext(sort);
        final TopDocsAndMaxScore oneCollector = withoutConcurrentSearch(searchContext, collectSorted(reader.leaves(), sort, after));
        final TopDocsAndMaxScore slices = reduce(
            sliceCollectors(reader.leaves(), leaves -> collectSorted(leaves, sort, after)),
            searchContext
        );
        assertSameResult(oneCollector, slices);
        assertSameSortValues(oneCollector.topDocs, slices.topDocs, sort);
        return subQueryHits(oneCollector.topDocs.scoreDocs);
    }

    /**
     * Collects with default-mode collapse on the keyword or the numeric field, sorted by the request sort or by score, over the
     * slices and over the whole shard, and asserts the reduced slices equal today's path with concurrent segment search off, collapse
     * values included; returns that reference.
     */
    private static CollapseTopFieldDocs assertCollapsedSlicesLikeOneCollector(
        final DirectoryReader reader,
        @Nullable final Sort sort,
        final boolean keyword
    ) throws IOException {
        final SearchContext searchContext = collapseSearchContext(sort, keyword ? GROUP_FIELD : BUCKET_FIELD);
        final TopDocsAndMaxScore oneCollector = withoutConcurrentSearch(searchContext, collectCollapsed(reader.leaves(), sort, keyword));
        final TopDocsAndMaxScore slices = reduce(
            sliceCollectors(reader.leaves(), leaves -> collectCollapsed(leaves, sort, keyword)),
            searchContext
        );
        assertSameResult(oneCollector, slices);
        assertSameSortValues(oneCollector.topDocs, slices.topDocs, collapseSort(sort));
        assertSameCollapseValues((CollapseTopFieldDocs) oneCollector.topDocs, (CollapseTopFieldDocs) slices.topDocs);
        return (CollapseTopFieldDocs) oneCollector.topDocs;
    }

    /** Default-mode collapse collector as the collector factory creates it; matches stay below its hits threshold. */
    private static HybridCollapsingTopDocsCollector<?> collectCollapsed(
        final List<LeafReaderContext> leaves,
        @Nullable final Sort sort,
        final boolean keyword
    ) throws IOException {
        final HitsThresholdChecker hitsThresholdChecker = new HitsThresholdChecker(Integer.MAX_VALUE);
        final HybridCollapsingTopDocsCollector<?> collector = keyword
            ? HybridCollapsingTopDocsCollector.createKeyword(
                GROUP_FIELD,
                collapseFieldType(true),
                collapseSort(sort),
                NUM_HITS,
                hitsThresholdChecker
            )
            : HybridCollapsingTopDocsCollector.createNumeric(
                BUCKET_FIELD,
                collapseFieldType(false),
                collapseSort(sort),
                NUM_HITS,
                hitsThresholdChecker
            );
        return collect(collector, leaves, HybridSliceResultsReducerTests::score);
    }

    /** The collector factory's collapse sort: the request sort, or score when the request has none. */
    private static Sort collapseSort(@Nullable final Sort sort) {
        return Objects.isNull(sort) ? new Sort(new SortField(null, SortField.Type.SCORE)) : sort;
    }

    private static MappedFieldType collapseFieldType(final boolean keyword) {
        return keyword
            ? new KeywordFieldMapper.KeywordFieldType(GROUP_FIELD)
            : new NumberFieldMapper.NumberFieldType(BUCKET_FIELD, NumberFieldMapper.NumberType.LONG);
    }

    private static SearchContext collapseSearchContext(@Nullable final Sort sort, final String collapseField) {
        final SearchContext searchContext = Objects.isNull(sort) ? mock(SearchContext.class) : sortedSearchContext(sort);
        final MappedFieldType fieldType = collapseFieldType(GROUP_FIELD.equals(collapseField));
        when(searchContext.collapse()).thenReturn(new CollapseContext(collapseField, fieldType, List.of()));
        return searchContext;
    }

    /** Today's path for these collectors: each one encoded on its own, then folded into one shard result. */
    private static TopDocsAndMaxScore perCollectorFold(final List<? extends Collector> collectors, final SearchContext searchContext)
        throws IOException {
        final HybridCollectorResultsUtilParams params = new HybridCollectorResultsUtilParams.Builder().searchContext(searchContext).build();
        final QuerySearchResult result = new QuerySearchResult();
        for (Collector collector : collectors) {
            final HybridSearchCollectorResultUtil collectorResult = new HybridSearchCollectorResultUtil(
                params,
                (HybridSearchCollector) collector
            );
            collectorResult.reduceCollectorResults(result, collectorResult.getTopDocsAndMaxScore());
        }
        return result.topDocs();
    }

    private static SearchContext sortedSearchContext(final Sort sort) {
        final DocValueFormat[] formats = new DocValueFormat[sort.getSort().length];
        Arrays.fill(formats, DocValueFormat.RAW);
        final SearchContext searchContext = mock(SearchContext.class);
        when(searchContext.sort()).thenReturn(new SortAndFormats(sort, formats));
        return searchContext;
    }

    /** Today's path with concurrent segment search off: one collector over the whole shard, encoded on its own. */
    private static TopDocsAndMaxScore withoutConcurrentSearch(final SearchContext searchContext, final List<LeafReaderContext> leaves)
        throws IOException {
        return withoutConcurrentSearch(searchContext, collect(leaves));
    }

    private static TopDocsAndMaxScore withoutConcurrentSearch(final SearchContext searchContext, final HybridSearchCollector collector)
        throws IOException {
        return new HybridSearchCollectorResultUtil(
            new HybridCollectorResultsUtilParams.Builder().searchContext(searchContext).build(),
            collector
        ).getTopDocsAndMaxScore();
    }

    /** Rescorer given as a lambda; explain is not used by the reduce. */
    @FunctionalInterface
    private interface TestRescorer extends Rescorer {
        @Override
        default Explanation explain(
            int topLevelDocId,
            IndexSearcher searcher,
            RescoreContext rescoreContext,
            Explanation sourceExplanation
        ) {
            throw new UnsupportedOperationException("explain is not used by the reduce");
        }
    }

    private static List<List<ScoreDoc>> subQueryHits(final ScoreDoc[] scoreDocs) {
        assertTrue(isHybridQueryStartStopElement(scoreDocs[0]));
        assertTrue(isHybridQueryStartStopElement(scoreDocs[scoreDocs.length - 1]));
        final List<List<ScoreDoc>> hits = new ArrayList<>();
        for (int i = 1; i < scoreDocs.length - 1; i++) {
            if (isHybridQueryDelimiterElement(scoreDocs[i])) {
                hits.add(new ArrayList<>());
            } else {
                assertTrue(isHybridQueryScoreDocElement(scoreDocs[i]));
                hits.get(hits.size() - 1).add(scoreDocs[i]);
            }
        }
        return hits;
    }

    /** Same hits per sub-query (checked first, for readable failures), then the same entries with sentinels, total and max score. */
    private static void assertSameResult(final TopDocsAndMaxScore expected, final TopDocsAndMaxScore actual) {
        final List<List<ScoreDoc>> expectedHits = subQueryHits(expected.topDocs.scoreDocs);
        final List<List<ScoreDoc>> actualHits = subQueryHits(actual.topDocs.scoreDocs);
        assertEquals(NUM_SUB_QUERIES, expectedHits.size());
        assertEquals(NUM_SUB_QUERIES, actualHits.size());
        for (int subQuery = 0; subQuery < NUM_SUB_QUERIES; subQuery++) {
            assertSameHits(String.format(Locale.ROOT, "sub-query %d", subQuery), expectedHits.get(subQuery), actualHits.get(subQuery));
        }
        assertSameEntries(expected.topDocs.scoreDocs, actual.topDocs.scoreDocs);
        assertEquals(expected.topDocs.totalHits, actual.topDocs.totalHits);
        assertEquals(expected.maxScore, actual.maxScore, 0.0f);
    }

    /** The request's sort fields, then the same sort values entry by entry, sentinels included. */
    private static void assertSameSortValues(final TopDocs expected, final TopDocs actual, final Sort sort) {
        assertArrayEquals(sort.getSort(), ((TopFieldDocs) actual).fields);
        assertArrayEquals(((TopFieldDocs) expected).fields, ((TopFieldDocs) actual).fields);
        for (int i = 0; i < expected.scoreDocs.length; i++) {
            assertArrayEquals(
                String.format(Locale.ROOT, "sort values at %d", i),
                ((FieldDoc) expected.scoreDocs[i]).fields,
                ((FieldDoc) actual.scoreDocs[i]).fields
            );
        }
    }

    /** Same collapse field and the same collapse value entry by entry, sentinels included. */
    private static void assertSameCollapseValues(final CollapseTopFieldDocs expected, final CollapseTopFieldDocs actual) {
        assertEquals(expected.field, actual.field);
        assertEquals(expected.scoreDocs.length, expected.collapseValues.length);
        assertEquals(actual.scoreDocs.length, actual.collapseValues.length);
        assertArrayEquals(expected.collapseValues, actual.collapseValues);
    }

    private static List<Integer> docs(final List<ScoreDoc> hits) {
        return hits.stream().map(hit -> hit.doc).toList();
    }

    private static List<Integer> docs(final TopDocs topDocs) {
        return docs(Arrays.asList(topDocs.scoreDocs));
    }

    private static FieldDoc fieldDoc(final int doc, final Object... sortValues) {
        return new FieldDoc(doc, 1.0f, sortValues);
    }

    private static CollapseTopFieldDocs collapseTopDocs(final Sort sort, final FieldDoc[] hits, final Object... collapseValues) {
        return new CollapseTopFieldDocs(
            GROUP_FIELD,
            new TotalHits(hits.length, TotalHits.Relation.EQUAL_TO),
            hits,
            sort.getSort(),
            collapseValues
        );
    }

    private static TopFieldDocs topFieldDocs(final Sort sort, final FieldDoc... hits) {
        return new TopFieldDocs(new TotalHits(hits.length, TotalHits.Relation.EQUAL_TO), hits, sort.getSort());
    }

    private static ScoreDoc hit(final int doc, final float score) {
        return new ScoreDoc(doc, score);
    }

    private static TopDocs topDocs(final ScoreDoc... hits) {
        return new TopDocs(new TotalHits(hits.length, TotalHits.Relation.EQUAL_TO), hits);
    }

    private static void assertHits(final TopDocs actual, final ScoreDoc... expected) {
        assertEquals("hits", expected.length, actual.scoreDocs.length);
        for (int i = 0; i < expected.length; i++) {
            assertEquals(String.format(Locale.ROOT, "doc at %d", i), expected[i].doc, actual.scoreDocs[i].doc);
            assertEquals(String.format(Locale.ROOT, "score at %d", i), expected[i].score, actual.scoreDocs[i].score, 0.0f);
        }
    }

    private static void assertSameEntries(final ScoreDoc[] expected, final ScoreDoc[] actual) {
        assertEquals("entries", expected.length, actual.length);
        for (int i = 0; i < expected.length; i++) {
            assertEquals(String.format(Locale.ROOT, "doc at %d", i), expected[i].doc, actual[i].doc);
            assertEquals(String.format(Locale.ROOT, "score at %d", i), expected[i].score, actual[i].score, 0.0f);
        }
    }

    private static void assertSameHits(final String message, final List<ScoreDoc> expected, final List<ScoreDoc> actual) {
        assertEquals(String.format(Locale.ROOT, "%s size", message), expected.size(), actual.size());
        for (int i = 0; i < expected.size(); i++) {
            assertEquals(String.format(Locale.ROOT, "%s doc at %d", message, i), expected.get(i).doc, actual.get(i).doc);
            assertEquals(String.format(Locale.ROOT, "%s score at %d", message, i), expected.get(i).score, actual.get(i).score, 0.0f);
        }
    }
}
