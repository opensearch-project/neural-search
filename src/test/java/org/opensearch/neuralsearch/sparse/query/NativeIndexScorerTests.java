/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.sparse.query;

import lombok.SneakyThrows;
import org.apache.lucene.codecs.Codec;
import org.apache.lucene.index.BinaryDocValues;
import org.apache.lucene.index.DocValuesSkipIndexType;
import org.apache.lucene.index.DocValuesType;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.FieldInfos;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.SegmentInfo;
import org.apache.lucene.index.SegmentWriteState;
import org.apache.lucene.index.VectorEncoding;
import org.apache.lucene.index.VectorSimilarityFunction;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.util.BitSetIterator;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.FixedBitSet;
import org.apache.lucene.util.InfoStream;
import org.apache.lucene.util.StringHelper;
import org.apache.lucene.util.Version;
import org.opensearch.neuralsearch.sparse.AbstractSparseTestBase;
import org.opensearch.neuralsearch.sparse.algorithm.SparseEngine;
import org.opensearch.neuralsearch.sparse.algorithm.SparseForwardIndex;
import org.opensearch.neuralsearch.sparse.codec.CodecUtils;
import org.opensearch.neuralsearch.sparse.codec.nativeindex.DefaultNativeIndexWriter;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.opensearch.neuralsearch.sparse.common.SparseConstants.APPROXIMATE_THRESHOLD_FIELD;
import static org.opensearch.neuralsearch.sparse.common.SparseConstants.CLUSTER_RATIO_FIELD;
import static org.opensearch.neuralsearch.sparse.common.SparseConstants.ENGINE_FIELD;
import static org.opensearch.neuralsearch.sparse.common.SparseConstants.FORWARD_INDEX_FIELD;
import static org.opensearch.neuralsearch.sparse.common.SparseConstants.N_POSTINGS_FIELD;
import static org.opensearch.neuralsearch.sparse.common.SparseConstants.QUANTIZATION_CEILING_INGEST_FIELD;
import static org.opensearch.neuralsearch.sparse.common.SparseConstants.QUANTIZATION_CEILING_SEARCH_FIELD;
import static org.opensearch.neuralsearch.sparse.common.SparseConstants.SUMMARY_PRUNE_RATIO_FIELD;
import static org.opensearch.neuralsearch.sparse.mapper.SparseVectorField.SPARSE_FIELD;

/**
 * Scores against a real nsparse index built by the production writer, since every path through the
 * scorer ends in a native call. The index is deliberately kept below the approximate threshold so it
 * is an unquantized inverted index: the scores are then exact dot products, which lets the
 * assertions pin the arithmetic rather than a ranking.
 */
public class NativeIndexScorerTests extends AbstractSparseTestBase {

    private static final String FIELD = "test_field";
    private static final int DOC_COUNT = 6;

    private Directory directory;
    private SegmentInfo segmentInfo;
    private FieldInfo fieldInfo;
    private LeafReader leafReader;

    @SneakyThrows
    @Override
    public void setUp() {
        super.setUp();
        directory = FSDirectory.open(createTempDir());
        segmentInfo = segmentInfo();
        fieldInfo = fieldInfo();

        // doc i carries token 7 with weight i+1, so the score against {7: 1.0} is i+1
        List<Map<Integer, Float>> vectors = new ArrayList<>();
        for (int doc = 0; doc < DOC_COUNT; doc++) {
            Map<Integer, Float> vector = new LinkedHashMap<>();
            vector.put(7, 1.0f + doc);
            vectors.add(vector);
        }
        new DefaultNativeIndexWriter(writeState(), fieldInfo).writeIndex(docValues(vectors));
        segmentInfo.setFiles(Set.of(engineFileName()));

        leafReader = mock(LeafReader.class);
        when(leafReader.getCoreCacheHelper()).thenReturn(null);
    }

    @SneakyThrows
    @Override
    public void tearDown() {
        directory.close();
        super.tearDown();
    }

    @SneakyThrows
    public void testScoresInDocOrderNotScoreOrder() {
        NativeIndexScorer scorer = scorer(3, null, null, 1.0f);

        List<Integer> docs = drain(scorer);

        // nsparse returns by score, but a DocIdSetIterator has to ascend or a conjunction trips an
        // assertion. The top 3 by weight are docs 3, 4, 5.
        assertEquals(List.of(3, 4, 5), docs);
    }

    @SneakyThrows
    public void testScoreIsTheDotProduct() {
        NativeIndexScorer scorer = scorer(1, null, null, 1.0f);

        assertEquals(DOC_COUNT - 1, scorer.iterator().nextDoc());
        assertEquals((float) DOC_COUNT, scorer.score(), 0.001f);
    }

    @SneakyThrows
    public void testBoostMultipliesTheScore() {
        NativeIndexScorer scorer = scorer(1, null, null, 2.5f);

        scorer.iterator().nextDoc();
        // nsparse decodes its own quantization, so the boost is a plain multiply
        assertEquals(DOC_COUNT * 2.5f, scorer.score(), 0.001f);
    }

    @SneakyThrows
    public void testFilterRestrictsTheCandidateSet() {
        FixedBitSet filter = new FixedBitSet(DOC_COUNT);
        filter.set(1);
        filter.set(2);

        List<Integer> docs = drain(scorer(DOC_COUNT, null, new BitSetIterator(filter, filter.cardinality()), 1.0f));

        assertEquals("only the filtered docs are candidates", List.of(1, 2), docs);
    }

    @SneakyThrows
    public void testFilterIsIntersectedWithLiveDocs() {
        FixedBitSet filter = new FixedBitSet(DOC_COUNT);
        filter.set(1);
        filter.set(2);
        FixedBitSet live = new FixedBitSet(DOC_COUNT);
        live.set(0, DOC_COUNT);
        live.clear(2);

        List<Integer> docs = drain(scorer(DOC_COUNT, live, new BitSetIterator(filter, filter.cardinality()), 1.0f));

        assertEquals("doc 2 is deleted, so it cannot survive the filter", List.of(1), docs);
    }

    @SneakyThrows
    public void testDeletedDocsAreDroppedFromTheResults() {
        FixedBitSet live = new FixedBitSet(DOC_COUNT);
        live.set(0, DOC_COUNT);
        live.clear(DOC_COUNT - 1);

        List<Integer> docs = drain(scorer(2, live, null, 1.0f));

        // The top scorer is deleted; refetching keeps k intact rather than returning one
        assertFalse("the deleted doc must not be returned", docs.contains(DOC_COUNT - 1));
        assertEquals(2, docs.size());
    }

    /**
     * The fetch starts at k and grows only when deleted hits ate into it, so a top-k that is entirely
     * deleted has to come back from a second, larger call -- a single fetch of k would return nothing.
     */
    @SneakyThrows
    public void testFetchGrowsWhenTheWholeTopKIsDeleted() {
        FixedBitSet live = new FixedBitSet(DOC_COUNT);
        live.set(0, DOC_COUNT);
        live.clear(DOC_COUNT - 1);
        live.clear(DOC_COUNT - 2);

        List<Integer> docs = drain(scorer(2, live, null, 1.0f));

        assertEquals("the best two live docs, not the best two docs", List.of(2, 3), docs);
    }

    /**
     * Growth is capped at maxDoc: once the whole segment has been fetched there is nothing left to ask
     * for, so a k that cannot be filled has to end the retries rather than spin.
     */
    @SneakyThrows
    public void testFetchStopsGrowingWhenTooFewDocsAreLive() {
        FixedBitSet live = new FixedBitSet(DOC_COUNT);
        live.set(0);
        live.set(1);

        List<Integer> docs = drain(scorer(DOC_COUNT, live, null, 1.0f));

        assertEquals(List.of(0, 1), docs);
    }

    @SneakyThrows
    public void testNoLiveDocsAndNoFilterReturnsEverythingRequested() {
        List<Integer> docs = drain(scorer(DOC_COUNT, null, null, 1.0f));

        assertEquals(DOC_COUNT, docs.size());
    }

    @SneakyThrows
    public void testEmptyFilterMatchesNothing() {
        FixedBitSet filter = new FixedBitSet(DOC_COUNT);

        List<Integer> docs = drain(scorer(DOC_COUNT, null, new BitSetIterator(filter, 0), 1.0f));

        assertTrue("an empty candidate set cannot produce hits", docs.isEmpty());
    }

    /**
     * Advertises no upper bound, matching the Lucene-path scorers. Pinned because the top-k is
     * already fixed before iteration, so there is nothing for a block-max collector to skip.
     */
    @SneakyThrows
    public void testAdvertisesNoMaxScore() {
        NativeIndexScorer scorer = scorer(DOC_COUNT, null, null, 1.0f);

        assertEquals(0.0f, scorer.getMaxScore(DocIdSetIterator.NO_MORE_DOCS), 0.0f);
    }

    // ---- helpers ----

    @SneakyThrows
    private NativeIndexScorer scorer(int k, Bits acceptedDocs, BitSetIterator filter, float boost) {
        return new NativeIndexScorer(
            fieldInfo,
            new SparseQueryContext(List.of("7"), 1.0f, k),
            Map.of(7, 1.0f),
            leafReader,
            segmentInfo,
            acceptedDocs,
            filter,
            boost
        );
    }

    @SneakyThrows
    private List<Integer> drain(NativeIndexScorer scorer) {
        List<Integer> docs = new ArrayList<>();
        DocIdSetIterator iterator = scorer.iterator();
        for (int doc = iterator.nextDoc(); doc != DocIdSetIterator.NO_MORE_DOCS; doc = iterator.nextDoc()) {
            docs.add(doc);
        }
        return docs;
    }

    private String engineFileName() {
        return CodecUtils.buildIndexFileName(segmentInfo.name, SparseEngine.NATIVE.version(), FIELD, SparseEngine.NATIVE.extension());
    }

    private SegmentWriteState writeState() {
        return new SegmentWriteState(
            InfoStream.getDefault(),
            directory,
            segmentInfo,
            new FieldInfos(new FieldInfo[] { fieldInfo }),
            null,
            IOContext.DEFAULT
        );
    }

    private SegmentInfo segmentInfo() {
        return new SegmentInfo(
            directory,
            Version.LATEST,
            Version.LATEST,
            "_0",
            DOC_COUNT,
            false,
            false,
            Codec.getDefault(),
            Collections.emptyMap(),
            new byte[StringHelper.ID_LENGTH],
            Collections.emptyMap(),
            null
        );
    }

    private FieldInfo fieldInfo() {
        FieldInfo info = new FieldInfo(
            FIELD,
            0,
            false,
            false,
            false,
            IndexOptions.DOCS,
            DocValuesType.BINARY,
            DocValuesSkipIndexType.NONE,
            -1,
            new HashMap<>(),
            0,
            0,
            0,
            0,
            VectorEncoding.FLOAT32,
            VectorSimilarityFunction.EUCLIDEAN,
            false,
            false
        );
        info.putAttribute(SPARSE_FIELD, "true");
        info.putAttribute(ENGINE_FIELD, SparseEngine.NATIVE.getName());
        info.putAttribute(FORWARD_INDEX_FIELD, SparseForwardIndex.SHARED.getName());
        // Above the segment's doc count, so the writer builds an unquantized inverted index
        info.putAttribute(APPROXIMATE_THRESHOLD_FIELD, String.valueOf(Integer.MAX_VALUE));
        info.putAttribute(N_POSTINGS_FIELD, "10");
        info.putAttribute(CLUSTER_RATIO_FIELD, "0.5");
        info.putAttribute(SUMMARY_PRUNE_RATIO_FIELD, "1.0");
        info.putAttribute(QUANTIZATION_CEILING_INGEST_FIELD, "32.0");
        info.putAttribute(QUANTIZATION_CEILING_SEARCH_FIELD, "32.0");
        return info;
    }

    private BinaryDocValues docValues(List<Map<Integer, Float>> vectors) {
        return new BinaryDocValues() {
            private int doc = -1;

            @Override
            public BytesRef binaryValue() {
                return encode(vectors.get(doc));
            }

            @Override
            public boolean advanceExact(int target) {
                doc = target;
                return true;
            }

            @Override
            public int docID() {
                return doc;
            }

            @Override
            public int nextDoc() {
                doc++;
                return doc < vectors.size() ? doc : NO_MORE_DOCS;
            }

            @Override
            public int advance(int target) {
                doc = target;
                return doc < vectors.size() ? doc : NO_MORE_DOCS;
            }

            @Override
            public long cost() {
                return vectors.size();
            }
        };
    }

    @SneakyThrows
    private BytesRef encode(Map<Integer, Float> weights) {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        try (DataOutputStream dos = new DataOutputStream(baos)) {
            for (Map.Entry<Integer, Float> entry : weights.entrySet()) {
                dos.writeInt(entry.getKey());
                dos.writeFloat(entry.getValue());
            }
        }
        return new BytesRef(baos.toByteArray());
    }

    /**
     * A seismic segment gets cut/heap_factor and the query range, and a per_block field also gets
     * k_prime -- the only shape a disk_seismic_sq index reads a query range from.
     */
    @SneakyThrows
    public void testSeismicSegmentIsQueriedWithTheQuantizedParameters() {
        // A fresh field over the threshold, and per_block so the k_prime branch runs too
        FieldInfo seismicField = fieldInfo();
        seismicField.putAttribute(APPROXIMATE_THRESHOLD_FIELD, "1");
        seismicField.putAttribute(FORWARD_INDEX_FIELD, SparseForwardIndex.PER_BLOCK.getName());

        Directory seismicDir = FSDirectory.open(createTempDir());
        try {
            SegmentInfo info = segmentInfo(seismicDir);
            List<Map<Integer, Float>> vectors = new ArrayList<>();
            for (int doc = 0; doc < DOC_COUNT; doc++) {
                vectors.add(Map.of(7, 1.0f + doc));
            }
            new DefaultNativeIndexWriter(
                new SegmentWriteState(
                    InfoStream.getDefault(),
                    seismicDir,
                    info,
                    new FieldInfos(new FieldInfo[] { seismicField }),
                    null,
                    IOContext.DEFAULT
                ),
                seismicField
            ).writeIndex(docValues(vectors));
            info.setFiles(
                Set.of(CodecUtils.buildIndexFileName(info.name, SparseEngine.NATIVE.version(), FIELD, SparseEngine.NATIVE.extension()))
            );

            NativeIndexScorer scorer = new NativeIndexScorer(
                seismicField,
                new SparseQueryContext(List.of("7"), 1.0f, 2),
                Map.of(7, 1.0f),
                leafReader,
                info,
                null,
                null,
                1.0f
            );

            assertFalse("the quantized path still returns hits", drain(scorer).isEmpty());
        } finally {
            seismicDir.close();
        }
    }

    @SneakyThrows
    public void testIteratorAdvanceSkipsToTheRequestedDoc() {
        NativeIndexScorer scorer = scorer(DOC_COUNT, null, null, 1.0f);
        DocIdSetIterator iterator = scorer.iterator();

        assertEquals(3, iterator.advance(3));
        assertEquals(3, scorer.docID());
        assertEquals(4, iterator.nextDoc());
    }

    /**
     * The filter iterator masks live docs as it goes, so advance() landing on a deleted doc has to
     * keep walking rather than return it.
     */
    @SneakyThrows
    public void testFilterAdvanceSkipsDeletedDocs() {
        FixedBitSet filter = new FixedBitSet(DOC_COUNT);
        filter.set(2);
        filter.set(4);
        FixedBitSet live = new FixedBitSet(DOC_COUNT);
        live.set(0, DOC_COUNT);
        live.clear(2);

        List<Integer> docs = drain(scorer(DOC_COUNT, live, new BitSetIterator(filter, filter.cardinality()), 1.0f));

        assertEquals(List.of(4), docs);
    }

    private SegmentInfo segmentInfo(Directory dir) {
        return new SegmentInfo(
            dir,
            Version.LATEST,
            Version.LATEST,
            "_0",
            DOC_COUNT,
            false,
            false,
            Codec.getDefault(),
            Collections.emptyMap(),
            new byte[StringHelper.ID_LENGTH],
            Collections.emptyMap(),
            null
        );
    }
}
