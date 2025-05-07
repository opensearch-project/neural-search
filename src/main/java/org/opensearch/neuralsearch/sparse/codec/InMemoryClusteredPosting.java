/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.sparse.codec;

import lombok.AllArgsConstructor;
import org.apache.lucene.codecs.BlockTermState;
import org.apache.lucene.codecs.NormsProducer;
import org.apache.lucene.codecs.PushPostingsWriterBase;
import org.apache.lucene.codecs.lucene101.Lucene101PostingsFormat;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.index.SegmentWriteState;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.store.DataOutput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.util.Accountable;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.FixedBitSet;
import org.apache.lucene.util.RamUsageEstimator;
import org.opensearch.neuralsearch.sparse.algorithm.DocumentCluster;
import org.opensearch.neuralsearch.sparse.algorithm.PostingClustering;
import org.opensearch.neuralsearch.sparse.algorithm.PostingClusters;
import org.opensearch.neuralsearch.sparse.common.DocFreq;
import org.opensearch.neuralsearch.sparse.common.InMemoryKey;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

/**
 * This class manages the in-memory postings for sparse vectors. It provides methods to write and read postings from memory.
 * It is used by the SparsePostingsConsumer and SparsePostingsReader classes.
 */
public class InMemoryClusteredPosting implements Accountable {
    public static final Map<InMemoryKey.IndexKey, Map<String, PostingClusters>> inMemoryPostings = new ConcurrentHashMap<>();

    public static void clearIndex(InMemoryKey.IndexKey key) {
        inMemoryPostings.remove(key);
    }

    static final int MAX_FREQ = Float.floatToIntBits(Float.MAX_VALUE) >>> 15;

    static float decodeFeatureValue(float freq) {
        if (freq > MAX_FREQ) {
            // This is never used in practice but callers of the SimScorer API might
            // occasionally call it on eg. Float.MAX_VALUE to compute the max score
            // so we need to be consistent.
            return Float.MAX_VALUE;
        }
        int tf = (int) freq; // lossless
        int featureBits = tf << 15;
        return Float.intBitsToFloat(featureBits);
    }

    @Override
    public long ramBytesUsed() {
        long ramUsed = 0;
        for (Map.Entry<InMemoryKey.IndexKey, Map<String, PostingClusters>> entry : inMemoryPostings.entrySet()) {
            ramUsed += RamUsageEstimator.shallowSizeOfInstance(InMemoryKey.IndexKey.class);
            for (Map.Entry<String, PostingClusters> entry2 : entry.getValue().entrySet()) {
                ramUsed += RamUsageEstimator.shallowSizeOfInstance(String.class);
                ramUsed += entry2.getValue().ramBytesUsed();
            }
        }
        return ramUsed;
    }

    @AllArgsConstructor
    public static class InMemoryClusteredPostingReader {
        private final InMemoryKey.IndexKey key;

        public PostingClusters read(BytesRef term) {
            String stringTerm = term.utf8ToString();
            return inMemoryPostings.getOrDefault(key, Collections.emptyMap()).get(stringTerm);
        }

        public Set<BytesRef> getTerms() {
            Map<String, PostingClusters> innerMap = inMemoryPostings.get(key);
            if (innerMap == null) {
                return Collections.emptySet();
            }
            // Create an unmodifiable copy of the keySet to ensure thread-safety
            return innerMap.keySet().stream().map(BytesRef::new).collect(Collectors.toSet());
        }
    }

    public static class InMemoryClusteredPostingWriter extends PushPostingsWriterBase {

        private List<DocFreq> docFreqs = new ArrayList<>();
        private BytesRef currentTerm;
        private final PostingClustering postingClustering;
        private final InMemoryKey.IndexKey key;

        public InMemoryClusteredPostingWriter(InMemoryKey.IndexKey key, FieldInfo fieldInfo, PostingClustering postingClustering) {
            super();
            setField(fieldInfo);
            this.key = key;
            this.postingClustering = postingClustering;
        }

        public BlockTermState writeInMemoryTerm(BytesRef term, TermsEnum termsEnum, FixedBitSet docsSeen, NormsProducer norms)
            throws IOException {
            this.currentTerm = term;
            return super.writeTerm(term, termsEnum, docsSeen, norms);
        }

        @Override
        public BlockTermState newTermState() throws IOException {
            return new Lucene101PostingsFormat.IntBlockTermState();
        }

        @Override
        public void startTerm(NumericDocValues norms) throws IOException {
            docFreqs.clear();
        }

        public static void writePostingClusters(InMemoryKey.IndexKey key, BytesRef term, List<DocumentCluster> clusters) {
            if (clusters == null || clusters.isEmpty()) {
                return;
            }

            inMemoryPostings.compute(key, (k, existingMap) -> {
                if (existingMap == null) {
                    existingMap = new ConcurrentHashMap<>();
                }
                existingMap.put(term.utf8ToString(), new PostingClusters(clusters));
                return existingMap;
            });
        }

        @Override
        public void finishTerm(BlockTermState state) throws IOException {
            List<DocumentCluster> clusters = this.postingClustering.cluster(docFreqs);
            writePostingClusters(key, this.currentTerm, clusters);
            this.docFreqs.clear();
            this.currentTerm = null;
        }

        @Override
        public void startDoc(int docID, int freq) throws IOException {
            if (docID == -1) {
                throw new IllegalStateException("docId must be set before startDoc");
            }
            docFreqs.add(new DocFreq(docID, decodeFeatureValue(freq)));
        }

        @Override
        public void addPosition(int position, BytesRef payload, int startOffset, int endOffset) throws IOException {

        }

        @Override
        public void finishDoc() throws IOException {

        }

        @Override
        public void init(IndexOutput termsOut, SegmentWriteState state) throws IOException {

        }

        @Override
        public void encodeTerm(DataOutput out, FieldInfo fieldInfo, BlockTermState state, boolean absolute) throws IOException {

        }

        @Override
        public void close() throws IOException {

        }
    }
}
