/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.sparse.codec;

import lombok.extern.log4j.Log4j2;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.lucene.codecs.FieldsProducer;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.MergeState;
import org.apache.lucene.index.PostingsEnum;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.util.BytesRef;
import org.opensearch.neuralsearch.sparse.SparseTokensField;
import org.opensearch.neuralsearch.sparse.algorithm.ClusterTrainingRunning;
import org.opensearch.neuralsearch.sparse.algorithm.DocumentCluster;
import org.opensearch.neuralsearch.sparse.algorithm.KMeansPlusPlus;
import org.opensearch.neuralsearch.sparse.algorithm.PostingClustering;
import org.opensearch.neuralsearch.sparse.common.DocFreq;
import org.opensearch.neuralsearch.sparse.common.DocFreqIterator;
import org.opensearch.neuralsearch.sparse.common.InMemoryKey;
import org.opensearch.neuralsearch.sparse.common.IteratorWrapper;
import org.opensearch.neuralsearch.sparse.common.SparseVector;
import org.opensearch.neuralsearch.sparse.mapper.SparseMethodContext;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;

/**
 * Merge sparse postings
 */
@Log4j2
public class SparsePostingsReader {
    private final MergeState mergeState;

    public SparsePostingsReader(MergeState state) {
        this.mergeState = state;
    }

    public void merge() throws IOException {
        Set<String> fields = new TreeSet<>();
        List<SparsePostingsProducer> fieldsProducers = new ArrayList<>();
        for (FieldsProducer fieldsProducer : mergeState.fieldsProducers) {
            if (fieldsProducer instanceof SparsePostingsProducer) {
                fieldsProducers.add((SparsePostingsProducer) fieldsProducer);
            }
        }

        for (FieldInfo fieldInfo : mergeState.mergeFieldInfos) {
            if (!SparseTokensField.isSparseField(fieldInfo)) {
                continue;
            }
            fields.add(fieldInfo.name);
            Map<BytesRef, Set<DocFreq>> docs = new TreeMap<>();
            Map<Integer, Pair<Integer, InMemoryKey.IndexKey>> newToOldDocIdMap = new HashMap<>();
            for (int i = 0; i < this.mergeState.fieldsProducers.length; i++) {
                if (!(this.mergeState.fieldsProducers[i] instanceof SparsePostingsProducer)) {
                    continue;
                }
                SparsePostingsProducer fieldsProducer = (SparsePostingsProducer) this.mergeState.fieldsProducers[i];
                Terms terms = fieldsProducer.terms(fieldInfo.name);
                TermsEnum termsEnum = terms.iterator();
                BytesRef term = termsEnum.next();
                while (term != null) {
                    if (!docs.containsKey(term)) {
                        docs.put(term, new TreeSet<>());
                    }
                    PostingsEnum postings = termsEnum.postings(null);
                    if (postings instanceof SparsePostingsEnum) {
                        SparsePostingsEnum sparsePostingsEnum = (SparsePostingsEnum) postings;
                        IteratorWrapper<DocumentCluster> clusterIter = sparsePostingsEnum.clusterIterator();
                        while (clusterIter.next() != null) {
                            DocFreqIterator docIter = clusterIter.getCurrent().getDisi();
                            while (docIter.nextDoc() != PostingsEnum.NO_MORE_DOCS) {
                                int newDocId = this.mergeState.docMaps[i].get(docIter.docID());
                                newToOldDocIdMap.put(
                                    newDocId,
                                    Pair.of(docIter.docID(), new InMemoryKey.IndexKey(fieldsProducer.getState().segmentInfo, fieldInfo))
                                );
                                docs.get(term).add(new DocFreq(newDocId, docIter.freq()));
                            }
                        }
                    }
                    term = termsEnum.next();
                }
            }

            InMemoryKey.IndexKey key = new InMemoryKey.IndexKey(mergeState.segmentInfo, fieldInfo);
            InMemorySparseVectorForwardIndex index = InMemorySparseVectorForwardIndex.get(key);
            int beta = Integer.parseInt(fieldInfo.attributes().get(SparseMethodContext.BETA_FIELD));
            int lambda = Integer.parseInt(fieldInfo.attributes().get(SparseMethodContext.LAMBDA_FIELD));
            float alpha = Float.parseFloat(fieldInfo.attributes().get(SparseMethodContext.ALPHA_FIELD));
            int clusterUtilDocCountReach = Integer.parseInt(fieldInfo.attributes().get(SparseMethodContext.CLUSTER_UNTIL_FIELD));
            int docCount = 0;
            for (int n : mergeState.maxDocs) {
                docCount += n;
            }
            log.info("Merge total doc: {}", docCount);
            if (clusterUtilDocCountReach > 0 && docCount < clusterUtilDocCountReach) {
                beta = 1;
            }
            PostingClustering postingClustering = new PostingClustering(lambda, new KMeansPlusPlus(alpha, beta, (newDocId) -> {
                if (index != null) {
                    SparseVector vector = index.getForwardIndexReader().readSparseVector(newDocId);
                    if (vector != null) {
                        return vector;
                    }
                }
                // new segment in-memory forward index hasn't been created, use old
                Pair<Integer, InMemoryKey.IndexKey> oldDocId = newToOldDocIdMap.get(newDocId);
                if (oldDocId != null) {
                    InMemorySparseVectorForwardIndex oldIndex = InMemorySparseVectorForwardIndex.get(oldDocId.getRight());
                    if (oldIndex != null) {
                        return oldIndex.getForwardIndexReader().readSparseVector(oldDocId.getLeft());
                    }
                }
                return null;
            }));
            for (Map.Entry<BytesRef, Set<DocFreq>> entry : docs.entrySet()) {
                ClusterTrainingRunning.getInstance().run(new Runnable() {
                    @Override
                    public void run() {
                        List<DocumentCluster> cluster = null;
                        try {
                            cluster = postingClustering.cluster(entry.getValue().stream().toList());
                        } catch (IOException e) {
                            throw new RuntimeException(e);
                        }
                        InMemoryClusteredPosting.InMemoryClusteredPostingWriter.writePostingClusters(key, entry.getKey(), cluster);
                    }
                });
            }
        }
    }
}
