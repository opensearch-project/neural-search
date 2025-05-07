/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.sparse.codec;

import lombok.extern.log4j.Log4j2;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.lucene.codecs.FieldsProducer;
import org.apache.lucene.index.BinaryDocValues;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.MergeState;
import org.apache.lucene.index.PostingsEnum;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.util.BytesRef;
import org.opensearch.neuralsearch.sparse.SparseTokensField;
import org.opensearch.neuralsearch.sparse.algorithm.ClusterTrainingRunning;
import org.opensearch.neuralsearch.sparse.algorithm.ClusteringTask;
import org.opensearch.neuralsearch.sparse.algorithm.DocumentCluster;
import org.opensearch.neuralsearch.sparse.common.DocFreq;
import org.opensearch.neuralsearch.sparse.common.DocFreqIterator;
import org.opensearch.neuralsearch.sparse.common.InMemoryKey;
import org.opensearch.neuralsearch.sparse.common.IteratorWrapper;
import org.opensearch.neuralsearch.sparse.mapper.SparseMethodContext;

import java.io.IOException;
import java.util.HashMap;
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

    public void merge() throws IOException, InterruptedException {
        for (FieldInfo fieldInfo : mergeState.mergeFieldInfos) {
            if (!SparseTokensField.isSparseField(fieldInfo)) {
                continue;
            }
            Map<BytesRef, Set<DocFreq>> docs = new TreeMap<>();
            Map<Integer, Pair<Integer, InMemoryKey.IndexKey>> newToOldDocIdMap = new HashMap<>();
            for (int i = 0; i < this.mergeState.fieldsProducers.length; i++) {
                FieldsProducer fieldsProducer = this.mergeState.fieldsProducers[i];
                // we need this SparseBinaryDocValuesPassThrough to get segment info
                BinaryDocValues binaryDocValues = this.mergeState.docValuesProducers[i].getBinary(fieldInfo);
                if (!(binaryDocValues instanceof SparseBinaryDocValuesPassThrough)) {
                    log.error("binaryDocValues is not SparseBinaryDocValuesPassThrough, {}", binaryDocValues.getClass().getName());
                    continue;
                }
                Terms terms = fieldsProducer.terms(fieldInfo.name);
                if (terms == null) {
                    log.error("terms is null");
                    continue;
                }
                TermsEnum termsEnum = terms.iterator();
                if (termsEnum == null) {
                    log.error("termsEnum is null");
                    continue;
                }
                BytesRef term = termsEnum.next();
                while (term != null) {
                    if (!docs.containsKey(term)) {
                        docs.put(term, new TreeSet<>());
                    }
                    PostingsEnum postings = termsEnum.postings(null);
                    if (!(postings instanceof SparsePostingsEnum)) {
                        log.error("postings is not SparsePostingsEnum, {}", postings);
                        continue;
                    }
                    SparsePostingsEnum sparsePostingsEnum = (SparsePostingsEnum) postings;
                    IteratorWrapper<DocumentCluster> clusterIter = sparsePostingsEnum.clusterIterator();
                    while (clusterIter.next() != null) {
                        DocFreqIterator docIter = clusterIter.getCurrent().getDisi();
                        while (docIter.nextDoc() != PostingsEnum.NO_MORE_DOCS) {
                            if (docIter.docID() == -1) {
                                log.error("docId is -1");
                                continue;
                            }
                            int newDocId = this.mergeState.docMaps[i].get(docIter.docID());
                            if (newDocId == -1) {
                                continue;
                            }
                            newToOldDocIdMap.put(
                                newDocId,
                                Pair.of(
                                    docIter.docID(),
                                    new InMemoryKey.IndexKey(
                                        ((SparseBinaryDocValuesPassThrough) binaryDocValues).getSegmentInfo(),
                                        fieldInfo
                                    )
                                )
                            );
                            docs.get(term).add(new DocFreq(newDocId, docIter.freq()));
                        }
                    }
                    term = termsEnum.next();
                }
            }

            InMemoryKey.IndexKey key = new InMemoryKey.IndexKey(mergeState.segmentInfo, fieldInfo);
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
            for (Map.Entry<BytesRef, Set<DocFreq>> entry : docs.entrySet()) {
                if (beta == 1) {
                    // not run asynchronously
                    new ClusteringTask(entry.getKey(), entry.getValue(), key, alpha, beta, lambda, newToOldDocIdMap).run();
                } else {
                    ClusterTrainingRunning.getInstance()
                        .run(new ClusteringTask(entry.getKey(), entry.getValue(), key, alpha, beta, lambda, newToOldDocIdMap));
                }
            }
        }
    }
}
