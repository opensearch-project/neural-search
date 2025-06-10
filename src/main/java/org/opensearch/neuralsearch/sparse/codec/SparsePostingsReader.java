/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.sparse.codec;

import lombok.extern.log4j.Log4j2;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.lucene.codecs.BlockTermState;
import org.apache.lucene.codecs.FieldsProducer;
import org.apache.lucene.index.BinaryDocValues;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.MergeState;
import org.apache.lucene.index.PostingsEnum;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.util.BytesRef;
import org.opensearch.neuralsearch.sparse.SparseTokensField;
import org.opensearch.neuralsearch.sparse.algorithm.BatchClusteringTask;
import org.opensearch.neuralsearch.sparse.algorithm.ClusterTrainingRunning;
import org.opensearch.neuralsearch.sparse.algorithm.DocumentCluster;
import org.opensearch.neuralsearch.sparse.algorithm.PostingClusters;
import org.opensearch.neuralsearch.sparse.common.DocFreq;
import org.opensearch.neuralsearch.sparse.common.DocFreqIterator;
import org.opensearch.neuralsearch.sparse.common.InMemoryKey;
import org.opensearch.neuralsearch.sparse.common.IteratorWrapper;
import org.opensearch.neuralsearch.sparse.mapper.SparseMethodContext;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;

/**
 * Merge sparse postings
 */
@Log4j2
public class SparsePostingsReader {
    private final MergeState mergeState;
    // this is a magic number for now which is effective
    private static final int BATCH_SIZE = 50;

    public SparsePostingsReader(MergeState state) {
        this.mergeState = state;
    }

    public void merge(SparseTermsLuceneWriter sparseTermsLuceneWriter, ClusteredPostingTermsWriter clusteredPostingTermsWriter)
        throws Exception {
        int docCount = 0;
        for (int n : mergeState.maxDocs) {
            docCount += n;
        }
        log.debug("Merge total doc: {}", docCount);
        List<FieldInfo> sparseFieldInfos = new ArrayList<>();
        for (FieldInfo fieldInfo : mergeState.mergeFieldInfos) {
            if (SparseTokensField.isSparseField(fieldInfo)) {
                sparseFieldInfos.add(fieldInfo);
            }
        }

        sparseTermsLuceneWriter.writeFieldCount(sparseFieldInfos.size());
        for (FieldInfo fieldInfo : sparseFieldInfos) {
            log.debug("Merge field: {}", fieldInfo.name);
            sparseTermsLuceneWriter.writeFieldNumber(fieldInfo.number);

            InMemoryKey.IndexKey key = new InMemoryKey.IndexKey(mergeState.segmentInfo, fieldInfo);
            int beta = Integer.parseInt(fieldInfo.attributes().get(SparseMethodContext.BETA_FIELD));
            int lambda = Integer.parseInt(fieldInfo.attributes().get(SparseMethodContext.LAMBDA_FIELD));
            float alpha = Float.parseFloat(fieldInfo.attributes().get(SparseMethodContext.ALPHA_FIELD));
            int clusterUtilDocCountReach = Integer.parseInt(fieldInfo.attributes().get(SparseMethodContext.CLUSTER_UNTIL_FIELD));

            if (clusterUtilDocCountReach > 0 && docCount < clusterUtilDocCountReach) {
                beta = 1;
            }

            // get all terms of old segments from InMemoryClusteredPosting
            Set<BytesRef> allTerms = getAllTerms(fieldInfo);
            sparseTermsLuceneWriter.writeTermsSize(allTerms.size());
            clusteredPostingTermsWriter.setFieldAndMaxDoc(fieldInfo, docCount);

            List<CompletableFuture<List<Pair<BytesRef, PostingClusters>>>> futures = new ArrayList<>(
                Math.round((float) allTerms.size() / BATCH_SIZE)
            );
            int i = 0;
            List<BytesRef> termBatch = new ArrayList<>(BATCH_SIZE);
            for (BytesRef term : allTerms) {
                termBatch.add(term);
                if (termBatch.size() == BATCH_SIZE || i == allTerms.size() - 1) {
                    if (beta == 1) {
                        futures.add(
                            CompletableFuture.completedFuture(
                                new BatchClusteringTask(termBatch, key, alpha, beta, lambda, mergeState, fieldInfo).get()
                            )
                        );

                    } else {
                        futures.add(
                            CompletableFuture.supplyAsync(
                                new BatchClusteringTask(termBatch, key, alpha, beta, lambda, mergeState, fieldInfo),
                                ClusterTrainingRunning.getInstance().getExecutor()
                            )
                        );
                    }
                    termBatch = new ArrayList<>(BATCH_SIZE);
                }
                ++i;
            }
            for (int j = 0; j < futures.size(); ++j) {
                try {
                    List<Pair<BytesRef, PostingClusters>> clusters = futures.get(j).join();
                    futures.set(j, null);
                    for (Pair<BytesRef, PostingClusters> p : clusters) {
                        BlockTermState state = clusteredPostingTermsWriter.write(p.getLeft(), p.getRight());
                        sparseTermsLuceneWriter.writeTerm(p.getLeft(), state);
                    }
                } catch (CancellationException | CompletionException ex) {
                    log.error("Thread of running clustering from {}th term batch during merge has exception", j, ex);
                } catch (IOException ex) {
                    clusteredPostingTermsWriter.closeWithException();
                    sparseTermsLuceneWriter.closeWithException();
                    throw ex;
                }
            }
        }
    }

    // get all terms of old segments from InMemoryClusteredPosting
    private Set<BytesRef> getAllTerms(FieldInfo fieldInfo) throws IOException {
        Set<BytesRef> allTerms = new HashSet<>();
        for (int i = 0; i < this.mergeState.fieldsProducers.length; i++) {
            FieldsProducer fieldsProducer = this.mergeState.fieldsProducers[i];
            // we need this SparseBinaryDocValuesPassThrough to get segment info
            BinaryDocValues binaryDocValues = this.mergeState.docValuesProducers[i].getBinary(fieldInfo);
            if (!(binaryDocValues instanceof SparseBinaryDocValuesPassThrough)) {
                log.error("binaryDocValues is not SparseBinaryDocValuesPassThrough, {}", binaryDocValues.getClass().getName());
                continue;
            }

            Terms terms = fieldsProducer.terms(fieldInfo.name);
            if (terms instanceof SparseTerms) {
                SparseTerms sparseTerms = (SparseTerms) terms;
                allTerms.addAll(sparseTerms.getReader().terms());
            } else {
                throw new RuntimeException("terms should always be SparseTerms");
            }
        }
        return allTerms;
    }

    public static List<DocFreq> getMergedPostingForATerm(
        MergeState mergeState,
        BytesRef term,
        FieldInfo fieldInfo,
        Map<Integer, Pair<Integer, InMemoryKey.IndexKey>> newToOldDocIdMap
    ) throws IOException {
        List<DocFreq> docFreqs = new ArrayList<>();
        for (int i = 0; i < mergeState.fieldsProducers.length; i++) {
            FieldsProducer fieldsProducer = mergeState.fieldsProducers[i];
            // we need this SparseBinaryDocValuesPassThrough to get segment info
            BinaryDocValues binaryDocValues = mergeState.docValuesProducers[i].getBinary(fieldInfo);
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

            if (termsEnum.seekCeil(term) == TermsEnum.SeekStatus.NOT_FOUND) {
                continue;
            }
            PostingsEnum postings = termsEnum.postings(null);
            if (!(postings instanceof SparsePostingsEnum)) {
                log.error("postings is not SparsePostingsEnum, {}", postings);
                continue;
            }
            InMemoryKey.IndexKey oldKey = new InMemoryKey.IndexKey(
                ((SparseBinaryDocValuesPassThrough) binaryDocValues).getSegmentInfo(),
                fieldInfo
            );

            SparsePostingsEnum sparsePostingsEnum = (SparsePostingsEnum) postings;
            IteratorWrapper<DocumentCluster> clusterIter = sparsePostingsEnum.clusterIterator();
            while (clusterIter.next() != null) {
                DocFreqIterator docIter = clusterIter.getCurrent().getDisi();
                while (docIter.nextDoc() != PostingsEnum.NO_MORE_DOCS) {
                    if (docIter.docID() == -1) {
                        log.error("docId is -1");
                        continue;
                    }
                    int newDocId = mergeState.docMaps[i].get(docIter.docID());
                    if (newDocId == -1) {
                        continue;
                    }
                    newToOldDocIdMap.put(newDocId, Pair.of(docIter.docID(), oldKey));
                    docFreqs.add(new DocFreq(newDocId, docIter.freq()));
                }
            }
        }
        return docFreqs;
    }
}
