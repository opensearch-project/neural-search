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
import org.opensearch.neuralsearch.sparse.algorithm.ClusterTrainingRunning;
import org.opensearch.neuralsearch.sparse.algorithm.ClusteringTask;
import org.opensearch.neuralsearch.sparse.algorithm.DocumentCluster;
import org.opensearch.neuralsearch.sparse.algorithm.PostingClusters;
import org.opensearch.neuralsearch.sparse.common.DocFreq;
import org.opensearch.neuralsearch.sparse.common.DocFreqIterator;
import org.opensearch.neuralsearch.sparse.common.InMemoryKey;
import org.opensearch.neuralsearch.sparse.common.IteratorWrapper;
import org.opensearch.neuralsearch.sparse.mapper.SparseMethodContext;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
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
            clusteredPostingTermsWriter.setField(fieldInfo);

            List<CompletableFuture<PostingClusters>> futures = new ArrayList<>(allTerms.size());
            for (BytesRef term : allTerms) {
                Map<Integer, Pair<Integer, InMemoryKey.IndexKey>> newToOldDocIdMap = new HashMap<>();
                List<DocFreq> docFreqs = getMergedPostingForATerm(term, fieldInfo, newToOldDocIdMap);
                if (beta == 1) {
                    // not run asynchronously
                    futures.add(
                        CompletableFuture.completedFuture(
                            new ClusteringTask(term, docFreqs, key, alpha, beta, lambda, newToOldDocIdMap).get()
                        )
                    );
                } else {
                    futures.add(
                        CompletableFuture.supplyAsync(
                            new ClusteringTask(term, docFreqs, key, alpha, beta, lambda, newToOldDocIdMap),
                            ClusterTrainingRunning.getInstance().getExecutor()
                        )
                    );
                }
            }
            int i = 0;
            for (BytesRef term : allTerms) {
                try {
                    PostingClusters cluster = futures.get(i).join();
                    BlockTermState state = clusteredPostingTermsWriter.write(term, cluster);
                    sparseTermsLuceneWriter.writeTerm(term, state);
                } catch (CancellationException | CompletionException ex) {
                    log.error("Thread of running clustering from term {} during merge has exception", term, ex);
                } catch (IOException ex) {
                    clusteredPostingTermsWriter.closeWithException();
                    sparseTermsLuceneWriter.closeWithException();
                }
                ++i;
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

    private List<DocFreq> getMergedPostingForATerm(
        BytesRef term,
        FieldInfo fieldInfo,
        Map<Integer, Pair<Integer, InMemoryKey.IndexKey>> newToOldDocIdMap
    ) throws IOException {
        List<DocFreq> docFreqs = new ArrayList<>();
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
                    int newDocId = this.mergeState.docMaps[i].get(docIter.docID());
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
