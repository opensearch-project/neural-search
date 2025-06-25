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
import org.opensearch.neuralsearch.sparse.algorithm.ByteQuantizer;
import org.opensearch.neuralsearch.sparse.algorithm.ClusterTrainingRunning;
import org.opensearch.neuralsearch.sparse.algorithm.PostingClusters;
import org.opensearch.neuralsearch.sparse.common.DocFreq;
import org.opensearch.neuralsearch.sparse.common.InMemoryKey;
import org.opensearch.neuralsearch.sparse.common.PredicateUtils;
import org.opensearch.neuralsearch.sparse.common.ValueEncoder;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;

import static org.opensearch.neuralsearch.sparse.common.SparseConstants.ALGO_TRIGGER_DOC_COUNT_FIELD;
import static org.opensearch.neuralsearch.sparse.common.SparseConstants.SUMMARY_PRUNE_RATIO_FIELD;
import static org.opensearch.neuralsearch.sparse.common.SparseConstants.CLUSTER_RATIO_FIELD;
import static org.opensearch.neuralsearch.sparse.common.SparseConstants.N_POSTINGS_FIELD;

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
            if (SparseTokensField.isSparseField(fieldInfo)
                && PredicateUtils.shouldRunSeisPredicate.test(mergeState.segmentInfo, fieldInfo)) {
                sparseFieldInfos.add(fieldInfo);
            }
        }

        sparseTermsLuceneWriter.writeFieldCount(sparseFieldInfos.size());
        for (FieldInfo fieldInfo : sparseFieldInfos) {
            log.debug("Merge field: {}", fieldInfo.name);
            sparseTermsLuceneWriter.writeFieldNumber(fieldInfo.number);

            InMemoryKey.IndexKey key = new InMemoryKey.IndexKey(mergeState.segmentInfo, fieldInfo);
            float clusterRatio = Float.parseFloat(fieldInfo.attributes().get(CLUSTER_RATIO_FIELD));
            int nPostings = Integer.parseInt(fieldInfo.attributes().get(N_POSTINGS_FIELD));
            float summaryPruneRatio = Float.parseFloat(fieldInfo.attributes().get(SUMMARY_PRUNE_RATIO_FIELD));
            int clusterUtilDocCountReach = Integer.parseInt(fieldInfo.attributes().get(ALGO_TRIGGER_DOC_COUNT_FIELD));

            if (clusterUtilDocCountReach > 0 && docCount < clusterUtilDocCountReach) {
                clusterRatio = 0;
            }

            // get all terms of old segments from InMemoryClusteredPosting
            Set<BytesRef> allTerms = getAllTerms(fieldInfo);
            sparseTermsLuceneWriter.writeTermsSize(allTerms.size());
            clusteredPostingTermsWriter.setFieldAndMaxDoc(fieldInfo, docCount);

            List<CompletableFuture<List<Pair<BytesRef, PostingClusters>>>> futures = new ArrayList<>(
                Math.round((float) allTerms.size() / BATCH_SIZE)
            );
            int index = 0;
            List<BytesRef> termBatch = new ArrayList<>(BATCH_SIZE);
            for (BytesRef term : allTerms) {
                termBatch.add(term);
                if (termBatch.size() == BATCH_SIZE || index == allTerms.size() - 1) {
                    if (clusterRatio == 0) {
                        futures.add(
                            CompletableFuture.completedFuture(
                                new BatchClusteringTask(termBatch, key, summaryPruneRatio, clusterRatio, nPostings, mergeState, fieldInfo)
                                    .get()
                            )
                        );
                    } else {
                        futures.add(
                            CompletableFuture.supplyAsync(
                                new BatchClusteringTask(termBatch, key, summaryPruneRatio, clusterRatio, nPostings, mergeState, fieldInfo),
                                ClusterTrainingRunning.getInstance().getExecutor()
                            )
                        );
                    }
                    termBatch = new ArrayList<>(BATCH_SIZE);
                }
                ++index;
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
                // fieldsProducer could be a delegate one as we need to merge normal segments into seis segment
                TermsEnum termsEnum = terms.iterator();
                while (true) {
                    BytesRef term = termsEnum.next();
                    if (term == null) {
                        break;
                    }
                    allTerms.add(BytesRef.deepCopyOf(term));
                }
            }
        }
        return allTerms;
    }

    public static List<DocFreq> getMergedPostingForATerm(
        MergeState mergeState,
        BytesRef term,
        FieldInfo fieldInfo,
        int[] newIdToFieldProducerIndex,
        int[] newIdToOldId
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
            SparseBinaryDocValuesPassThrough sparseBinaryDocValues = (SparseBinaryDocValuesPassThrough) binaryDocValues;
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
            boolean isSparsePostings = postings instanceof SparsePostingsEnum;
            int docId = postings.nextDoc();
            while (docId != PostingsEnum.NO_MORE_DOCS) {
                if (docId == -1) {
                    log.error("docId is -1");
                    continue;
                }
                int newDocId = mergeState.docMaps[i].get(docId);
                if (newDocId == -1) {
                    continue;
                }
                newIdToFieldProducerIndex[newDocId] = i;
                newIdToOldId[newDocId] = docId;
                int freq = postings.freq();
                byte freqByte = 0;
                if (isSparsePostings) {
                    // SparsePostingsEnum.freq() already transform byte freq to int
                    freqByte = (byte) freq;
                } else {
                    // decode to float first
                    freqByte = ByteQuantizer.quantizeFloatToByte(ValueEncoder.decodeFeatureValue(freq));
                }
                docFreqs.add(new DocFreq(newDocId, freqByte));
                docId = postings.nextDoc();
            }
        }
        return docFreqs;
    }
}
