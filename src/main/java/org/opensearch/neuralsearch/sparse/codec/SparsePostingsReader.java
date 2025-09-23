/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.sparse.codec;

import lombok.AllArgsConstructor;
import lombok.extern.log4j.Log4j2;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.lucene.codecs.BlockTermState;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.util.BytesRef;
import org.opensearch.neuralsearch.sparse.algorithm.ClusterTrainingExecutor;
import org.opensearch.neuralsearch.sparse.algorithm.seismic.BatchClusteringTask;
import org.opensearch.neuralsearch.sparse.cache.CacheKey;
import org.opensearch.neuralsearch.sparse.common.MergeStateFacade;
import org.opensearch.neuralsearch.sparse.common.PredicateUtils;
import org.opensearch.neuralsearch.sparse.data.PostingClusters;
import org.opensearch.neuralsearch.sparse.mapper.SparseVectorField;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;

import static org.opensearch.neuralsearch.sparse.common.SparseConstants.CLUSTER_RATIO_FIELD;
import static org.opensearch.neuralsearch.sparse.common.SparseConstants.N_POSTINGS_FIELD;
import static org.opensearch.neuralsearch.sparse.common.SparseConstants.SUMMARY_PRUNE_RATIO_FIELD;
import static org.opensearch.neuralsearch.sparse.common.SparseConstants.Seismic.DEFAULT_N_POSTINGS;
import static org.opensearch.neuralsearch.sparse.common.SparseConstants.Seismic.DEFAULT_POSTING_MINIMUM_LENGTH;
import static org.opensearch.neuralsearch.sparse.common.SparseConstants.Seismic.DEFAULT_POSTING_PRUNE_RATIO;

/**
 * Merge sparse postings
 */
@Log4j2
@AllArgsConstructor
public class SparsePostingsReader {
    private final MergeStateFacade mergeStateFacade;
    private final MergeHelper mergeHelper;
    // this is a magic number for now which is effective
    private static final int BATCH_SIZE = 50;

    public void merge(SparseTermsLuceneWriter sparseTermsLuceneWriter, ClusteredPostingTermsWriter clusteredPostingTermsWriter)
        throws Exception {
        int docCount = 0;
        for (int n : mergeStateFacade.getMaxDocs()) {
            docCount += n;
        }
        log.debug("Merge total doc: {}", docCount);
        List<FieldInfo> sparseFieldInfos = new ArrayList<>();
        for (FieldInfo fieldInfo : mergeStateFacade.getMergeFieldInfos()) {
            if (SparseVectorField.isSparseField(fieldInfo)
                && PredicateUtils.shouldRunSeisPredicate.test(mergeStateFacade.getSegmentInfo(), fieldInfo)) {
                sparseFieldInfos.add(fieldInfo);
            }
        }

        try {
            sparseTermsLuceneWriter.writeFieldCount(sparseFieldInfos.size());
            for (FieldInfo fieldInfo : sparseFieldInfos) {
                log.debug("Merge field: {}", fieldInfo.name);
                sparseTermsLuceneWriter.writeFieldNumber(fieldInfo.getFieldNumber());

                CacheKey key = new CacheKey(mergeStateFacade.getSegmentInfo(), fieldInfo);
                float clusterRatio = Float.parseFloat(fieldInfo.attributes().get(CLUSTER_RATIO_FIELD));
                int nPostings;
                if (Integer.parseInt(fieldInfo.attributes().get(N_POSTINGS_FIELD)) == DEFAULT_N_POSTINGS) {
                    nPostings = Math.max((int) (DEFAULT_POSTING_PRUNE_RATIO * docCount), DEFAULT_POSTING_MINIMUM_LENGTH);
                } else {
                    nPostings = Integer.parseInt(fieldInfo.attributes().get(N_POSTINGS_FIELD));
                }
                float summaryPruneRatio = Float.parseFloat(fieldInfo.attributes().get(SUMMARY_PRUNE_RATIO_FIELD));

                // get all terms of old segments from CacheClusteredPosting
                Set<BytesRef> allTerms = mergeHelper.getAllTerms(mergeStateFacade, fieldInfo);
                sparseTermsLuceneWriter.writeTermsSize(allTerms.size());
                clusteredPostingTermsWriter.setFieldAndMaxDoc(fieldInfo, docCount, true);

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
                                    new BatchClusteringTask(
                                        termBatch,
                                        key,
                                        summaryPruneRatio,
                                        clusterRatio,
                                        nPostings,
                                        mergeStateFacade,
                                        fieldInfo,
                                        mergeHelper
                                    ).get()
                                )
                            );
                        } else {
                            futures.add(
                                CompletableFuture.supplyAsync(
                                    new BatchClusteringTask(
                                        termBatch,
                                        key,
                                        summaryPruneRatio,
                                        clusterRatio,
                                        nPostings,
                                        mergeStateFacade,
                                        fieldInfo,
                                        mergeHelper
                                    ),
                                    ClusterTrainingExecutor.getInstance().getExecutor()
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
                    }
                }
            }
        } catch (IOException ex) {
            clusteredPostingTermsWriter.closeWithException();
            sparseTermsLuceneWriter.closeWithException();
            throw ex;
        }
    }
}
