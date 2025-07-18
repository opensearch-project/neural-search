/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.processor;

import lombok.extern.log4j.Log4j2;
import org.apache.lucene.index.LeafReaderContext;
import org.opensearch.action.search.SearchPhaseContext;
import org.opensearch.common.document.DocumentField;
import org.opensearch.neuralsearch.processor.normalization.ScoreNormalizer;
import org.opensearch.search.fetch.FetchContext;
import org.opensearch.search.fetch.FetchSubPhase;
import org.opensearch.search.fetch.FetchSubPhaseProcessor;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.opensearch.neuralsearch.common.MinClusterVersionUtil.isClusterOnOrAfterMinReqVersionForSubQuerySupport;

/**
 * Fetch sub phase to add hybridization scores to the search response
 */
@Log4j2
public class HybridizationFetchSubPhase implements FetchSubPhase {

    private static final String SUB_QUERY_SCORES_NAME = "hybridization_sub_query_scores";

    @Override
    public FetchSubPhaseProcessor getProcessor(FetchContext fetchContext) throws IOException {
        // Check if inner hits are present
        boolean hasInnerHits = fetchContext.innerHits() != null && !fetchContext.innerHits().getInnerHits().isEmpty();
        SearchPhaseContext context = ScoreNormalizer.getSearchPhaseContext();

        return new FetchSubPhaseProcessor() {
            LeafReaderContext ctx;

            @Override
            public void setNextReader(LeafReaderContext leafReaderContext) throws IOException {
                this.ctx = leafReaderContext;
            }

            @Override
            public void process(HitContext hitContext) {
                boolean shouldAddHybridScores = hitContext.hit().getDocumentFields().containsKey(SUB_QUERY_SCORES_NAME) == false
                    && isClusterOnOrAfterMinReqVersionForSubQuerySupport()
                    && hasInnerHits == false;
                if (shouldAddHybridScores) {
                    Map<String, float[]> scoreMap = context == null ? null : HybridScoreRegistry.get(context);
                    if (scoreMap == null) {
                        log.debug("No sub query scores found");
                        return;
                    }
                    int docId = hitContext.docId();
                    int shardId = fetchContext.getQueryShardContext().getShardId();
                    String key = shardId + "_" + docId;
                    float[] subqueryScores = scoreMap.get(key);

                    if (subqueryScores != null) {
                        // Add it as a field rather than modifying _source
                        List<Object> hybridScores = new ArrayList<>(subqueryScores.length);
                        for (float score : subqueryScores) {
                            hybridScores.add(score);
                        }
                        hitContext.hit().setDocumentField(SUB_QUERY_SCORES_NAME, new DocumentField(SUB_QUERY_SCORES_NAME, hybridScores));
                    }
                }
            }
        };
    }
}
