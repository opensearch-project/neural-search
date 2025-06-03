/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.processor;

import org.apache.lucene.index.LeafReaderContext;
import org.opensearch.common.document.DocumentField;
import org.opensearch.neuralsearch.processor.normalization.ScoreNormalizer;
import org.opensearch.search.fetch.FetchContext;
import org.opensearch.search.fetch.FetchSubPhase;
import org.opensearch.search.fetch.FetchSubPhaseProcessor;
import org.opensearch.search.internal.SearchContext;

import java.io.IOException;
import java.util.List;
import java.util.Map;

public class HybridizationFetchSubPhase implements FetchSubPhase {

    public HybridizationFetchSubPhase() {}

    @Override
    public FetchSubPhaseProcessor getProcessor(FetchContext fetchContext) throws IOException {
        SearchContext context = ScoreNormalizer.getSearchContext();

        return new FetchSubPhaseProcessor() {
            LeafReaderContext ctx;

            @Override
            public void setNextReader(LeafReaderContext leafReaderContext) throws IOException {
                this.ctx = leafReaderContext;
            }

            @Override
            public void process(HitContext hitContext) {
                Map<Integer, float[]> scoreMap = HybridScoreRegistry.get(context);
                if (scoreMap == null) {
                    return;
                }
                int docId = hitContext.docId();
                float[] subqueryScores = scoreMap.get(docId);

                if (subqueryScores != null) {
                    // Add it as a field rather than modifying _source
                    hitContext.hit().setDocumentField("_hybridization", new DocumentField("_hybridization", List.of(subqueryScores)));
                }
            }
        };
    }
}
