/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.neuralsearch.processor;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import java.util.ArrayList;
import java.util.List;

import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.TotalHits;
import org.opensearch.action.OriginalIndices;
import org.opensearch.common.lucene.search.TopDocsAndMaxScore;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.neuralsearch.processor.combination.ScoreCombinationFactory;
import org.opensearch.neuralsearch.processor.combination.ScoreCombiner;
import org.opensearch.neuralsearch.processor.normalization.ScoreNormalizationTechnique;
import org.opensearch.neuralsearch.processor.normalization.ScoreNormalizer;
import org.opensearch.neuralsearch.search.CompoundTopDocs;
import org.opensearch.search.DocValueFormat;
import org.opensearch.search.SearchShardTarget;
import org.opensearch.search.query.QuerySearchResult;
import org.opensearch.test.OpenSearchTestCase;

public class NormalizationProcessorWorkflowTests extends OpenSearchTestCase {

    public void testSearchResultTypes_whenCompoundDocs_thenDoNormalizationCombination() {
        NormalizationProcessorWorkflow normalizationProcessorWorkflow = spy(
            new NormalizationProcessorWorkflow(new ScoreNormalizer(), new ScoreCombiner())
        );

        List<QuerySearchResult> querySearchResults = new ArrayList<>();
        for (int shardId = 0; shardId < 4; shardId++) {
            SearchShardTarget searchShardTarget = new SearchShardTarget(
                "node",
                new ShardId("index", "uuid", shardId),
                null,
                OriginalIndices.NONE
            );
            QuerySearchResult querySearchResult = new QuerySearchResult();
            CompoundTopDocs topDocs = new CompoundTopDocs(
                new TotalHits(4, TotalHits.Relation.EQUAL_TO),
                List.of(
                    new TopDocs(
                        new TotalHits(4, TotalHits.Relation.EQUAL_TO),
                        new ScoreDoc[] { new ScoreDoc(0, 0.5f), new ScoreDoc(2, 0.3f), new ScoreDoc(4, 0.25f), new ScoreDoc(10, 0.2f) }
                    )
                )
            );
            querySearchResult.topDocs(new TopDocsAndMaxScore(topDocs, 0.5f), new DocValueFormat[0]);
            querySearchResult.setSearchShardTarget(searchShardTarget);
            querySearchResult.setShardIndex(shardId);
            querySearchResults.add(querySearchResult);
        }

        normalizationProcessorWorkflow.execute(
            querySearchResults,
            ScoreNormalizationTechnique.DEFAULT,
            new ScoreCombinationFactory().defaultCombination()
        );

        verify(normalizationProcessorWorkflow, times(1)).updateOriginalQueryResults(any(), any(), any());
    }
}
