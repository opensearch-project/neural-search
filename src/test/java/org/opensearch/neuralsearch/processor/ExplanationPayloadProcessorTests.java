/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.processor;

import lombok.SneakyThrows;
import org.apache.commons.lang3.Range;
import org.apache.lucene.search.Explanation;
import org.apache.lucene.search.SortField;
import org.apache.lucene.search.TotalHits;
import org.opensearch.action.OriginalIndices;
import org.opensearch.action.search.SearchRequest;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.action.search.ShardSearchFailure;
import org.opensearch.core.index.Index;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.neuralsearch.processor.explain.CombinedExplainDetails;
import org.opensearch.neuralsearch.processor.explain.ExplanationDetails;
import org.opensearch.neuralsearch.processor.explain.ExplanationPayload;
import org.opensearch.search.DocValueFormat;
import org.opensearch.search.SearchHit;
import org.opensearch.search.SearchHits;
import org.opensearch.search.SearchShardTarget;
import org.opensearch.search.aggregations.InternalAggregations;
import org.opensearch.search.internal.InternalSearchResponse;
import org.opensearch.search.pipeline.PipelineProcessingContext;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.transport.RemoteClusterAware;

import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.TreeMap;

import static org.mockito.Mockito.mock;
import static org.opensearch.neuralsearch.util.TestUtils.DELTA_FOR_SCORE_ASSERTION;

public class ExplanationPayloadProcessorTests extends OpenSearchTestCase {
    private static final String PROCESSOR_TAG = "mockTag";
    private static final String DESCRIPTION = "mockDescription";

    public void testClassFields_whenCreateNewObject_thenAllFieldsPresent() {
        ExplanationResponseProcessor explanationResponseProcessor = new ExplanationResponseProcessor(DESCRIPTION, PROCESSOR_TAG, false);

        assertEquals(DESCRIPTION, explanationResponseProcessor.getDescription());
        assertEquals(PROCESSOR_TAG, explanationResponseProcessor.getTag());
        assertFalse(explanationResponseProcessor.isIgnoreFailure());
    }

    @SneakyThrows
    public void testPipelineContext_whenPipelineContextHasNoExplanationInfo_thenProcessorIsNoOp() {
        ExplanationResponseProcessor explanationResponseProcessor = new ExplanationResponseProcessor(DESCRIPTION, PROCESSOR_TAG, false);
        SearchRequest searchRequest = mock(SearchRequest.class);
        SearchResponse searchResponse = new SearchResponse(
            null,
            null,
            1,
            1,
            0,
            1000,
            new ShardSearchFailure[0],
            SearchResponse.Clusters.EMPTY
        );

        SearchResponse processedResponse = explanationResponseProcessor.processResponse(searchRequest, searchResponse);
        assertEquals(searchResponse, processedResponse);

        SearchResponse processedResponse2 = explanationResponseProcessor.processResponse(searchRequest, searchResponse, null);
        assertEquals(searchResponse, processedResponse2);

        PipelineProcessingContext pipelineProcessingContext = new PipelineProcessingContext();
        SearchResponse processedResponse3 = explanationResponseProcessor.processResponse(
            searchRequest,
            searchResponse,
            pipelineProcessingContext
        );
        assertEquals(searchResponse, processedResponse3);
    }

    @SneakyThrows
    public void testParsingOfExplanations_whenResponseHasExplanations_thenSuccessful() {
        // Setup
        ExplanationResponseProcessor explanationResponseProcessor = new ExplanationResponseProcessor(DESCRIPTION, PROCESSOR_TAG, false);
        SearchRequest searchRequest = mock(SearchRequest.class);

        int numResponses = 1;
        int numIndices = 2;
        Iterator<Map.Entry<String, Index[]>> indicesIterator = randomRealisticIndices(numIndices, numResponses).entrySet().iterator();
        Map.Entry<String, Index[]> entry = indicesIterator.next();
        String clusterAlias = entry.getKey();
        Index[] indices = entry.getValue();

        int requestedSize = 2;
        PriorityQueue<SearchHit> priorityQueue = new PriorityQueue<>(new SearchHitComparator(null));
        TotalHits.Relation totalHitsRelation = randomFrom(TotalHits.Relation.values());
        TotalHits totalHits = new TotalHits(randomLongBetween(0, 1000), totalHitsRelation);

        final int numDocs = totalHits.value >= requestedSize ? requestedSize : (int) totalHits.value;
        int scoreFactor = randomIntBetween(1, numResponses);
        float maxScore = numDocs * scoreFactor;

        SearchHit[] searchHitArray = randomSearchHitArray(
            numDocs,
            numResponses,
            clusterAlias,
            indices,
            maxScore,
            scoreFactor,
            null,
            priorityQueue
        );
        for (SearchHit searchHit : searchHitArray) {
            Explanation explanation = Explanation.match(1.0f, "base scores from subqueries:", Explanation.match(1.0f, "field1:[0 TO 100]"));
            searchHit.explanation(explanation);
        }

        SearchHits searchHits = new SearchHits(searchHitArray, new TotalHits(numResponses, TotalHits.Relation.EQUAL_TO), 1.0f);

        SearchResponse searchResponse = getSearchResponse(searchHits);

        PipelineProcessingContext pipelineProcessingContext = new PipelineProcessingContext();
        Explanation generalExplanation = Explanation.match(
            maxScore,
            "combined score with techniques: normalization [l2], combination [arithmetic_mean] with optional parameters [[]]"
        );
        Map<SearchShard, List<CombinedExplainDetails>> combinedExplainDetails = Map.of(
            SearchShard.createSearchShard(searchHitArray[0].getShard()),
            List.of(
                CombinedExplainDetails.builder()
                    .normalizationExplain(new ExplanationDetails(1.0f, "source scores: [1.0] normalized to scores: [0.5]"))
                    .combinationExplain(new ExplanationDetails(0.5f, "normalized scores: [0.5] combined to a final score: 0.5"))
                    .build()
            ),
            SearchShard.createSearchShard(searchHitArray[1].getShard()),
            List.of(
                CombinedExplainDetails.builder()
                    .normalizationExplain(new ExplanationDetails(0.5f, "source scores: [0.5] normalized to scores: [0.25]"))
                    .combinationExplain(new ExplanationDetails(0.25f, "normalized scores: [0.25] combined to a final score: 0.25"))
                    .build()
            )
        );
        Map<ExplanationPayload.PayloadType, Object> explainPayload = Map.of(
            ExplanationPayload.PayloadType.NORMALIZATION_PROCESSOR,
            combinedExplainDetails
        );
        ExplanationPayload explanationPayload = ExplanationPayload.builder()
            .explanation(generalExplanation)
            .explainPayload(explainPayload)
            .build();
        pipelineProcessingContext.setAttribute(org.opensearch.neuralsearch.plugin.NeuralSearch.EXPLAIN_RESPONSE_KEY, explanationPayload);

        // Act
        SearchResponse processedResponse = explanationResponseProcessor.processResponse(
            searchRequest,
            searchResponse,
            pipelineProcessingContext
        );

        // Assert
        assertOnExplanationResults(processedResponse, maxScore);
    }

    @SneakyThrows
    public void testParsingOfExplanations_whenFieldSortingAndExplanations_thenSuccessful() {
        // Setup
        ExplanationResponseProcessor explanationResponseProcessor = new ExplanationResponseProcessor(DESCRIPTION, PROCESSOR_TAG, false);
        SearchRequest searchRequest = mock(SearchRequest.class);

        int numResponses = 1;
        int numIndices = 2;
        Iterator<Map.Entry<String, Index[]>> indicesIterator = randomRealisticIndices(numIndices, numResponses).entrySet().iterator();
        Map.Entry<String, Index[]> entry = indicesIterator.next();
        String clusterAlias = entry.getKey();
        Index[] indices = entry.getValue();
        final SortField[] sortFields = new SortField[] {
            new SortField("random-text-field-1", SortField.Type.INT, randomBoolean()),
            new SortField("random-text-field-2", SortField.Type.STRING, randomBoolean()) };

        int requestedSize = 2;
        PriorityQueue<SearchHit> priorityQueue = new PriorityQueue<>(new SearchHitComparator(sortFields));
        TotalHits.Relation totalHitsRelation = randomFrom(TotalHits.Relation.values());
        TotalHits totalHits = new TotalHits(randomLongBetween(0, 1000), totalHitsRelation);

        final int numDocs = totalHits.value >= requestedSize ? requestedSize : (int) totalHits.value;
        int scoreFactor = randomIntBetween(1, numResponses);
        float maxScore = Float.NaN;

        SearchHit[] searchHitArray = randomSearchHitArray(
            numDocs,
            numResponses,
            clusterAlias,
            indices,
            maxScore,
            scoreFactor,
            sortFields,
            priorityQueue
        );
        for (SearchHit searchHit : searchHitArray) {
            Explanation explanation = Explanation.match(1.0f, "base scores from subqueries:", Explanation.match(1.0f, "field1:[0 TO 100]"));
            searchHit.explanation(explanation);
        }

        SearchHits searchHits = new SearchHits(searchHitArray, totalHits, maxScore, sortFields, null, null);

        SearchResponse searchResponse = getSearchResponse(searchHits);

        PipelineProcessingContext pipelineProcessingContext = new PipelineProcessingContext();
        Explanation generalExplanation = Explanation.match(
            maxScore,
            "combined score with techniques: normalization [l2], combination [arithmetic_mean] with optional parameters [[]]"
        );
        Map<SearchShard, List<CombinedExplainDetails>> combinedExplainDetails = Map.of(
            SearchShard.createSearchShard(searchHitArray[0].getShard()),
            List.of(
                CombinedExplainDetails.builder()
                    .normalizationExplain(new ExplanationDetails(1.0f, "source scores: [1.0] normalized to scores: [0.5]"))
                    .combinationExplain(new ExplanationDetails(0.5f, "normalized scores: [0.5] combined to a final score: 0.5"))
                    .build()
            ),
            SearchShard.createSearchShard(searchHitArray[1].getShard()),
            List.of(
                CombinedExplainDetails.builder()
                    .normalizationExplain(new ExplanationDetails(0.5f, "source scores: [0.5] normalized to scores: [0.25]"))
                    .combinationExplain(new ExplanationDetails(0.25f, "normalized scores: [0.25] combined to a final score: 0.25"))
                    .build()
            )
        );
        Map<ExplanationPayload.PayloadType, Object> explainPayload = Map.of(
            ExplanationPayload.PayloadType.NORMALIZATION_PROCESSOR,
            combinedExplainDetails
        );
        ExplanationPayload explanationPayload = ExplanationPayload.builder()
            .explanation(generalExplanation)
            .explainPayload(explainPayload)
            .build();
        pipelineProcessingContext.setAttribute(org.opensearch.neuralsearch.plugin.NeuralSearch.EXPLAIN_RESPONSE_KEY, explanationPayload);

        // Act
        SearchResponse processedResponse = explanationResponseProcessor.processResponse(
            searchRequest,
            searchResponse,
            pipelineProcessingContext
        );

        // Assert
        assertOnExplanationResults(processedResponse, maxScore);
    }

    @SneakyThrows
    public void testParsingOfExplanations_whenScoreSortingAndExplanations_thenSuccessful() {
        // Setup
        ExplanationResponseProcessor explanationResponseProcessor = new ExplanationResponseProcessor(DESCRIPTION, PROCESSOR_TAG, false);
        SearchRequest searchRequest = mock(SearchRequest.class);

        int numResponses = 1;
        int numIndices = 2;
        Iterator<Map.Entry<String, Index[]>> indicesIterator = randomRealisticIndices(numIndices, numResponses).entrySet().iterator();
        Map.Entry<String, Index[]> entry = indicesIterator.next();
        String clusterAlias = entry.getKey();
        Index[] indices = entry.getValue();
        final SortField[] sortFields = new SortField[] { SortField.FIELD_SCORE };

        int requestedSize = 2;
        PriorityQueue<SearchHit> priorityQueue = new PriorityQueue<>(new SearchHitComparator(sortFields));
        TotalHits.Relation totalHitsRelation = randomFrom(TotalHits.Relation.values());
        TotalHits totalHits = new TotalHits(randomLongBetween(0, 1000), totalHitsRelation);

        final int numDocs = totalHits.value >= requestedSize ? requestedSize : (int) totalHits.value;
        int scoreFactor = randomIntBetween(1, numResponses);
        float maxScore = Float.NaN;

        SearchHit[] searchHitArray = randomSearchHitArray(
            numDocs,
            numResponses,
            clusterAlias,
            indices,
            maxScore,
            scoreFactor,
            sortFields,
            priorityQueue
        );
        for (SearchHit searchHit : searchHitArray) {
            Explanation explanation = Explanation.match(1.0f, "base scores from subqueries:", Explanation.match(1.0f, "field1:[0 TO 100]"));
            searchHit.explanation(explanation);
        }

        SearchHits searchHits = new SearchHits(searchHitArray, totalHits, maxScore, sortFields, null, null);

        SearchResponse searchResponse = getSearchResponse(searchHits);

        PipelineProcessingContext pipelineProcessingContext = new PipelineProcessingContext();
        Explanation generalExplanation = Explanation.match(
            maxScore,
            "combined score with techniques: normalization [l2], combination [arithmetic_mean] with optional parameters [[]]"
        );
        Map<SearchShard, List<CombinedExplainDetails>> combinedExplainDetails = Map.of(
            SearchShard.createSearchShard(searchHitArray[0].getShard()),
            List.of(
                CombinedExplainDetails.builder()
                    .normalizationExplain(new ExplanationDetails(1.0f, "source scores: [1.0] normalized to scores: [0.5]"))
                    .combinationExplain(new ExplanationDetails(0.5f, "normalized scores: [0.5] combined to a final score: 0.5"))
                    .build()
            ),
            SearchShard.createSearchShard(searchHitArray[1].getShard()),
            List.of(
                CombinedExplainDetails.builder()
                    .normalizationExplain(new ExplanationDetails(0.5f, "source scores: [0.5] normalized to scores: [0.25]"))
                    .combinationExplain(new ExplanationDetails(0.25f, "normalized scores: [0.25] combined to a final score: 0.25"))
                    .build()
            )
        );
        Map<ExplanationPayload.PayloadType, Object> explainPayload = Map.of(
            ExplanationPayload.PayloadType.NORMALIZATION_PROCESSOR,
            combinedExplainDetails
        );
        ExplanationPayload explanationPayload = ExplanationPayload.builder()
            .explanation(generalExplanation)
            .explainPayload(explainPayload)
            .build();
        pipelineProcessingContext.setAttribute(org.opensearch.neuralsearch.plugin.NeuralSearch.EXPLAIN_RESPONSE_KEY, explanationPayload);

        // Act
        SearchResponse processedResponse = explanationResponseProcessor.processResponse(
            searchRequest,
            searchResponse,
            pipelineProcessingContext
        );

        // Assert
        assertOnExplanationResults(processedResponse, maxScore);
    }

    private static SearchResponse getSearchResponse(SearchHits searchHits) {
        InternalSearchResponse internalSearchResponse = new InternalSearchResponse(
            searchHits,
            InternalAggregations.EMPTY,
            null,
            null,
            false,
            null,
            1
        );
        SearchResponse searchResponse = new SearchResponse(
            internalSearchResponse,
            null,
            1,
            1,
            0,
            1000,
            new ShardSearchFailure[0],
            SearchResponse.Clusters.EMPTY
        );
        return searchResponse;
    }

    private static void assertOnExplanationResults(SearchResponse processedResponse, float maxScore) {
        assertNotNull(processedResponse);
        Explanation explanationHit1 = processedResponse.getHits().getHits()[0].getExplanation();
        assertNotNull(explanationHit1);
        assertEquals(
            "combined score with techniques: normalization [l2], combination [arithmetic_mean] with optional parameters [[]]",
            explanationHit1.getDescription()
        );
        assertEquals(maxScore, (float) explanationHit1.getValue(), DELTA_FOR_SCORE_ASSERTION);

        Explanation[] detailsHit1 = explanationHit1.getDetails();
        assertEquals(3, detailsHit1.length);
        assertEquals("source scores: [1.0] normalized to scores: [0.5]", detailsHit1[0].getDescription());
        assertEquals(1.0f, (float) detailsHit1[0].getValue(), DELTA_FOR_SCORE_ASSERTION);

        assertEquals("normalized scores: [0.5] combined to a final score: 0.5", detailsHit1[1].getDescription());
        assertEquals(0.5f, (float) detailsHit1[1].getValue(), DELTA_FOR_SCORE_ASSERTION);

        assertEquals("base scores from subqueries:", detailsHit1[2].getDescription());
        assertEquals(1.0f, (float) detailsHit1[2].getValue(), DELTA_FOR_SCORE_ASSERTION);

        Explanation explanationHit2 = processedResponse.getHits().getHits()[1].getExplanation();
        assertNotNull(explanationHit2);
        assertEquals(
            "combined score with techniques: normalization [l2], combination [arithmetic_mean] with optional parameters [[]]",
            explanationHit2.getDescription()
        );
        assertTrue(Range.of(0.0f, maxScore).contains((float) explanationHit2.getValue()));

        Explanation[] detailsHit2 = explanationHit2.getDetails();
        assertEquals(3, detailsHit2.length);
        assertEquals("source scores: [0.5] normalized to scores: [0.25]", detailsHit2[0].getDescription());
        assertEquals(.5f, (float) detailsHit2[0].getValue(), DELTA_FOR_SCORE_ASSERTION);

        assertEquals("normalized scores: [0.25] combined to a final score: 0.25", detailsHit2[1].getDescription());
        assertEquals(.25f, (float) detailsHit2[1].getValue(), DELTA_FOR_SCORE_ASSERTION);

        assertEquals("base scores from subqueries:", detailsHit2[2].getDescription());
        assertEquals(1.0f, (float) detailsHit2[2].getValue(), DELTA_FOR_SCORE_ASSERTION);
    }

    private static Map<String, Index[]> randomRealisticIndices(int numIndices, int numClusters) {
        String[] indicesNames = new String[numIndices];
        for (int i = 0; i < numIndices; i++) {
            indicesNames[i] = randomAlphaOfLengthBetween(5, 10);
        }
        Map<String, Index[]> indicesPerCluster = new TreeMap<>();
        for (int i = 0; i < numClusters; i++) {
            Index[] indices = new Index[indicesNames.length];
            for (int j = 0; j < indices.length; j++) {
                String indexName = indicesNames[j];
                String indexUuid = frequently() ? randomAlphaOfLength(10) : indexName;
                indices[j] = new Index(indexName, indexUuid);
            }
            String clusterAlias;
            if (frequently() || indicesPerCluster.containsKey(RemoteClusterAware.LOCAL_CLUSTER_GROUP_KEY)) {
                clusterAlias = randomAlphaOfLengthBetween(5, 10);
            } else {
                clusterAlias = RemoteClusterAware.LOCAL_CLUSTER_GROUP_KEY;
            }
            indicesPerCluster.put(clusterAlias, indices);
        }
        return indicesPerCluster;
    }

    private static SearchHit[] randomSearchHitArray(
        int numDocs,
        int numResponses,
        String clusterAlias,
        Index[] indices,
        float maxScore,
        int scoreFactor,
        SortField[] sortFields,
        PriorityQueue<SearchHit> priorityQueue
    ) {
        SearchHit[] hits = new SearchHit[numDocs];

        int[] sortFieldFactors = new int[sortFields == null ? 0 : sortFields.length];
        for (int j = 0; j < sortFieldFactors.length; j++) {
            sortFieldFactors[j] = randomIntBetween(1, numResponses);
        }

        for (int j = 0; j < numDocs; j++) {
            ShardId shardId = new ShardId(randomFrom(indices), randomIntBetween(0, 10));
            SearchShardTarget shardTarget = new SearchShardTarget(
                randomAlphaOfLengthBetween(3, 8),
                shardId,
                clusterAlias,
                OriginalIndices.NONE
            );
            SearchHit hit = new SearchHit(randomIntBetween(0, Integer.MAX_VALUE));

            float score = Float.NaN;
            if (!Float.isNaN(maxScore)) {
                score = (maxScore - j) * scoreFactor;
                hit.score(score);
            }

            hit.shard(shardTarget);
            if (sortFields != null) {
                Object[] rawSortValues = new Object[sortFields.length];
                DocValueFormat[] docValueFormats = new DocValueFormat[sortFields.length];
                for (int k = 0; k < sortFields.length; k++) {
                    SortField sortField = sortFields[k];
                    if (sortField == SortField.FIELD_SCORE) {
                        hit.score(score);
                        rawSortValues[k] = score;
                    } else {
                        rawSortValues[k] = sortField.getReverse() ? numDocs * sortFieldFactors[k] - j : j;
                    }
                    docValueFormats[k] = DocValueFormat.RAW;
                }
                hit.sortValues(rawSortValues, docValueFormats);
            }
            hits[j] = hit;
            priorityQueue.add(hit);
        }
        return hits;
    }

    private static final class SearchHitComparator implements Comparator<SearchHit> {

        private final SortField[] sortFields;

        SearchHitComparator(SortField[] sortFields) {
            this.sortFields = sortFields;
        }

        @Override
        public int compare(SearchHit a, SearchHit b) {
            if (sortFields == null) {
                int scoreCompare = Float.compare(b.getScore(), a.getScore());
                if (scoreCompare != 0) {
                    return scoreCompare;
                }
            } else {
                for (int i = 0; i < sortFields.length; i++) {
                    SortField sortField = sortFields[i];
                    if (sortField == SortField.FIELD_SCORE) {
                        int scoreCompare = Float.compare(b.getScore(), a.getScore());
                        if (scoreCompare != 0) {
                            return scoreCompare;
                        }
                    } else {
                        Integer aSortValue = (Integer) a.getRawSortValues()[i];
                        Integer bSortValue = (Integer) b.getRawSortValues()[i];
                        final int compare;
                        if (sortField.getReverse()) {
                            compare = Integer.compare(bSortValue, aSortValue);
                        } else {
                            compare = Integer.compare(aSortValue, bSortValue);
                        }
                        if (compare != 0) {
                            return compare;
                        }
                    }
                }
            }
            SearchShardTarget aShard = a.getShard();
            SearchShardTarget bShard = b.getShard();
            int shardIdCompareTo = aShard.getShardId().compareTo(bShard.getShardId());
            if (shardIdCompareTo != 0) {
                return shardIdCompareTo;
            }
            int clusterAliasCompareTo = aShard.getClusterAlias().compareTo(bShard.getClusterAlias());
            if (clusterAliasCompareTo != 0) {
                return clusterAliasCompareTo;
            }
            return Integer.compare(a.docId(), b.docId());
        }
    }
}
