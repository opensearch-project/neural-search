/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.processor;

import lombok.SneakyThrows;
import org.apache.commons.lang3.Range;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.lucene.search.Explanation;
import org.apache.lucene.search.SortField;
import org.apache.lucene.search.TotalHits;
import org.opensearch.action.OriginalIndices;
import org.opensearch.action.search.SearchRequest;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.action.search.ShardSearchFailure;
import org.opensearch.core.index.Index;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.neuralsearch.processor.explain.CombinedExplanationDetails;
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
        float maxScore = 1.0f;
        SearchHits searchHits = getSearchHits(maxScore);
        SearchResponse searchResponse = getSearchResponse(searchHits);
        PipelineProcessingContext pipelineProcessingContext = new PipelineProcessingContext();
        Map<SearchShard, List<CombinedExplanationDetails>> combinedExplainDetails = getCombinedExplainDetails(searchHits);
        Map<ExplanationPayload.PayloadType, Object> explainPayload = Map.of(
            ExplanationPayload.PayloadType.NORMALIZATION_PROCESSOR,
            combinedExplainDetails
        );
        ExplanationPayload explanationPayload = ExplanationPayload.builder().explainPayload(explainPayload).build();
        pipelineProcessingContext.setAttribute(
            org.opensearch.neuralsearch.plugin.NeuralSearch.EXPLANATION_RESPONSE_KEY,
            explanationPayload
        );

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

        float maxScore = 1.0f;
        SearchHits searchHitsWithoutSorting = getSearchHits(maxScore);
        for (SearchHit searchHit : searchHitsWithoutSorting.getHits()) {
            Explanation explanation = Explanation.match(1.0f, "combined score of:", Explanation.match(1.0f, "field1:[0 TO 100]"));
            searchHit.explanation(explanation);
        }
        TotalHits.Relation totalHitsRelation = randomFrom(TotalHits.Relation.values());
        TotalHits totalHits = new TotalHits(randomLongBetween(0, 1000), totalHitsRelation);
        final SortField[] sortFields = new SortField[] {
            new SortField("random-text-field-1", SortField.Type.INT, randomBoolean()),
            new SortField("random-text-field-2", SortField.Type.STRING, randomBoolean()) };
        SearchHits searchHits = new SearchHits(searchHitsWithoutSorting.getHits(), totalHits, maxScore, sortFields, null, null);

        SearchResponse searchResponse = getSearchResponse(searchHits);

        PipelineProcessingContext pipelineProcessingContext = new PipelineProcessingContext();
        Map<SearchShard, List<CombinedExplanationDetails>> combinedExplainDetails = getCombinedExplainDetails(searchHits);
        Map<ExplanationPayload.PayloadType, Object> explainPayload = Map.of(
            ExplanationPayload.PayloadType.NORMALIZATION_PROCESSOR,
            combinedExplainDetails
        );
        ExplanationPayload explanationPayload = ExplanationPayload.builder().explainPayload(explainPayload).build();
        pipelineProcessingContext.setAttribute(
            org.opensearch.neuralsearch.plugin.NeuralSearch.EXPLANATION_RESPONSE_KEY,
            explanationPayload
        );

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

        float maxScore = 1.0f;

        SearchHits searchHits = getSearchHits(maxScore);

        SearchResponse searchResponse = getSearchResponse(searchHits);

        PipelineProcessingContext pipelineProcessingContext = new PipelineProcessingContext();

        Map<SearchShard, List<CombinedExplanationDetails>> combinedExplainDetails = getCombinedExplainDetails(searchHits);
        Map<ExplanationPayload.PayloadType, Object> explainPayload = Map.of(
            ExplanationPayload.PayloadType.NORMALIZATION_PROCESSOR,
            combinedExplainDetails
        );
        ExplanationPayload explanationPayload = ExplanationPayload.builder().explainPayload(explainPayload).build();
        pipelineProcessingContext.setAttribute(
            org.opensearch.neuralsearch.plugin.NeuralSearch.EXPLANATION_RESPONSE_KEY,
            explanationPayload
        );

        // Act
        SearchResponse processedResponse = explanationResponseProcessor.processResponse(
            searchRequest,
            searchResponse,
            pipelineProcessingContext
        );

        // Assert
        assertOnExplanationResults(processedResponse, maxScore);
    }

    private static SearchHits getSearchHits(float maxScore) {
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
            Explanation explanation = Explanation.match(1.0f, "combined score of:", Explanation.match(1.0f, "field1:[0 TO 100]"));
            searchHit.explanation(explanation);
        }

        SearchHits searchHits = new SearchHits(searchHitArray, new TotalHits(numResponses, TotalHits.Relation.EQUAL_TO), maxScore);
        return searchHits;
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

    private static Map<SearchShard, List<CombinedExplanationDetails>> getCombinedExplainDetails(SearchHits searchHits) {
        Map<SearchShard, List<CombinedExplanationDetails>> combinedExplainDetails = Map.of(
            SearchShard.createSearchShard(searchHits.getHits()[0].getShard()),
            List.of(
                CombinedExplanationDetails.builder()
                    .normalizationExplanations(new ExplanationDetails(List.of(Pair.of(1.0f, "min_max normalization of:"))))
                    .combinationExplanations(new ExplanationDetails(List.of(Pair.of(0.5f, "arithmetic_mean combination of:"))))
                    .build()
            ),
            SearchShard.createSearchShard(searchHits.getHits()[1].getShard()),
            List.of(
                CombinedExplanationDetails.builder()
                    .normalizationExplanations(new ExplanationDetails(List.of(Pair.of(0.5f, "min_max normalization of:"))))
                    .combinationExplanations(new ExplanationDetails(List.of(Pair.of(0.25f, "arithmetic_mean combination of:"))))
                    .build()
            )
        );
        return combinedExplainDetails;
    }

    private static void assertOnExplanationResults(SearchResponse processedResponse, float maxScore) {
        assertNotNull(processedResponse);
        Explanation hit1TopLevelExplanation = processedResponse.getHits().getHits()[0].getExplanation();
        assertNotNull(hit1TopLevelExplanation);
        assertEquals("arithmetic_mean combination of:", hit1TopLevelExplanation.getDescription());
        assertEquals(maxScore, (float) hit1TopLevelExplanation.getValue(), DELTA_FOR_SCORE_ASSERTION);

        Explanation[] hit1SecondLevelDetails = hit1TopLevelExplanation.getDetails();
        assertEquals(1, hit1SecondLevelDetails.length);
        assertEquals("min_max normalization of:", hit1SecondLevelDetails[0].getDescription());
        assertEquals(1.0f, (float) hit1SecondLevelDetails[0].getValue(), DELTA_FOR_SCORE_ASSERTION);

        assertNotNull(hit1SecondLevelDetails[0].getDetails());
        assertEquals(1, hit1SecondLevelDetails[0].getDetails().length);
        Explanation hit1ShardLevelExplanation = hit1SecondLevelDetails[0].getDetails()[0];

        assertEquals(1.0f, (float) hit1ShardLevelExplanation.getValue(), DELTA_FOR_SCORE_ASSERTION);
        assertEquals("field1:[0 TO 100]", hit1ShardLevelExplanation.getDescription());

        Explanation hit2TopLevelExplanation = processedResponse.getHits().getHits()[1].getExplanation();
        assertNotNull(hit2TopLevelExplanation);
        assertEquals("arithmetic_mean combination of:", hit2TopLevelExplanation.getDescription());
        assertEquals(0.0f, (float) hit2TopLevelExplanation.getValue(), DELTA_FOR_SCORE_ASSERTION);

        Explanation[] hit2SecondLevelDetails = hit2TopLevelExplanation.getDetails();
        assertEquals(1, hit2SecondLevelDetails.length);
        assertEquals("min_max normalization of:", hit2SecondLevelDetails[0].getDescription());
        assertEquals(.5f, (float) hit2SecondLevelDetails[0].getValue(), DELTA_FOR_SCORE_ASSERTION);

        assertNotNull(hit2SecondLevelDetails[0].getDetails());
        assertEquals(1, hit2SecondLevelDetails[0].getDetails().length);
        Explanation hit2ShardLevelExplanation = hit2SecondLevelDetails[0].getDetails()[0];

        assertEquals(1.0f, (float) hit2ShardLevelExplanation.getValue(), DELTA_FOR_SCORE_ASSERTION);
        assertEquals("field1:[0 TO 100]", hit2ShardLevelExplanation.getDescription());

        Explanation explanationHit2 = processedResponse.getHits().getHits()[1].getExplanation();
        assertNotNull(explanationHit2);
        assertEquals("arithmetic_mean combination of:", explanationHit2.getDescription());
        assertTrue(Range.of(0.0f, maxScore).contains((float) explanationHit2.getValue()));

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
