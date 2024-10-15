/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.processor.rerank;

import org.apache.lucene.search.TotalHits;
import org.junit.Before;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.opensearch.action.search.SearchRequest;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.action.search.SearchResponseSections;
import org.opensearch.action.search.ShardSearchFailure;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.common.bytes.BytesArray;
import org.opensearch.neuralsearch.processor.factory.RerankProcessorFactory;
import org.opensearch.search.SearchHit;
import org.opensearch.search.SearchHits;
import org.opensearch.search.pipeline.PipelineProcessingContext;
import org.opensearch.search.pipeline.Processor;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;
import java.util.AbstractMap;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.stream.IntStream;

import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

public class ByFieldRerankProcessorTests extends OpenSearchTestCase {
    private SearchRequest request;

    private SearchResponse response;

    @Mock
    private Processor.PipelineContext pipelineContext;

    @Mock
    private PipelineProcessingContext ppctx;

    @Mock
    private ClusterService clusterService;

    private RerankProcessorFactory factory;

    private ByFieldRerankProcessor processor;

    private final List<Map.Entry<Integer, Float>> sampleIndexMLScorePairs = List.of(
        Map.entry(1, 12.0f),
        Map.entry(2, 5.2f),
        Map.entry(3, 18.0f),
        Map.entry(4, 1.0f)
    );

    @Before
    public void setup() {
        MockitoAnnotations.openMocks(this);
        doReturn(Settings.EMPTY).when(clusterService).getSettings();
        factory = new RerankProcessorFactory(null, clusterService);
    }

    /**
     * Creates a searchResponse where the value to reRank by is Nested.
     * The location where the target is within a map of size 1 meaning after
     * Using ByFieldReRank the expected behavior is to delete the info mapping
     * as it is only has one mapping i.e. the duplicate value.
     * <hr>
     * The targetField for this scenario is <code>ml.info.score</code>
     */
    public void setUpValidSearchResultsWithNestedTargetValue() throws IOException {
        SearchHit[] hits = new SearchHit[sampleIndexMLScorePairs.size()];

        String templateString = """
            {
               "my_field" : "%s",
               "ml": {
                    "model" : "myModel",
                    "info"  : {
                              "score": %s
                    }
               }
            }
            """.replace("\n", "");

        for (int i = 0; i < sampleIndexMLScorePairs.size(); i++) {
            int docId = sampleIndexMLScorePairs.get(i).getKey();
            String mlScore = sampleIndexMLScorePairs.get(i).getValue() + "";

            String sourceMap = templateString.formatted(i, mlScore);

            hits[i] = new SearchHit(docId, docId + "", Collections.emptyMap(), Collections.emptyMap());
            hits[i].sourceRef(new BytesArray(sourceMap));
            hits[i].score(new Random().nextFloat());
        }

        TotalHits totalHits = new TotalHits(sampleIndexMLScorePairs.size(), TotalHits.Relation.EQUAL_TO);

        SearchHits searchHits = new SearchHits(hits, totalHits, 1.0f);
        SearchResponseSections internal = new SearchResponseSections(searchHits, null, null, false, false, null, 0);
        response = new SearchResponse(internal, null, 1, 1, 0, 1, new ShardSearchFailure[0], new SearchResponse.Clusters(1, 1, 0), null);
    }

    /**
     * Creates a searchResponse where the value to reRank is not Nested.
     * The location where the target is within the first level of the _source mapping.
     * There will be other fields as well (this is a dense map), the expected behavior is to leave the _source mapping
     * without the targetField and leave the other fields intact.
     * <hr>
     * The targetField for this scenario is <code>ml_score</code>
     */
    public void setUpValidSearchResultsWithNonNestedTargetValueWithDenseSourceMapping() throws IOException {
        SearchHit[] hits = new SearchHit[sampleIndexMLScorePairs.size()];

        String templateString = """
            {
               "my_field" : "%s",
               "ml_score" : %s,
                "info"    : {
                       "model" : "myModel"
                }
            }
            """.replace("\n", "");

        for (int i = 0; i < sampleIndexMLScorePairs.size(); i++) {
            int docId = sampleIndexMLScorePairs.get(i).getKey();
            String mlScore = sampleIndexMLScorePairs.get(i).getValue() + "";

            String sourceMap = templateString.formatted(i, mlScore);

            hits[i] = new SearchHit(docId, docId + "", Collections.emptyMap(), Collections.emptyMap());
            hits[i].sourceRef(new BytesArray(sourceMap));
            hits[i].score(new Random().nextFloat());
        }

        TotalHits totalHits = new TotalHits(sampleIndexMLScorePairs.size(), TotalHits.Relation.EQUAL_TO);

        SearchHits searchHits = new SearchHits(hits, totalHits, 1.0f);
        SearchResponseSections internal = new SearchResponseSections(searchHits, null, null, false, false, null, 0);
        response = new SearchResponse(internal, null, 1, 1, 0, 1, new ShardSearchFailure[0], new SearchResponse.Clusters(1, 1, 0), null);
    }

    /**
     * This test checks that the ByField successfully extracts <b>the values</b> using the targetField, this is
     * the responsibility of extending the RescoreRerankProcessor.
     * In this scenario it checks that the targetField is within the first level of the _source mapping.
     * <br>
     * The expected behavior is to check that the sample ML Scores are returned from the rescoreSearchResponse.
     * The target field is <code>ml_score</code>
     */
    public void testRescoreSearchResponse_returnsScoresSuccessfully_WhenResponseHasTargetValueFirstLevelOfSourceMapping()
        throws IOException {
        String targetField = "ml_score";
        setUpValidSearchResultsWithNonNestedTargetValueWithDenseSourceMapping();

        Map<String, Object> config = new HashMap<>(
            Map.of(RerankType.BY_FIELD.getLabel(), new HashMap<>(Map.of(ByFieldRerankProcessor.TARGET_FIELD, targetField)))
        );
        processor = (ByFieldRerankProcessor) factory.create(
            Map.of(),
            "rerank processor",
            "processor for 2nd level reranking based on provided field",
            false,
            config,
            pipelineContext
        );

        @SuppressWarnings("unchecked")
        ActionListener<List<Float>> listener = mock(ActionListener.class);
        processor.rescoreSearchResponse(response, Map.of(), listener);

        ArgumentCaptor<List<Float>> argCaptor = ArgumentCaptor.forClass(List.class);
        verify(listener, times(1)).onResponse(argCaptor.capture());

        assert (argCaptor.getValue().size() == sampleIndexMLScorePairs.size());
        for (int i = 0; i < sampleIndexMLScorePairs.size(); i++) {
            float mlScore = sampleIndexMLScorePairs.get(i).getValue();
            assert (argCaptor.getValue().get(i) == mlScore);
        }
    }

    /**
     * This test checks that the ByField successfully extracts <b>the values</b> using the targetField <b>(where the
     * target value is within a nested map)</b>, this is the responsibility of extending the RescoreRerankProcessor.
     * In this scenario it checks that the targetField is within a nested map.
     * <hr>
     * The expected behavior is to check that the sample ML Scores are returned from the rescoreSearchResponse.
     * the targetField is <code>ml.info.score</code>
     */
    public void testRescoreSearchResponse_returnsScoresSuccessfully_WhenResponseHasTargetValueInNestedMapping() throws IOException {
        String targetField = "ml.info.score";
        setUpValidSearchResultsWithNestedTargetValue();

        Map<String, Object> config = new HashMap<>(
            Map.of(RerankType.BY_FIELD.getLabel(), new HashMap<>(Map.of(ByFieldRerankProcessor.TARGET_FIELD, targetField)))
        );
        processor = (ByFieldRerankProcessor) factory.create(
            Map.of(),
            "rerank processor",
            "processor for 2nd level reranking based on provided field, This will check a nested field",
            false,
            config,
            pipelineContext
        );

        @SuppressWarnings("unchecked")
        ActionListener<List<Float>> listener = mock(ActionListener.class);
        processor.rescoreSearchResponse(response, Map.of(), listener);

        ArgumentCaptor<List<Float>> argCaptor = ArgumentCaptor.forClass(List.class);
        verify(listener, times(1)).onResponse(argCaptor.capture());

        assertEquals(sampleIndexMLScorePairs.size(), argCaptor.getValue().size());
        for (int i = 0; i < sampleIndexMLScorePairs.size(); i++) {
            float mlScore = sampleIndexMLScorePairs.get(i).getValue();
            assertEquals(mlScore, argCaptor.getValue().get(i), 0.01);
        }
    }

    /**
     * In this scenario the reRanking is being tested i.e. making sure that the search response has
     * updated <code>_score</code> fields. This also tests that they are returned in sorted order as
     * specified by <b>sortedScoresDescending</b>
     */
    public void testReRank_SortsDescendingWithNewScores_WhenResponseHasNestedField() throws IOException {
        String targetField = "ml.info.score";
        setUpValidSearchResultsWithNestedTargetValue();
        List<Map.Entry<Integer, Float>> sortedScoresDescending = sampleIndexMLScorePairs.stream()
            .sorted(Map.Entry.<Integer, Float>comparingByValue().reversed())
            .toList();

        Map<String, Object> config = new HashMap<>(
            Map.of(RerankType.BY_FIELD.getLabel(), new HashMap<>(Map.of(ByFieldRerankProcessor.TARGET_FIELD, targetField)))
        );
        processor = (ByFieldRerankProcessor) factory.create(
            Map.of(),
            "rerank processor",
            "processor for 2nd level reranking based on provided field, This will check a nested field",
            false,
            config,
            pipelineContext
        );
        ActionListener<SearchResponse> listener = mock(ActionListener.class);
        processor.rerank(response, Map.of(), listener);

        ArgumentCaptor<SearchResponse> argCaptor = ArgumentCaptor.forClass(SearchResponse.class);

        verify(listener, times(1)).onResponse(argCaptor.capture());
        SearchResponse searchResponse = argCaptor.getValue();

        assertEquals(sampleIndexMLScorePairs.size(), searchResponse.getHits().getHits().length);
        assertEquals(sortedScoresDescending.getFirst().getValue(), searchResponse.getHits().getMaxScore(), 0.0001);

        for (int i = 0; i < sortedScoresDescending.size(); i++) {
            int docId = sortedScoresDescending.get(i).getKey();
            float ml_score = sortedScoresDescending.get(i).getValue();
            assertEquals(docId, searchResponse.getHits().getAt(i).docId());
            assertEquals(ml_score, searchResponse.getHits().getAt(i).getScore(), 0.001);
        }
    }

    /**
     * This scenario adds the <code>remove_target_field</code> to be able to test that <code>_source</code> mapping
     * has been modified.
     * <p>
     * In this scenario the object will start off like this
     * <pre>
     * {
     *    "my_field" : "%s",
     *    "ml": {
     *         "model" : "myModel",
     *         "info"  : {
     *          "score": %s
     *         }
     *    }
     *  }
     * </pre>
     * and then be transformed into
     * <pre>
     * {
     *     "my_field" : "%s",
     *     "ml": {
     *         "model" : "myModel"
     *      },
     *      "previous_score" : float
     * }
     * </pre>
     * The reason for this was to delete any empty maps as the result of deleting <code>score</code>.
     * This test also checks that previous score was added
     */
    public void testReRank_deletesEmptyMaps_WhenResponseHasNestedField() throws IOException {
        String targetField = "ml.info.score";
        boolean removeTargetField = true;
        setUpValidSearchResultsWithNestedTargetValue();

        Map<String, Object> config = new HashMap<>(
            Map.of(
                RerankType.BY_FIELD.getLabel(),
                new HashMap<>(
                    Map.of(ByFieldRerankProcessor.TARGET_FIELD, targetField, ByFieldRerankProcessor.REMOVE_TARGET_FIELD, removeTargetField)
                )
            )
        );
        processor = (ByFieldRerankProcessor) factory.create(
            Map.of(),
            "rerank processor",
            "processor for 2nd level reranking based on provided field, This will check a nested field",
            false,
            config,
            pipelineContext
        );
        ActionListener<SearchResponse> listener = mock(ActionListener.class);
        processor.rerank(response, Map.of(), listener);

        ArgumentCaptor<SearchResponse> argCaptor = ArgumentCaptor.forClass(SearchResponse.class);

        verify(listener, times(1)).onResponse(argCaptor.capture());
        SearchResponse searchResponse = argCaptor.getValue();

        assertEquals(sampleIndexMLScorePairs.size(), searchResponse.getHits().getHits().length);

        for (int i = 0; i < searchResponse.getHits().getHits().length; i++) {
            SearchHit searchHit = searchResponse.getHits().getAt(i);
            Map<String, Object> sourceMap = searchHit.getSourceAsMap();

            assertTrue("The source mapping now has `previous_score` entry", sourceMap.containsKey("previous_score"));
            assertEquals("The first level of the map is the containing `my_field`, `ml`, and `previous_score`", 3, sourceMap.size());

            @SuppressWarnings("unchecked")
            Map<String, Object> innerMLMap = (Map<String, Object>) sourceMap.get("ml");

            assertEquals("The ml map now only has 1 mapping `model` instead of 2", 1, innerMLMap.size());
            assertTrue("The ml map has `model` as a mapping", innerMLMap.containsKey("model"));
            assertFalse("The ml map no longer has the score `info` mapping ", innerMLMap.containsKey("info"));

        }
    }

    /**
     * This scenario adds the <code>remove_target_field</code> to be able to test that <code>_source</code> mapping
     * has been modified.
     * <p>
     * In this scenario the object will start off like this
     * <pre>
     * {
     *  "my_field" : "%s",
     *  "ml_score" : %s,
     *   "info"    : {
     *          "model" : "myModel"
     *    }
     * }
     * </pre>
     * and then be transformed into
     * <pre>
     * {
     *  "my_field" : "%s",
     *   "info"    : {
     *          "model" : "myModel"
     *    },
     *    "previous_score" : float
     * }
     * </pre>
     * This test also checks that previous score was added
     */
    public void testReRank_deletesEmptyMaps_WhenResponseHasNonNestedField() throws IOException {
        String targetField = "ml_score";
        boolean removeTargetField = true;
        setUpValidSearchResultsWithNonNestedTargetValueWithDenseSourceMapping();

        Map<String, Object> config = new HashMap<>(
            Map.of(
                RerankType.BY_FIELD.getLabel(),
                new HashMap<>(
                    Map.of(ByFieldRerankProcessor.TARGET_FIELD, targetField, ByFieldRerankProcessor.REMOVE_TARGET_FIELD, removeTargetField)
                )
            )
        );
        processor = (ByFieldRerankProcessor) factory.create(
            Map.of(),
            "rerank processor",
            "processor for 2nd level reranking based on provided field, This will check a nested field",
            false,
            config,
            pipelineContext
        );
        ActionListener<SearchResponse> listener = mock(ActionListener.class);
        processor.rerank(response, Map.of(), listener);

        ArgumentCaptor<SearchResponse> argCaptor = ArgumentCaptor.forClass(SearchResponse.class);

        verify(listener, times(1)).onResponse(argCaptor.capture());
        SearchResponse searchResponse = argCaptor.getValue();

        assertEquals(sampleIndexMLScorePairs.size(), searchResponse.getHits().getHits().length);

        for (int i = 0; i < searchResponse.getHits().getHits().length; i++) {
            SearchHit searchHit = searchResponse.getHits().getAt(i);
            Map<String, Object> sourceMap = searchHit.getSourceAsMap();

            assertTrue("The source mapping now has `previous_score` entry", sourceMap.containsKey("previous_score"));
            assertEquals("The first level of the map is the containing `my_field`, `info`, and `previous_score`", 3, sourceMap.size());

            @SuppressWarnings("unchecked")
            Map<String, Object> innerInfoMap = (Map<String, Object>) sourceMap.get("info");

            assertEquals("The info map has 1 mapping", 1, innerInfoMap.size());
            assertTrue("The info map has the model as the only mapping", innerInfoMap.containsKey("model"));

        }
    }

    /**
     * This scenario makes sure the contents of the nested mapping have been updated by checking that a new field
     * <code>previous_score</code> was added along with the correct values in which they came from
     * and that the targetField has been deleted (along with other empty maps as a result of deleting this entry).
     */
    public void testReRank_storesPreviousScoresInSourceMap_WhenResponseHasNestedField() throws IOException {
        String targetField = "ml.info.score";
        boolean removeTargetField = true;
        setUpValidSearchResultsWithNestedTargetValue();

        List<AbstractMap.SimpleImmutableEntry<Integer, Float>> previousDocIdScorePair = IntStream.range(
            0,
            response.getHits().getHits().length
        )
            .boxed()
            .map(i -> new AbstractMap.SimpleImmutableEntry<>(response.getHits().getAt(i).docId(), response.getHits().getAt(i).getScore()) {
            })
            .toList();

        Map<String, Object> config = new HashMap<>(
            Map.of(
                RerankType.BY_FIELD.getLabel(),
                new HashMap<>(
                    Map.of(ByFieldRerankProcessor.TARGET_FIELD, targetField, ByFieldRerankProcessor.REMOVE_TARGET_FIELD, removeTargetField)
                )
            )
        );
        processor = (ByFieldRerankProcessor) factory.create(
            Map.of(),
            "rerank processor",
            "processor for 2nd level reranking based on provided field, This will check a nested field",
            false,
            config,
            pipelineContext
        );
        ActionListener<SearchResponse> listener = mock(ActionListener.class);
        processor.rerank(response, Map.of(), listener);

        ArgumentCaptor<SearchResponse> argCaptor = ArgumentCaptor.forClass(SearchResponse.class);

        verify(listener, times(1)).onResponse(argCaptor.capture());
        SearchResponse searchResponse = argCaptor.getValue();

        for (int i = 0; i < searchResponse.getHits().getHits().length; i++) {
            float currentPreviousScore = ((Number) searchResponse.getHits().getAt(i).getSourceAsMap().get("previous_score")).floatValue();
            int currentDocId = searchResponse.getHits().getAt(i).docId();

            // to access the corresponding document id it does so by counting at 0
            float trackedPreviousScore = previousDocIdScorePair.get(currentDocId - 1).getValue();
            int trackedDocId = previousDocIdScorePair.get(currentDocId - 1).getKey();

            assertEquals("The document Ids need to match to compare previous scores", trackedDocId, currentDocId);
            assertEquals(
                "The scores for the search response previoiusly need to match to the score in the source map",
                trackedPreviousScore,
                currentPreviousScore,
                0.01
            );

        }
    }

    /**
     * This scenario makes sure the contents of the mapping have been updated by checking that a new field
     * <code>previous_score</code> was added along with the correct values in which they came from
     * and that the targetField has been deleted.
     */
    public void testReRank_storesPreviousScoresInSourceMap_WhenResponseHasNonNestedField() throws IOException {
        String targetField = "ml_score";
        boolean removeTargetField = true;
        setUpValidSearchResultsWithNonNestedTargetValueWithDenseSourceMapping();

        List<AbstractMap.SimpleImmutableEntry<Integer, Float>> previousDocIdScorePair = IntStream.range(
            0,
            response.getHits().getHits().length
        )
            .boxed()
            .map(i -> new AbstractMap.SimpleImmutableEntry<>(response.getHits().getAt(i).docId(), response.getHits().getAt(i).getScore()) {
            })
            .toList();

        Map<String, Object> config = new HashMap<>(
            Map.of(
                RerankType.BY_FIELD.getLabel(),
                new HashMap<>(
                    Map.of(ByFieldRerankProcessor.TARGET_FIELD, targetField, ByFieldRerankProcessor.REMOVE_TARGET_FIELD, removeTargetField)
                )
            )
        );
        processor = (ByFieldRerankProcessor) factory.create(
            Map.of(),
            "rerank processor",
            "processor for 2nd level reranking based on provided field, This will check a nested field",
            false,
            config,
            pipelineContext
        );
        ActionListener<SearchResponse> listener = mock(ActionListener.class);
        processor.rerank(response, Map.of(), listener);

        ArgumentCaptor<SearchResponse> argCaptor = ArgumentCaptor.forClass(SearchResponse.class);

        verify(listener, times(1)).onResponse(argCaptor.capture());
        SearchResponse searchResponse = argCaptor.getValue();

        for (int i = 0; i < searchResponse.getHits().getHits().length; i++) {
            float currentPreviousScore = ((Number) searchResponse.getHits().getAt(i).getSourceAsMap().get("previous_score")).floatValue();
            int currentDocId = searchResponse.getHits().getAt(i).docId();

            // to access the corresponding document id it does so by counting at 0
            float trackedPreviousScore = previousDocIdScorePair.get(currentDocId - 1).getValue();
            int trackedDocId = previousDocIdScorePair.get(currentDocId - 1).getKey();

            assertEquals("The document Ids need to match to compare previous scores", trackedDocId, currentDocId);
            assertEquals(
                "The scores for the search response previoiusly need to match to the score in the source map",
                trackedPreviousScore,
                currentPreviousScore,
                0.01
            );

        }
    }
}
