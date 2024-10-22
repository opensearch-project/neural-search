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
import org.opensearch.common.document.DocumentField;
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

    public void testBasics() throws IOException {
        setUpValidSearchResultsWithNonNestedTargetValueWithDenseSourceMapping();
        String targetField = "ml_score";
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

        assert (processor.getTag().equals("rerank processor"));
        assert (processor.getDescription().equals("processor for 2nd level reranking based on provided field"));
        assert (!processor.isIgnoreFailure());
        assertThrows(
            "Use asyncProcessResponse unless you can guarantee to not deadlock yourself",
            UnsupportedOperationException.class,
            () -> processor.processResponse(request, response)
        );
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
     * has been modified. It also asserts that the previous_score has been aggregated by <code>keep_previous_score</code>
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
     * This test also checks that previous score was added as a result of <code>keep_previous_score</code> being true
     */
    public void testReRank_deletesEmptyMapsAndKeepsPreviousScore_WhenResponseHasNestedField() throws IOException {
        String targetField = "ml.info.score";
        boolean removeTargetField = true;
        boolean keepPreviousScore = true;

        setUpValidSearchResultsWithNestedTargetValue();

        Map<String, Object> config = new HashMap<>(
            Map.of(
                RerankType.BY_FIELD.getLabel(),
                new HashMap<>(
                    Map.of(
                        ByFieldRerankProcessor.TARGET_FIELD,
                        targetField,
                        ByFieldRerankProcessor.REMOVE_TARGET_FIELD,
                        removeTargetField,
                        ByFieldRerankProcessor.KEEP_PREVIOUS_SCORE,
                        keepPreviousScore
                    )
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
     * This scenario tests the rerank functionality when the response has a nested field.
     * It adds the <code>remove_target_field</code> to verify that the <code>_source</code> mapping
     * has been modified. It also asserts that empty maps are deleted and no previous score is retained.
     * <p>
     * In this scenario the object will start off like this:
     * <pre>
     * {
     *    "my_field" : "%s",
     *    "ml": {
     *         "model" : "myModel",
     *         "info"  : {
     *          "score": %s
     *         }
     *    }
     * }
     * </pre>
     * and then be transformed into:
     * <pre>
     * {
     *     "my_field" : "%s",
     *     "ml": {
     *         "model" : "myModel"
     *     }
     * }
     * </pre>
     * The reason for this transformation is to delete any empty maps resulting from removing the <code>target_field</code>.
     * This test also verifies that the nested structure is properly handled and the target field is removed.
     */
    public void testReRank_deletesEmptyMapsAndHasNoPreviousScore_WhenResponseHasNestedField() throws IOException {
        String targetField = "ml.info.score";
        boolean removeTargetField = true;
        boolean keepPreviousScore = false;

        setUpValidSearchResultsWithNestedTargetValue();

        Map<String, Object> config = new HashMap<>(
            Map.of(
                RerankType.BY_FIELD.getLabel(),
                new HashMap<>(
                    Map.of(
                        ByFieldRerankProcessor.TARGET_FIELD,
                        targetField,
                        ByFieldRerankProcessor.REMOVE_TARGET_FIELD,
                        removeTargetField,
                        ByFieldRerankProcessor.KEEP_PREVIOUS_SCORE,
                        keepPreviousScore
                    )
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

            assertTrue(
                "The source mapping does ot have `previous_score` entry because "
                    + ByFieldRerankProcessor.KEEP_PREVIOUS_SCORE
                    + " is "
                    + keepPreviousScore,
                !sourceMap.containsKey("previous_score")
            );
            assertEquals("The first level of the map is the containing `my_field` and `ml`", 2, sourceMap.size());

            @SuppressWarnings("unchecked")
            Map<String, Object> innerMLMap = (Map<String, Object>) sourceMap.get("ml");

            assertEquals("The ml map now only has 1 mapping `model` instead of 2", 1, innerMLMap.size());
            assertTrue("The ml map has `model` as a mapping", innerMLMap.containsKey("model"));
            assertFalse("The ml map no longer has the score `info` mapping ", innerMLMap.containsKey("info"));

        }
    }

    /**
     * This scenario adds the <code>remove_target_field</code> to be able to test that <code>_source</code> mapping
     * has been modified. It also enables <code>keep_previous_score</code> to test that <code>previous_score</code> is appended.
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
     */
    public void testReRank_deletesEmptyMapsAndKeepsPreviousScore_WhenResponseHasNonNestedField() throws IOException {
        String targetField = "ml_score";
        boolean removeTargetField = true;
        boolean keepPreviousScore = true;
        setUpValidSearchResultsWithNonNestedTargetValueWithDenseSourceMapping();

        Map<String, Object> config = new HashMap<>(
            Map.of(
                RerankType.BY_FIELD.getLabel(),
                new HashMap<>(
                    Map.of(
                        ByFieldRerankProcessor.TARGET_FIELD,
                        targetField,
                        ByFieldRerankProcessor.REMOVE_TARGET_FIELD,
                        removeTargetField,
                        ByFieldRerankProcessor.KEEP_PREVIOUS_SCORE,
                        keepPreviousScore
                    )
                )
            )
        );
        processor = (ByFieldRerankProcessor) factory.create(
            Map.of(),
            "rerank processor",
            "processor for 2nd level reranking based on provided field, This will check a NON-nested field",
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
    * This scenario tests the rerank functionality when the response has a non-nested field.
    * It adds the <code>remove_target_field</code> to verify that the <code>_source</code> mapping
    * has been modified. It also disables <code>keep_previous_score</code> to test that <code>previous_score</code> is not appended.
    * <p>
    * In this scenario the object will start off like this:
    * <pre>
    * {
    *  "my_field" : "%s",
    *  "ml_score" : %s,
    *   "info"    : {
    *          "model" : "myModel"
    *    }
    * }
    * </pre>
    * and then be transformed into:
    * <pre>
    * {
    *  "my_field" : "%s",
    *   "info"    : {
    *          "model" : "myModel"
    *    }
    * }
    * </pre>
    * This test verifies that the target field is removed, empty maps are deleted, and no previous score is retained
    * when dealing with a non-nested field structure.
    */
    public void testReRank_deletesEmptyMapsAndHasNoPreviousScore_WhenResponseHasNonNestedField() throws IOException {
        String targetField = "ml_score";
        boolean removeTargetField = true;
        boolean keepPreviousScore = false;
        setUpValidSearchResultsWithNonNestedTargetValueWithDenseSourceMapping();

        Map<String, Object> config = new HashMap<>(
            Map.of(
                RerankType.BY_FIELD.getLabel(),
                new HashMap<>(
                    Map.of(
                        ByFieldRerankProcessor.TARGET_FIELD,
                        targetField,
                        ByFieldRerankProcessor.REMOVE_TARGET_FIELD,
                        removeTargetField,
                        ByFieldRerankProcessor.KEEP_PREVIOUS_SCORE,
                        keepPreviousScore
                    )
                )
            )
        );
        processor = (ByFieldRerankProcessor) factory.create(
            Map.of(),
            "rerank processor",
            "processor for 2nd level reranking based on provided field, This will check a NON-nested field",
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

            assertTrue(
                "The source mapping does ot have `previous_score` entry because "
                    + ByFieldRerankProcessor.KEEP_PREVIOUS_SCORE
                    + " is "
                    + keepPreviousScore,
                !sourceMap.containsKey("previous_score")
            );
            assertEquals("The first level of the map is the containing `my_field` and `info`", 2, sourceMap.size());

            @SuppressWarnings("unchecked")
            Map<String, Object> innerInfoMap = (Map<String, Object>) sourceMap.get("info");

            assertEquals("The info map has 1 mapping", 1, innerInfoMap.size());
            assertTrue("The info map has the model as the only mapping", innerInfoMap.containsKey("model"));

        }
    }

    /**
     * This scenario makes sure turning on <code>keep_previous_score</code>, updates the contents of the <b>nested
     * mapping</b> by checking that a new field <code>previous_score</code> was added along with the correct values in which they came from
     * and that the targetField has been deleted (along with other empty maps as a result of deleting this entry).
     * <hr>
     * In order to check that <code>previous_score</code> is valid it will check that the docIds and scores match from
     * the resulting rerank and original sample data
     */
    public void testReRank_storesPreviousScoresInSourceMap_WhenResponseHasNestedField() throws IOException {
        String targetField = "ml.info.score";
        boolean removeTargetField = true;
        boolean keepPreviousScore = true;
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
                    Map.of(
                        ByFieldRerankProcessor.TARGET_FIELD,
                        targetField,
                        ByFieldRerankProcessor.REMOVE_TARGET_FIELD,
                        removeTargetField,
                        ByFieldRerankProcessor.KEEP_PREVIOUS_SCORE,
                        keepPreviousScore
                    )
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
     * This scenario makes sure turning on <code>keep_previous_score</code>, updates the contents of the <b>NON-nested
     * mapping</b> by checking that a new field <code>previous_score</code> was added along with the correct values in which they came from
     * and that the targetField has been deleted (along with other empty maps as a result of deleting this entry).
     * <hr>
     * In order to check that <code>previous_score</code> is valid it will check that the docIds and scores match from
     * the resulting rerank and original sample data
     */
    public void testReRank_storesPreviousScoresInSourceMap_WhenResponseHasNonNestedField() throws IOException {
        String targetField = "ml_score";
        boolean removeTargetField = true;
        boolean keepPreviousScore = true;
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
                    Map.of(
                        ByFieldRerankProcessor.TARGET_FIELD,
                        targetField,
                        ByFieldRerankProcessor.REMOVE_TARGET_FIELD,
                        removeTargetField,
                        ByFieldRerankProcessor.KEEP_PREVIOUS_SCORE,
                        keepPreviousScore
                    )
                )
            )
        );
        processor = (ByFieldRerankProcessor) factory.create(
            Map.of(),
            "rerank processor",
            "processor for 2nd level reranking based on provided field, This will check a Non-nested field",
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
                "The scores for the search response previously need to match to the score in the source map",
                trackedPreviousScore,
                currentPreviousScore,
                0.01
            );

        }
    }

    public void testRerank_keepsTargetFieldAndHasNoPreviousScore_WhenByFieldHasDefaultValues() throws IOException {
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

            // Test that the path to targetField is valid
            Map<String, Object> currentMap = searchResponse.getHits().getAt(i).getSourceAsMap();
            String[] keys = targetField.split("\\.");
            String lastKey = keys[keys.length - 1];
            for (int keyIndex = 0; keyIndex < keys.length - 1; keyIndex++) {
                String key = keys[keyIndex];
                assertTrue("The key:" + key + "does not exist in" + currentMap, currentMap.containsKey(key));
                currentMap = (Map<String, Object>) currentMap.get(key);
            }
            assertTrue("The key:" + lastKey + "does not exist in" + currentMap, currentMap.containsKey(lastKey));

        }
    }

    /**
     * Creates a searchResponse where the value to reRank by is Nested.
     * The location where the target is within a map of size 1 meaning after
     * Using ByFieldReRank the expected behavior is to delete the info mapping
     * as it is only has one mapping i.e. the duplicate value.
     * <hr>
     * The targetField for this scenario is <code>ml.info.score</code>
     */
    private void setUpValidSearchResultsWithNestedTargetValue() throws IOException {
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
            hits[i].score(1);
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
    private void setUpValidSearchResultsWithNonNestedTargetValueWithDenseSourceMapping() throws IOException {
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
            hits[i].score(1);
        }

        TotalHits totalHits = new TotalHits(sampleIndexMLScorePairs.size(), TotalHits.Relation.EQUAL_TO);

        SearchHits searchHits = new SearchHits(hits, totalHits, 1.0f);
        SearchResponseSections internal = new SearchResponseSections(searchHits, null, null, false, false, null, 0);
        response = new SearchResponse(internal, null, 1, 1, 0, 1, new ShardSearchFailure[0], new SearchResponse.Clusters(1, 1, 0), null);
    }

    /**
     * This scenario checks the byField rerank is able to check when a search hit has no source mapping.
     * It is always required to have a source mapping if you want to use this processor.
     */
    public void testRerank_throwsExceptionOnNoSource_WhenSearchResponseHasNoSourceMapping() {
        String targetField = "similarity_score";
        boolean removeTargetField = true;
        setUpInvalidSearchResultsWithNonSourceMapping();

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
            "processor for 2nd level reranking based on provided field. <This will be used for error handling>",
            false,
            config,
            pipelineContext
        );

        ActionListener<SearchResponse> listener = mock(ActionListener.class);

        processor.rerank(response, config, listener);

        ArgumentCaptor<Exception> argumentCaptor = ArgumentCaptor.forClass(Exception.class);
        verify(listener, times(1)).onFailure(argumentCaptor.capture());

        assertEquals("There is no source field to be able to perform rerank on hit [" + 1 + "]", argumentCaptor.getValue().getMessage());
        assert (argumentCaptor.getValue() instanceof IllegalArgumentException);
    }

    /**
     * The scenario checks that the search response has a source mapping for each search hit and verifies that the target field exists.
     * In this case the test will see that the target field has no entry inside the source mapping.
     */
    public void testRerank_throwsExceptionOnMappingNotExistingInSource_WhenSearchResponseHasAMissingMapping() {
        String targetField = "similarity_score";
        boolean removeTargetField = true;
        setUpInvalidSearchResultsWithMissingTargetFieldMapping();

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
            "processor for 2nd level reranking based on provided field. <This will be used for error handling>",
            false,
            config,
            pipelineContext
        );

        ActionListener<SearchResponse> listener = mock(ActionListener.class);

        processor.rerank(response, config, listener);

        ArgumentCaptor<Exception> argumentCaptor = ArgumentCaptor.forClass(Exception.class);
        verify(listener, times(1)).onFailure(argumentCaptor.capture());

        assertEquals("The field to rerank by is not found at hit [" + 1 + "]", argumentCaptor.getValue().getMessage());
        assert (argumentCaptor.getValue() instanceof IllegalArgumentException);
    }

    /**
     * The scenario checks that the search response has source mapping within each search hit
     * and the entry for the target field exists. However, the value for the target value target field is null the
     * expected behavior is to return a message that it is not found, similar to the test case where there's no
     * entry mapping for this target field
     */
    public void testRerank_throwsExceptionOnHavingEmptyMapping_WhenTargetFieldHasNullMapping() {
        String targetField = "similarity_score";
        boolean removeTargetField = true;
        setUpInvalidSearchResultsWithTargetFieldHavingNullMapping();

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
            "processor for 2nd level reranking based on provided field. <This will be used for error handling>",
            false,
            config,
            pipelineContext
        );

        ActionListener<SearchResponse> listener = mock(ActionListener.class);

        processor.rerank(response, config, listener);

        ArgumentCaptor<Exception> argumentCaptor = ArgumentCaptor.forClass(Exception.class);
        verify(listener, times(1)).onFailure(argumentCaptor.capture());

        assertEquals("The field to rerank by is not found at hit [" + 1 + "]", argumentCaptor.getValue().getMessage());
        assert (argumentCaptor.getValue() instanceof IllegalArgumentException);
    }

    /**
     * the scenario checks that the search response has source mapping within each search it and the entry for the target field exist.
     * however, the value for the target field is non-numeric the expected behaviors is to throw an exception that the value is not numeric.
     */
    public void testRerank_throwsExceptionOnHavingNonNumericValue_WhenTargetFieldHasNonNumericMapping() {
        String targetField = "similarity_score";
        boolean removeTargetField = true;
        setUpInvalidSearchResultsWithTargetFieldHavingNonNumericMapping();

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
            "processor for 2nd level reranking based on provided field. <This will be used for error handling>",
            false,
            config,
            pipelineContext
        );

        ActionListener<SearchResponse> listener = mock(ActionListener.class);

        processor.rerank(response, config, listener);

        ArgumentCaptor<Exception> argumentCaptor = ArgumentCaptor.forClass(Exception.class);
        verify(listener, times(1)).onFailure(argumentCaptor.capture());

        assertEquals("The field mapping to rerank by [hello world] is not Numerical", argumentCaptor.getValue().getMessage());
        assert (argumentCaptor.getValue() instanceof IllegalArgumentException);

    }

    /**
     * This creates a search response with two hits, the first hit being in the correct form.
     * While, the second search hit has a non-numeric target field mapping.
     */
    private void setUpInvalidSearchResultsWithTargetFieldHavingNonNumericMapping() {
        SearchHit[] hits = new SearchHit[2];
        hits[0] = new SearchHit(0, "1", Collections.emptyMap(), Collections.emptyMap());
        hits[0].sourceRef(new BytesArray("{\"diary\" : \"how are you\",\"similarity_score\":777}"));
        hits[0].score(1.0F);

        Map<String, DocumentField> dummyMap = new HashMap<>();
        dummyMap.put("test", new DocumentField("test", Collections.singletonList("test-field-mapping")));
        hits[1] = new SearchHit(1, "2", dummyMap, Collections.emptyMap());
        hits[1].sourceRef(new BytesArray("{\"diary\" : \"how do you do\",\"similarity_score\":\"hello world\"}"));
        hits[1].score(1.0F);

        SearchHits searchHits = new SearchHits(hits, new TotalHits(1, TotalHits.Relation.EQUAL_TO), 1);
        SearchResponseSections searchResponseSections = new SearchResponseSections(searchHits, null, null, false, false, null, 0);
        this.response = new SearchResponse(searchResponseSections, null, 1, 1, 0, 10, null, null);
    }

    /**
     * This creates a search response with two hits, the first hit being in the correct form.
     * While, the second search hit has a null target field mapping.
     */
    private void setUpInvalidSearchResultsWithTargetFieldHavingNullMapping() {
        SearchHit[] hits = new SearchHit[2];
        hits[0] = new SearchHit(0, "1", Collections.emptyMap(), Collections.emptyMap());
        hits[0].sourceRef(new BytesArray("{\"diary\" : \"how are you\",\"similarity_score\":-11.055182}"));
        hits[0].score(1.0F);

        Map<String, DocumentField> dummyMap = new HashMap<>();
        dummyMap.put("test", new DocumentField("test", Collections.singletonList("test-field-mapping")));
        hits[1] = new SearchHit(1, "2", dummyMap, Collections.emptyMap());
        hits[1].sourceRef(new BytesArray("{\"diary\" : \"how are you\",\"similarity_score\":null}"));
        hits[1].score(1.0F);

        SearchHits searchHits = new SearchHits(hits, new TotalHits(2, TotalHits.Relation.EQUAL_TO), 1);
        SearchResponseSections searchResponseSections = new SearchResponseSections(searchHits, null, null, false, false, null, 0);
        this.response = new SearchResponse(searchResponseSections, null, 1, 1, 0, 10, null, null);
    }

    /**
     * This creates a search response with two hits, the first hit being in the correct form.
     * While, the second search hit has having a missing entry that is needed to perform reranking
     */
    private void setUpInvalidSearchResultsWithMissingTargetFieldMapping() {
        SearchHit[] hits = new SearchHit[2];
        hits[0] = new SearchHit(0, "1", Collections.emptyMap(), Collections.emptyMap());
        hits[0].sourceRef(new BytesArray("{\"diary\" : \"how are you\",\"similarity_score\":-11.055182}"));
        hits[0].score(1.0F);

        hits[1] = new SearchHit(1, "2", Collections.emptyMap(), Collections.emptyMap());
        hits[1].sourceRef(new BytesArray("{\"diary\" : \"how are you\" }"));
        hits[1].score(1.0F);

        SearchHits searchHits = new SearchHits(hits, new TotalHits(2, TotalHits.Relation.EQUAL_TO), 1);
        SearchResponseSections searchResponseSections = new SearchResponseSections(searchHits, null, null, false, false, null, 0);
        this.response = new SearchResponse(searchResponseSections, null, 1, 1, 0, 10, null, null);

    }

    /**
     * This creates a search response with two hits, the first hit being in correct form.
     * While, the second search hit has no source mapping.
     */
    private void setUpInvalidSearchResultsWithNonSourceMapping() {
        SearchHit[] hits = new SearchHit[2];
        hits[0] = new SearchHit(0, "1", Collections.emptyMap(), Collections.emptyMap());
        hits[0].sourceRef(new BytesArray("{\"diary\" : \"how are you\",\"similarity_score\":-11.055182}"));
        hits[0].score(1.0F);

        Map<String, DocumentField> dummyMap = new HashMap<>();
        dummyMap.put("test", new DocumentField("test", Collections.singletonList("test-field-mapping")));
        hits[1] = new SearchHit(1, "2", dummyMap, Collections.emptyMap());
        hits[1].score(1.0F);

        SearchHits searchHits = new SearchHits(hits, new TotalHits(1, TotalHits.Relation.EQUAL_TO), 1);
        SearchResponseSections searchResponseSections = new SearchResponseSections(searchHits, null, null, false, false, null, 0);
        this.response = new SearchResponse(searchResponseSections, null, 1, 1, 0, 10, null, null);
    }

}
