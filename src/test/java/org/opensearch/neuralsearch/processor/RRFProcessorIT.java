/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.processor;

import lombok.SneakyThrows;
import org.opensearch.index.query.MatchQueryBuilder;
import org.opensearch.knn.index.query.KNNQueryBuilder;
import org.opensearch.neuralsearch.BaseNeuralSearchIT;
import org.opensearch.neuralsearch.query.HybridQueryBuilder;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.opensearch.neuralsearch.util.TestUtils.DELTA_FOR_SCORE_ASSERTION;
import static org.opensearch.neuralsearch.util.TestUtils.TEST_SPACE_TYPE;

public class RRFProcessorIT extends BaseNeuralSearchIT {

    private int currentDoc = 1;
    private static final String RRF_INDEX_NAME = "rrf-index";
    private static final String RRF_SEARCH_PIPELINE = "rrf-search-pipeline";
    private static final String RRF_INGEST_PIPELINE = "rrf-ingest-pipeline";

    private static final int RRF_DIMENSION = 5;

    @SneakyThrows
    public void testRRF_whenValidInput_thenSucceed() {
        createPipelineProcessor(null, RRF_INGEST_PIPELINE, ProcessorType.TEXT_EMBEDDING);
        prepareKnnIndex(RRF_INDEX_NAME, Collections.singletonList(new KNNFieldConfig("passage_embedding", RRF_DIMENSION, TEST_SPACE_TYPE)));
        addDocuments();
        createDefaultRRFSearchPipeline();

        HybridQueryBuilder hybridQueryBuilder = getHybridQueryBuilder();

        Map<String, Object> results = search(RRF_INDEX_NAME, hybridQueryBuilder, null, 5, Map.of("search_pipeline", RRF_SEARCH_PIPELINE));
        Map<String, Object> hits = (Map<String, Object>) results.get("hits");
        ArrayList<HashMap<String, Object>> hitsList = (ArrayList<HashMap<String, Object>>) hits.get("hits");
        assertEquals(3, hitsList.size());
        assertEquals(0.016393442, (Double) hitsList.getFirst().get("_score"), DELTA_FOR_SCORE_ASSERTION);
        assertEquals(0.016129032, (Double) hitsList.get(1).get("_score"), DELTA_FOR_SCORE_ASSERTION);
        assertEquals(0.015873017, (Double) hitsList.getLast().get("_score"), DELTA_FOR_SCORE_ASSERTION);

        createRRFSearchPipeline(RRF_SEARCH_PIPELINE, Arrays.asList(0.7, 0.3), false);
        Map<String, Object> weightedResults = search(
            RRF_INDEX_NAME,
            hybridQueryBuilder,
            null,
            5,
            Map.of("search_pipeline", RRF_SEARCH_PIPELINE)
        );
        Map<String, Object> weightedHits = (Map<String, Object>) weightedResults.get("hits");
        ArrayList<HashMap<String, Object>> weightedHitsList = (ArrayList<HashMap<String, Object>>) weightedHits.get("hits");
        assertEquals(3, weightedHitsList.size());
        assertEquals(0.011475409, (Double) weightedHitsList.getFirst().get("_score"), DELTA_FOR_SCORE_ASSERTION);
        assertEquals(0.011475409, (Double) weightedHitsList.get(1).get("_score"), DELTA_FOR_SCORE_ASSERTION);
        assertEquals(0.011290322, (Double) weightedHitsList.getLast().get("_score"), DELTA_FOR_SCORE_ASSERTION);

    }

    private HybridQueryBuilder getHybridQueryBuilder() {
        MatchQueryBuilder matchQueryBuilder = new MatchQueryBuilder("text", "cowboy rodeo bronco");
        KNNQueryBuilder knnQueryBuilder = new KNNQueryBuilder.Builder().fieldName("passage_embedding")
            .k(5)
            .vector(new float[] { 0.1f, 1.2f, 2.3f, 3.4f, 4.5f })
            .build();

        HybridQueryBuilder hybridQueryBuilder = new HybridQueryBuilder();
        hybridQueryBuilder.add(matchQueryBuilder);
        hybridQueryBuilder.add(knnQueryBuilder);
        return hybridQueryBuilder;
    }

    @SneakyThrows
    private void addDocuments() {
        addDocument(
            "A West Virginia university women 's basketball team , officials , and a small gathering of fans are in a West Virginia arena .",
            "4319130149.jpg"
        );
        addDocument("A wild animal races across an uncut field with a minimal amount of trees .", "1775029934.jpg");
        addDocument(
            "People line the stands which advertise Freemont 's orthopedics , a cowboy rides a light brown bucking bronco .",
            "2664027527.jpg"
        );
        addDocument("A man who is riding a wild horse in the rodeo is very near to falling off .", "4427058951.jpg");
        addDocument("A rodeo cowboy , wearing a cowboy hat , is being thrown off of a wild white horse .", "2691147709.jpg");
    }

    @SneakyThrows
    private void addDocument(String description, String imageText) {
        addDocument(RRF_INDEX_NAME, String.valueOf(currentDoc++), "text", description, "image_text", imageText);
    }
}
