/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.processor;

import lombok.SneakyThrows;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.xcontent.XContentFactory;
import org.opensearch.index.query.MatchQueryBuilder;
import org.opensearch.neuralsearch.BaseNeuralSearchIT;
import org.opensearch.neuralsearch.query.HybridQueryBuilder;
import org.opensearch.neuralsearch.query.NeuralQueryBuilder;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import static org.opensearch.neuralsearch.util.TestUtils.DELTA_FOR_SCORE_ASSERTION;

public class RRFProcessorIT extends BaseNeuralSearchIT {

    private int currentDoc = 1;
    private static final String RRF_INDEX_NAME = "rrf-index";

    @SneakyThrows
    public void testRRF_whenValidInput_thenSucceed() {
        String ingestPipelineName = "rrf-ingest-pipeline";
        String modelId = null;
        try {
            modelId = prepareModel();
            createPipelineProcessor(modelId, ingestPipelineName, ProcessorType.TEXT_EMBEDDING);
            Settings indexSettings = Settings.builder().put("index.knn", true).put("default_pipeline", ingestPipelineName).build();
            String indexMappings = XContentFactory.jsonBuilder()
                .startObject()
                .startObject("properties")
                .startObject("id")
                .field("type", "text")
                .endObject()
                .startObject("passage_embedding")
                .field("type", "knn_vector")
                .field("dimension", "768")
                .startObject("method")
                .field("engine", "lucene")
                .field("space_type", "l2")
                .field("name", "hnsw")
                .endObject()
                .endObject()
                .startObject("text")
                .field("type", "text")
                .endObject()
                .endObject()
                .endObject()
                .toString();
            // Removes the {} around the string, since they are already included with createIndex
            indexMappings = indexMappings.substring(1, indexMappings.length() - 1);
            createIndex(RRF_INDEX_NAME, indexSettings, indexMappings, null);
            addDocuments();
            createDefaultRRFSearchPipeline();

            HybridQueryBuilder hybridQueryBuilder = getHybridQueryBuilder(modelId);

            Map<String, Object> results = search(
                RRF_INDEX_NAME,
                hybridQueryBuilder,
                null,
                5,
                Map.of("search_pipeline", RRF_SEARCH_PIPELINE)
            );
            Map<String, Object> hits = (Map<String, Object>) results.get("hits");
            ArrayList<HashMap<String, Object>> hitsList = (ArrayList<HashMap<String, Object>>) hits.get("hits");
            assertEquals(3, hitsList.size());
            assertEquals(0.016393442, (Double) hitsList.getFirst().get("_score"), DELTA_FOR_SCORE_ASSERTION);
            assertEquals(0.016129032, (Double) hitsList.get(1).get("_score"), DELTA_FOR_SCORE_ASSERTION);
            assertEquals(0.015873017, (Double) hitsList.getLast().get("_score"), DELTA_FOR_SCORE_ASSERTION);
        } finally {
            wipeOfTestResources(RRF_INDEX_NAME, ingestPipelineName, modelId, RRF_SEARCH_PIPELINE);
        }
    }

    private HybridQueryBuilder getHybridQueryBuilder(String modelId) {
        MatchQueryBuilder matchQueryBuilder = new MatchQueryBuilder("text", "cowboy rodeo bronco");

        NeuralQueryBuilder neuralQueryBuilder = new NeuralQueryBuilder(
            "passage_embedding",
            "wild_west",
            "",
            modelId,
            5,
            null,
            null,
            null,
            null,
            null,
            null
        );

        HybridQueryBuilder hybridQueryBuilder = new HybridQueryBuilder();
        hybridQueryBuilder.add(matchQueryBuilder);
        hybridQueryBuilder.add(neuralQueryBuilder);
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
