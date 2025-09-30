/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.processor.mmr;

import lombok.SneakyThrows;
import org.junit.After;
import org.junit.Before;
import org.opensearch.common.xcontent.XContentFactory;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.knn.index.SpaceType;
import org.opensearch.knn.index.VectorDataType;
import org.opensearch.knn.search.processor.mmr.MMROverSampleProcessor;
import org.opensearch.knn.search.processor.mmr.MMRRerankProcessor;
import org.opensearch.neuralsearch.BaseNeuralSearchIT;
import org.opensearch.neuralsearch.query.NeuralQueryBuilder;

import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;

import static org.opensearch.knn.common.KNNConstants.CANDIDATES;
import static org.opensearch.knn.common.KNNConstants.DIVERSITY;
import static org.opensearch.knn.common.KNNConstants.MMR;
import static org.opensearch.knn.common.KNNConstants.MODEL_ID;
import static org.opensearch.knn.common.KNNConstants.VECTOR_FIELD_DATA_TYPE;
import static org.opensearch.knn.common.KNNConstants.VECTOR_FIELD_PATH;
import static org.opensearch.knn.common.KNNConstants.VECTOR_FIELD_SPACE_TYPE;
import static org.opensearch.search.pipeline.SearchPipelineService.ENABLED_SYSTEM_GENERATED_FACTORIES_SETTING;

public class MMRNeuralQueryTransformerIT extends BaseNeuralSearchIT {
    private final String INDEX_NAME = "test-index";
    private final String SEMANTIC_INDEX_MAPPING = "mmr/SemanticIndexMapping.json";
    private final String SEMANTIC_FIELD_NAME = "semantic_field";
    private final String SEMANTIC_VECTOR_FIELD_NAME = "semantic_field_semantic_info.embedding";
    private final String SEMANTIC_DOC_VALUE_1 = "This is the test doc 1.";
    private final String SEMANTIC_DOC_VALUE_2 = "This is the test doc 2.";
    private final String SEMANTIC_DOC_1 = Files.readString(Path.of(classLoader.getResource("processor/mmr/semantic_doc_1.json").toURI()));
    private final String SEMANTIC_DOC_2 = Files.readString(Path.of(classLoader.getResource("processor/mmr/semantic_doc_2.json").toURI()));
    private String modelId;

    public MMRNeuralQueryTransformerIT() throws IOException, URISyntaxException {}

    @Before
    public void setUp() throws Exception {
        super.setUp();
        enableMMRProcessors();
        setupIndex();
    }

    @After
    public void tearDown() throws Exception {
        super.tearDown();
        disableMMRProcessors();
    }

    private void setupIndex() throws Exception {
        modelId = prepareModel();
        loadAndWaitForModelToBeReady(modelId);
        createIndexWithModelId(INDEX_NAME, SEMANTIC_INDEX_MAPPING, modelId);
        for (int i = 0; i < 99; i++) {
            ingestDocument(INDEX_NAME, SEMANTIC_DOC_1);
        }
        ingestDocument(INDEX_NAME, SEMANTIC_DOC_2);
    }

    @SneakyThrows
    private void enableMMRProcessors() {
        updateClusterSettings(
            ENABLED_SYSTEM_GENERATED_FACTORIES_SETTING.getKey(),
            new String[] { MMROverSampleProcessor.MMROverSampleProcessorFactory.TYPE, MMRRerankProcessor.MMRRerankProcessorFactory.TYPE }
        );
    }

    @SneakyThrows
    private void disableMMRProcessors() {
        updateClusterSettings(ENABLED_SYSTEM_GENERATED_FACTORIES_SETTING.getKey(), "");
    }

    public void testMMR_whenNeuralQuerySemanticFieldAndNoVectorInfo_theSuccess() throws Exception {
        XContentBuilder builder = XContentFactory.jsonBuilder()
            .startObject()
            .startObject("query")
            .startObject(NeuralQueryBuilder.NAME)
            .startObject(SEMANTIC_FIELD_NAME)
            .field(NeuralQueryBuilder.QUERY_TEXT_FIELD.getPreferredName(), SEMANTIC_DOC_VALUE_1)
            .endObject()
            .endObject()
            .endObject()
            .startObject("ext")
            .startObject(MMR)
            .field(CANDIDATES, 100)
            .field(DIVERSITY, 0.9)
            .endObject()
            .endObject()
            .endObject();

        Map<String, Object> response = search(INDEX_NAME, builder.toString(), 2);

        assertResponse(response);
    }

    public void testMMR_whenNeuralQuerySemanticFieldAndProvideVectorInfo_theSuccess() throws Exception {
        XContentBuilder builder = XContentFactory.jsonBuilder()
            .startObject()
            .startObject("query")
            .startObject(NeuralQueryBuilder.NAME)
            .startObject(SEMANTIC_FIELD_NAME)
            .field(NeuralQueryBuilder.QUERY_TEXT_FIELD.getPreferredName(), SEMANTIC_DOC_VALUE_1)
            .endObject()
            .endObject()
            .endObject()
            .startObject("ext")
            .startObject(MMR)
            .field(CANDIDATES, 100)
            .field(DIVERSITY, 0.9)
            .field(VECTOR_FIELD_PATH, SEMANTIC_VECTOR_FIELD_NAME)
            .field(VECTOR_FIELD_SPACE_TYPE, SpaceType.L2.getValue())
            .field(VECTOR_FIELD_DATA_TYPE, VectorDataType.FLOAT.getValue())
            .endObject()
            .endObject()
            .endObject();

        Map<String, Object> response = search(INDEX_NAME, builder.toString(), 2);

        assertResponse(response);
    }

    public void testMMR_whenNeuralQueryKnnFieldAndNoVectorInfo_theSuccess() throws Exception {
        XContentBuilder builder = XContentFactory.jsonBuilder()
            .startObject()
            .startObject("query")
            .startObject(NeuralQueryBuilder.NAME)
            .startObject(SEMANTIC_VECTOR_FIELD_NAME)
            .field(NeuralQueryBuilder.QUERY_TEXT_FIELD.getPreferredName(), SEMANTIC_DOC_VALUE_1)
            .field(MODEL_ID, modelId)
            .endObject()
            .endObject()
            .endObject()
            .startObject("ext")
            .startObject(MMR)
            .field(CANDIDATES, 100)
            .field(DIVERSITY, 0.9)
            .endObject()
            .endObject()
            .endObject();

        Map<String, Object> response = search(INDEX_NAME, builder.toString(), 2);

        assertResponse(response);
    }

    public void testMMR_whenNeuralQueryKnnFieldAndProvideVectorInfo_theSuccess() throws Exception {
        XContentBuilder builder = XContentFactory.jsonBuilder()
            .startObject()
            .startObject("query")
            .startObject(NeuralQueryBuilder.NAME)
            .startObject(SEMANTIC_VECTOR_FIELD_NAME)
            .field(NeuralQueryBuilder.QUERY_TEXT_FIELD.getPreferredName(), SEMANTIC_DOC_VALUE_1)
            .field(MODEL_ID, modelId)
            .endObject()
            .endObject()
            .endObject()
            .startObject("ext")
            .startObject(MMR)
            .field(CANDIDATES, 100)
            .field(DIVERSITY, 0.9)
            .field(VECTOR_FIELD_PATH, SEMANTIC_VECTOR_FIELD_NAME)
            .field(VECTOR_FIELD_SPACE_TYPE, SpaceType.L2.getValue())
            .field(VECTOR_FIELD_DATA_TYPE, VectorDataType.FLOAT.getValue())
            .endObject()
            .endObject()
            .endObject();

        Map<String, Object> response = search(INDEX_NAME, builder.toString(), 2);

        assertResponse(response);
    }

    private void assertResponse(Map<String, Object> response) {
        assertHitCount(response);

        List<Map<String, Object>> hitsDoc = getHits(response);
        assertEquals("Should only return 2 docs since the query size is 2.", 2, hitsDoc.size());

        assertValueEquals(hitsDoc.get(0), SEMANTIC_DOC_VALUE_1);
        // The second doc in the response should be with the value 2 since we do mmr rerank
        assertValueEquals(hitsDoc.get(1), SEMANTIC_DOC_VALUE_2);
    }

    @SuppressWarnings("unchecked")
    private void assertHitCount(Map<String, Object> response) {
        Map<String, Object> hits = (Map<String, Object>) response.get("hits");
        Map<String, Object> totalHits = (Map<String, Object>) hits.get("total");
        assertEquals("Total hit should be 100 since we have 100 docs and the MMR candidates is 100.", 100, totalHits.get("value"));
    }

    @SuppressWarnings("unchecked")
    private List<Map<String, Object>> getHits(Map<String, Object> response) {
        return (List<Map<String, Object>>) ((Map<String, Object>) response.get("hits")).get("hits");
    }

    @SuppressWarnings("unchecked")
    private void assertValueEquals(Map<String, Object> hitDoc, String expectedValue) {
        Map<String, Object> source = (Map<String, Object>) hitDoc.get("_source");
        String actualValue = (String) source.get(SEMANTIC_FIELD_NAME);
        assertEquals(expectedValue, actualValue);
    }

}
