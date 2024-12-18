/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.processor;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;

import org.apache.hc.core5.http.HttpHeaders;
import org.apache.hc.core5.http.io.entity.EntityUtils;
import org.apache.hc.core5.http.message.BasicHeader;
import org.junit.Before;
import org.opensearch.client.Response;
import org.opensearch.common.xcontent.XContentHelper;
import org.opensearch.common.xcontent.XContentType;
import org.opensearch.neuralsearch.BaseNeuralSearchIT;

import com.google.common.collect.ImmutableList;
import org.opensearch.neuralsearch.query.NeuralSparseQueryBuilder;

public class SparseEncodingProcessIT extends BaseNeuralSearchIT {

    private static final String INDEX_NAME = "sparse_encoding_index";

    private static final String PIPELINE_NAME = "pipeline-sparse-encoding";

    @Before
    public void setUp() throws Exception {
        super.setUp();
        updateClusterSettings();
    }

    public void testSparseEncodingProcessor() throws Exception {
        String modelId = null;
        try {
            modelId = prepareSparseEncodingModel();
            createPipelineProcessor(modelId, PIPELINE_NAME, ProcessorType.SPARSE_ENCODING);
            createSparseEncodingIndex();
            ingestDocument();
            assertEquals(1, getDocCount(INDEX_NAME));

            NeuralSparseQueryBuilder neuralSparseQueryBuilder = new NeuralSparseQueryBuilder();
            neuralSparseQueryBuilder.fieldName("title_sparse");
            neuralSparseQueryBuilder.queryTokensSupplier(() -> Map.of("good", 1.0f, "a", 2.0f));
            Map<String, Object> searchResponse = search(INDEX_NAME, neuralSparseQueryBuilder, 2);
            assertFalse(searchResponse.isEmpty());
            double maxScore = (Double) ((Map) searchResponse.get("hits")).get("max_score");
            assertEquals(4.4433594, maxScore, 1e-3);
        } finally {
            wipeOfTestResources(INDEX_NAME, PIPELINE_NAME, modelId, null);
        }
    }

    public void testSparseEncodingProcessorWithPrune() throws Exception {
        String modelId = null;
        try {
            modelId = prepareSparseEncodingModel();
            createPipelineProcessor(modelId, PIPELINE_NAME, ProcessorType.SPARSE_ENCODING_PRUNE);
            createSparseEncodingIndex();
            ingestDocument();
            assertEquals(1, getDocCount(INDEX_NAME));

            NeuralSparseQueryBuilder neuralSparseQueryBuilder = new NeuralSparseQueryBuilder();
            neuralSparseQueryBuilder.fieldName("title_sparse");
            neuralSparseQueryBuilder.queryTokensSupplier(() -> Map.of("good", 1.0f, "a", 2.0f));
            Map<String, Object> searchResponse = search(INDEX_NAME, neuralSparseQueryBuilder, 2);
            assertFalse(searchResponse.isEmpty());
            double maxScore = (Double) ((Map) searchResponse.get("hits")).get("max_score");
            assertEquals(3.640625, maxScore, 1e-3);
        } finally {
            wipeOfTestResources(INDEX_NAME, PIPELINE_NAME, modelId, null);
        }
    }

    private void createSparseEncodingIndex() throws Exception {
        createIndexWithConfiguration(
            INDEX_NAME,
            Files.readString(Path.of(classLoader.getResource("processor/SparseEncodingIndexMappings.json").toURI())),
            PIPELINE_NAME
        );
    }

    private void ingestDocument() throws Exception {
        String ingestDocument = "{\n"
            + "  \"title\": \"This is a good day\",\n"
            + "  \"description\": \"daily logging\",\n"
            + "  \"favor_list\": [\n"
            + "    \"test\",\n"
            + "    \"hello\",\n"
            + "    \"mock\"\n"
            + "  ],\n"
            + "  \"favorites\": {\n"
            + "    \"game\": \"overwatch\",\n"
            + "    \"movie\": null\n"
            + "  }\n"
            + "}\n";
        Response response = makeRequest(
            client(),
            "POST",
            INDEX_NAME + "/_doc?refresh",
            null,
            toHttpEntity(ingestDocument),
            ImmutableList.of(new BasicHeader(HttpHeaders.USER_AGENT, "Kibana"))
        );
        Map<String, Object> map = XContentHelper.convertToMap(
            XContentType.JSON.xContent(),
            EntityUtils.toString(response.getEntity()),
            false
        );
        assertEquals("created", map.get("result"));
    }

}
