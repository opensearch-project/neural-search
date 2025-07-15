/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.mappingtransformer;

import org.junit.Before;
import org.opensearch.common.xcontent.XContentType;
import org.opensearch.neuralsearch.BaseNeuralSearchIT;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Locale;
import java.util.Map;

public class SemanticMappingTransformerIT extends BaseNeuralSearchIT {
    private static final String INDEX_WITH_REMOTE_DENSE_MODEL = "semantic_field_remote_dense_model_index";

    @Before
    public void setUp() throws Exception {
        super.setUp();
        updateClusterSettings();
    }

    public void testTransformMappingWithRemoteDenseModel() throws Exception {
        final String createConnectorRequestBody = Files.readString(
            Path.of(classLoader.getResource("mappingtransformer/CreateConnectorRequestBody.json").toURI())
        );
        final String connectorId = createConnector(createConnectorRequestBody);

        final String registerRemoteDenseModelRequestBody = String.format(
            Locale.ROOT,
            Files.readString(Path.of(classLoader.getResource("mappingtransformer/RegisterRemoteDenseModelRequestBody.json").toURI())),
            connectorId
        );
        final String modelId = registerModelGroupAndUploadModel(registerRemoteDenseModelRequestBody);

        final String createIndexRequestBody = Files.readString(
            Path.of(classLoader.getResource("mappingtransformer/SemanticIndexMappings.json").toURI())
        );
        createSemanticIndexWithConfiguration(INDEX_WITH_REMOTE_DENSE_MODEL, createIndexRequestBody, modelId);

        final Map<String, Object> indexMapping = getIndexMapping(INDEX_WITH_REMOTE_DENSE_MODEL);

        final String expectedIndexMappingStr = String.format(
            Locale.ROOT,
            Files.readString(Path.of(classLoader.getResource("mappingtransformer/expectedIndexMappingWithRemoteDenseModel.json").toURI())),
            modelId
        );
        final Map<String, Object> expectedIndexMappingMap = createParser(XContentType.JSON.xContent(), expectedIndexMappingStr).map();

        org.assertj.core.api.Assertions.assertThat(indexMapping).isEqualTo(expectedIndexMappingMap);
    }
}
