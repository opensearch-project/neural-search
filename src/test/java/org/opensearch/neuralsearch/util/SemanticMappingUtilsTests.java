/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.util;

import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.index.mapper.MapperService;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.opensearch.neuralsearch.util.SemanticMappingUtils.collectSemanticField;

public class SemanticMappingUtilsTests extends OpenSearchTestCase {
    private final ClassLoader classLoader = this.getClass().getClassLoader();
    private final NamedXContentRegistry namedXContentRegistry = new NamedXContentRegistry(Collections.emptyList());

    public void testCollectSemanticField() throws URISyntaxException, IOException {
        final String expectedTransformedMappingString = Files.readString(
            Path.of(classLoader.getResource("mapper/mappingWithNestedSemanticFields.json").toURI())
        );

        final Map<String, Object> originalMapping = MapperService.parseMapping(namedXContentRegistry, expectedTransformedMappingString);
        final Map<String, Map<String, Object>> semanticFieldPathToConfigMap = new HashMap<>();

        collectSemanticField(originalMapping, semanticFieldPathToConfigMap);

        final Map<String, Map<String, Object>> expectedPathToConfigMap = Map.of(
            "products.product_description",
            Map.of("model_id", "dummy model id", "type", "semantic")
        );
        assertEquals(expectedPathToConfigMap, semanticFieldPathToConfigMap);
    }

    public void testExtractModelIdToFieldPathMap() {
        final Map<String, Map<String, Object>> semanticFieldPathToConfigMap = Map.of(
            "semantic_field_1",
            Map.of("model_id", "dummy model id", "type", "semantic"),
            "semantic_field_2",
            Map.of("model_id", "dummy model id", "type", "semantic"),
            "semantic_field_3",
            Map.of("model_id", "dummy model id 3", "type", "semantic")
        );

        final Map<String, List<String>> modelIdToFieldPathMap = SemanticMappingUtils.extractModelIdToFieldPathMap(
            semanticFieldPathToConfigMap
        );
        modelIdToFieldPathMap.get("dummy model id").sort(String::compareTo);

        final Map<String, List<String>> expectedPathToFieldPathMap = Map.of(
            "dummy model id",
            List.of("semantic_field_1", "semantic_field_2"),
            "dummy model id 3",
            List.of("semantic_field_3")
        );

        assertEquals(expectedPathToFieldPathMap, modelIdToFieldPathMap);
    }
}
