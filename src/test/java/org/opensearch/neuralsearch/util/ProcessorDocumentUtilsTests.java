/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.util;

import org.junit.Before;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.xcontent.XContentHelper;
import org.opensearch.common.xcontent.XContentType;
import org.opensearch.env.Environment;
import org.opensearch.neuralsearch.query.OpenSearchQueryTestCase;

import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.RETURNS_DEEP_STUBS;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class ProcessorDocumentUtilsTests extends OpenSearchQueryTestCase {

    private ClusterService clusterService = mock(ClusterService.class, RETURNS_DEEP_STUBS);

    @Mock
    private Environment environment;

    @Before
    public void setup() {
        MockitoAnnotations.openMocks(this);
    }

    public void testValidateMapTypeValue_withDifferentConfigurations_thenSuccess() throws URISyntaxException, IOException {
        Settings settings = Settings.builder().put("index.mapping.depth.limit", 20).build();
        when(clusterService.state().metadata().index(anyString()).getSettings()).thenReturn(settings);
        String processorDocumentTestJson = Files.readString(
            Path.of(ProcessorDocumentUtils.class.getClassLoader().getResource("util/ProcessorDocumentUtils.json").toURI())
        );
        Map<String, Object> processorDocumentTestMap = XContentHelper.convertToMap(
            XContentType.JSON.xContent(),
            processorDocumentTestJson,
            false
        );
        for (Map.Entry<String, Object> entry : processorDocumentTestMap.entrySet()) {
            String testCaseName = entry.getKey();
            Map<String, Object> metadata = (Map<String, Object>) entry.getValue();

            Map<String, Object> fieldMap = (Map<String, Object>) metadata.get("field_map");
            Map<String, Object> source = (Map<String, Object>) metadata.get("source");
            Map<String, Object> expectation = (Map<String, Object>) metadata.get("expectation");
            try {
                ProcessorDocumentUtils.validateMapTypeValue(
                    "field_map",
                    source,
                    fieldMap,
                    "test_index",
                    clusterService,
                    environment,
                    false
                );
            } catch (Exception e) {
                if (expectation != null) {
                    if (expectation.containsKey("type")) {
                        assertEquals("test case: " + testCaseName + " failed", expectation.get("type"), e.getClass().getSimpleName());
                    }
                    if (expectation.containsKey("message")) {
                        assertEquals("test case: " + testCaseName + " failed", expectation.get("message"), e.getMessage());
                    }
                } else {
                    fail("test case: " + testCaseName + " failed: " + e.getMessage());
                }
            }
        }
    }

    public void testUnflatten_withSimpleDotNotation_thenSuccess() {
        Map<String, Object> input = Map.of("a.b", "c");

        Map<String, Object> nested = Map.of("b", "c");
        Map<String, Object> expected = Map.of("a", nested);

        Map<String, Object> result = ProcessorDocumentUtils.unflattenJson(input);
        assertEquals(expected, result);
    }

    public void testUnflatten_withSimpleNoDot_thenSuccess() {
        Map<String, Object> nestedA = Map.of("b", "c");
        Map<String, Object> input = Map.of("a", nestedA);

        Map<String, Object> result = ProcessorDocumentUtils.unflattenJson(input);
        assertEquals(input, result);
    }

    public void testUnflatten_withMultipleDotNotation_thenSuccess() {
        Map<String, Object> input = Map.of("a.b.c", "d", "a.b.e", "f", "x.y", "z");

        Map<String, Object> nestedAB = Map.of("c", "d", "e", "f");
        Map<String, Object> nestedA = Map.of("b", nestedAB);
        Map<String, Object> nestedX = Map.of("y", "z");

        Map<String, Object> expected = Map.of("a", nestedA, "x", nestedX);

        Map<String, Object> result = ProcessorDocumentUtils.unflattenJson(input);
        assertEquals(expected, result);
    }

    public void testUnflatten_withList_thenSuccess() {
        Map<String, Object> map1 = Map.of("b.c", "d");
        Map<String, Object> map2 = Map.of("b.c", "e");
        List<Map<String, Object>> list = Arrays.asList(map1, map2);
        Map<String, Object> input = Map.of("a", list);

        Map<String, Object> nestedB1 = Map.of("c", "d");
        Map<String, Object> expectedMap1 = Map.of("b", nestedB1);
        Map<String, Object> nestedB2 = Map.of("c", "e");
        Map<String, Object> expectedMap2 = Map.of("b", nestedB2);

        List<Map<String, Object>> expectedList = Arrays.asList(expectedMap1, expectedMap2);

        Map<String, Object> expected = Map.of("a", expectedList);

        Map<String, Object> result = ProcessorDocumentUtils.unflattenJson(input);
        assertEquals(expected, result);
    }

    public void testUnflatten_withListOfObject_thenSuccess() {
        Map<String, Object> map1 = Map.of("b.c", "d", "f", "h");
        Map<String, Object> map2 = Map.of("b.c", "e", "f", "i");
        List<Map<String, Object>> list = Arrays.asList(map1, map2);
        Map<String, Object> input = Map.of("a", list);

        Map<String, Object> nestedB1 = Map.of("c", "d");
        Map<String, Object> expectedMap1 = Map.of("b", nestedB1, "f", "h");
        Map<String, Object> nestedB2 = Map.of("c", "e");
        Map<String, Object> expectedMap2 = Map.of("b", nestedB2, "f", "i");

        List<Map<String, Object>> expectedList = Arrays.asList(expectedMap1, expectedMap2);

        Map<String, Object> expected = Map.of("a", expectedList);

        Map<String, Object> result = ProcessorDocumentUtils.unflattenJson(input);
        assertEquals(expected, result);
    }

    public void testUnflatten_withMixedContent_thenSuccess() {
        Map<String, Object> input = Map.of("a.b", "c", "d", "e", "f.g.h", "i");

        Map<String, Object> nestedA = Map.of("b", "c");
        Map<String, Object> nestedG = Map.of("h", "i");
        Map<String, Object> nestedF = Map.of("g", nestedG);
        Map<String, Object> expected = Map.of("a", nestedA, "d", "e", "f", nestedF);

        Map<String, Object> result = ProcessorDocumentUtils.unflattenJson(input);
        assertEquals(expected, result);
    }

    public void testUnflatten_wthEmptyMap_thenSuccess() {
        Map<String, Object> result = ProcessorDocumentUtils.unflattenJson(Map.of());
        assertTrue(result.isEmpty());
    }

    public void testUnflatten_withNullValue_thenSuccess() {
        Map<String, Object> input = new HashMap<>();
        input.put("a.b", null);
        Map<String, Object> nested = new HashMap<>();
        nested.put("b", null);
        Map<String, Object> expected = Map.of("a", nested);

        Map<String, Object> result = ProcessorDocumentUtils.unflattenJson(input);
        assertEquals(expected, result);
    }

    public void testUnflatten_withNestedListWithMultipleLevels_thenSuccess() {
        Map<String, Object> map1 = Map.of("b.c.d", "e");
        Map<String, Object> map2 = Map.of("b.c.f", "g");
        List<Map<String, Object>> outerList = Arrays.asList(map1, map2);

        Map<String, Object> input = Map.of("a", outerList);

        Map<String, Object> nestedC1 = Map.of("d", "e");
        Map<String, Object> nestedB1 = Map.of("c", nestedC1);
        Map<String, Object> expectedMap1 = Map.of("b", nestedB1);
        Map<String, Object> nestedC2 = Map.of("f", "g");
        Map<String, Object> nestedB2 = Map.of("c", nestedC2);
        Map<String, Object> expectedMap2 = Map.of("b", nestedB2);
        List<Map<String, Object>> expectedOuterList = Arrays.asList(expectedMap1, expectedMap2);

        Map<String, Object> expected = Map.of("a", expectedOuterList);

        Map<String, Object> result = ProcessorDocumentUtils.unflattenJson(input);
        assertEquals(expected, result);
    }

    public void testUnflatten_withNullInput_thenFail() {
        IllegalArgumentException illegalArgumentException = assertThrows(
            IllegalArgumentException.class,
            () -> ProcessorDocumentUtils.unflattenJson(null)
        );

        assertEquals("originalJsonMap cannot be null", illegalArgumentException.getMessage());
    }

    public void testUnflatten_withSimpleField_withLeadingDots_thenFail() {
        String fieldName = ".a.b.c";
        Map<String, Object> input = Map.of(fieldName, "d");
        testUnflatten_withInvalidUsageOfDots_thenFail(fieldName, input);
    }

    public void testUnflatten_withSimpleField_withInBetweenMultiDots_thenFail() {
        String fieldName = "a..b.c";
        Map<String, Object> input = Map.of(fieldName, "d");
        testUnflatten_withInvalidUsageOfDots_thenFail(fieldName, input);
    }

    public void testUnflatten_withSimpleField_withTrailingDots_thenFail() {
        String fieldName = "a.b.c.";
        Map<String, Object> input = Map.of(fieldName, "d");
        testUnflatten_withInvalidUsageOfDots_thenFail(fieldName, input);
    }

    public void testUnflatten_withNestedField_withTrailingDots_thenFail() {
        String fieldName = "b.c.d.";
        Map<String, Object> input = Map.of("a", Map.of(fieldName, "e"));
        testUnflatten_withInvalidUsageOfDots_thenFail(fieldName, input);
    }

    private void testUnflatten_withInvalidUsageOfDots_thenFail(String fieldName, Map<String, Object> input) {
        IllegalArgumentException illegalArgumentException = assertThrows(
            IllegalArgumentException.class,
            () -> ProcessorDocumentUtils.unflattenJson(input)
        );
        assert (illegalArgumentException.getMessage()
            .contains(String.format(Locale.ROOT, "Field name '%s' contains invalid dot usage", fieldName)));
    }

    public void testFlattenAndFlip_withMultipleLevelsSeparatedByDots_thenSuccess() {
        /*
         * parent
         *    child_level1
         *           child_leve1_text_field: child_level2.text_field_knn
         * */
        Map<String, Object> childLevel1 = Map.of("child_leve1_text_field", "child_level2.text_field_knn");
        Map<String, Object> parentMap = Map.of("child_level1", childLevel1);
        Map<String, Object> nestedMap = Map.of("parent", parentMap);

        Map<String, String> expected = Map.of(
            "parent.child_level1.child_level2.text_field_knn",
            "parent.child_level1.child_leve1_text_field"
        );

        Map<String, String> actual = ProcessorDocumentUtils.flattenAndFlip(nestedMap);
        assertEquals(expected, actual);
    }

    public void testFlattenAndFlip_withMultipleLevelsWithNestedMaps_thenSuccess() {
        /*
        * parent
        *    child_level1
        *       child_level2
        *           child_level2_text: child_level2_knn
        *       child_level3
        *           child_level4:
        *               child_level4_text:child_level4_knn
        * */
        Map<String, String> childLevel4 = Map.of("child_level4_text", "child_level4_knn");
        Map<String, Object> childLevel3 = Map.of("child_level4", childLevel4);
        Map<String, String> childLevel2 = Map.of("child_level2_text", "child_level2_knn");
        Map<String, Object> childLevel1 = Map.of("child_level2", childLevel2, "child_level3", childLevel3);
        Map<String, Object> parentMap = Map.of("child_level1", childLevel1);
        Map<String, Object> nestedMap = Map.of("parent", parentMap);

        Map<String, String> expected = Map.of(
            "parent.child_level1.child_level2.child_level2_knn",
            "parent.child_level1.child_level2.child_level2_text",
            "parent.child_level1.child_level3.child_level4.child_level4_knn",
            "parent.child_level1.child_level3.child_level4.child_level4_text"
        );

        Map<String, String> actual = ProcessorDocumentUtils.flattenAndFlip(nestedMap);
        assertEquals(expected, actual);
    }
}
