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

    public void test_with_different_configurations() throws URISyntaxException, IOException {
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
                    1,
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

}
