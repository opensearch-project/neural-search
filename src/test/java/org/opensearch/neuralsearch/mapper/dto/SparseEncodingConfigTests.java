/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.mapper.dto;

import org.opensearch.index.mapper.MapperParsingException;
import org.opensearch.neuralsearch.util.prune.PruneType;
import org.opensearch.neuralsearch.util.prune.PruneUtils;
import org.opensearch.test.OpenSearchTestCase;

import java.util.HashMap;
import java.util.Map;

import static org.opensearch.neuralsearch.constants.SemanticFieldConstants.SPARSE_ENCODING_CONFIG;

public class SparseEncodingConfigTests extends OpenSearchTestCase {
    private final float FLOAT_EQUAL_DELTA = 1e-6f;

    public void testConstruct_whenValidConfig_thenSuccess() {
        final Map<String, Object> config = new HashMap<>();
        config.put(PruneUtils.PRUNE_TYPE_FIELD, PruneType.MAX_RATIO.getValue());
        config.put(PruneUtils.PRUNE_RATIO_FIELD, 0.5);
        final SparseEncodingConfig parsedConfig = new SparseEncodingConfig(SPARSE_ENCODING_CONFIG, config);

        assertEquals(PruneType.MAX_RATIO, parsedConfig.getPruneType());
        assertEquals(0.5f, parsedConfig.getPruneRatio(), FLOAT_EQUAL_DELTA);
    }

    public void testConstruct_whenPruneTypeNone_thenSuccess() {
        final Map<String, Object> config = new HashMap<>();
        config.put(PruneUtils.PRUNE_TYPE_FIELD, PruneType.NONE.getValue());
        final SparseEncodingConfig parsedConfig = new SparseEncodingConfig(SPARSE_ENCODING_CONFIG, config);

        assertEquals(PruneType.NONE, parsedConfig.getPruneType());
        assertNull(parsedConfig.getPruneRatio());
    }

    public void testConstruct_whenEmptyConfig_thenEmpty() {
        final Map<String, Object> config = new HashMap<>();
        final SparseEncodingConfig parsedConfig = new SparseEncodingConfig(SPARSE_ENCODING_CONFIG, config);

        assertNull(parsedConfig.getPruneType());
        assertNull(parsedConfig.getPruneRatio());
    }

    public void testConstruct_whenConfigNotAMap_thenException() {
        final MapperParsingException exception = assertThrows(
            MapperParsingException.class,
            () -> new SparseEncodingConfig(SPARSE_ENCODING_CONFIG, "invalid")
        );

        final String expectedError = "[sparse_encoding_config] must be a Map";
        assertEquals(expectedError, exception.getMessage());
    }

    public void testConstruct_whenConfigWithUnsupportedParameters_thenException() {
        final Map<String, Object> config = new HashMap<>();
        config.put("invalid", "dummy");
        final MapperParsingException exception = assertThrows(
            MapperParsingException.class,
            () -> new SparseEncodingConfig(SPARSE_ENCODING_CONFIG, config)
        );

        final String expectedError = "Unsupported parameters invalid in sparse_encoding_config";
        assertEquals(expectedError, exception.getMessage());
    }

    public void testConstruct_whenConfigMissingPruneRatio_thenException() {
        final Map<String, Object> config = new HashMap<>();
        config.put(PruneUtils.PRUNE_TYPE_FIELD, PruneType.MAX_RATIO.getValue());
        final MapperParsingException exception = assertThrows(
            MapperParsingException.class,
            () -> new SparseEncodingConfig(SPARSE_ENCODING_CONFIG, config)
        );

        final String expectedError = "prune_ratio is required when prune_type is defined and not none";
        assertEquals(expectedError, exception.getMessage());
    }

    public void testConstruct_whenConfigMissingPruneType_thenException() {
        final Map<String, Object> config = new HashMap<>();
        config.put(PruneUtils.PRUNE_RATIO_FIELD, 0.5);
        final MapperParsingException exception = assertThrows(
            MapperParsingException.class,
            () -> new SparseEncodingConfig(SPARSE_ENCODING_CONFIG, config)
        );

        final String expectedError = "prune_ratio should not be defined when prune_type is none or null";
        assertEquals(expectedError, exception.getMessage());
    }

    public void testConstruct_whenConfigWithInvaliPruneCombo_thenException() {
        final Map<String, Object> config = new HashMap<>();
        config.put(PruneUtils.PRUNE_TYPE_FIELD, PruneType.MAX_RATIO.getValue());
        config.put(PruneUtils.PRUNE_RATIO_FIELD, 2);
        final MapperParsingException exception = assertThrows(
            MapperParsingException.class,
            () -> new SparseEncodingConfig(SPARSE_ENCODING_CONFIG, config)
        );

        final String expectedError = "Illegal prune_ratio 2.000000 for prune_type: max_ratio. prune_ratio should be in the range [0, 1).";
        assertEquals(expectedError, exception.getMessage());
    }

    public void testConstruct_whenValidSemanticConfig_thenSuccess() {
        final Map<String, Object> sparseEncodingConfig = new HashMap<>();
        sparseEncodingConfig.put(PruneUtils.PRUNE_TYPE_FIELD, PruneType.MAX_RATIO.getValue());
        sparseEncodingConfig.put(PruneUtils.PRUNE_RATIO_FIELD, 0.5);
        final Map<String, Object> semanticConfig = Map.of(SPARSE_ENCODING_CONFIG, sparseEncodingConfig);
        final SparseEncodingConfig parsedConfig = new SparseEncodingConfig(semanticConfig);

        assertEquals(PruneType.MAX_RATIO, parsedConfig.getPruneType());
        assertEquals(0.5f, parsedConfig.getPruneRatio(), FLOAT_EQUAL_DELTA);
    }
}
