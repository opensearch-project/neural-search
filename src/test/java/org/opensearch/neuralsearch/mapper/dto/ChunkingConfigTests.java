/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.mapper.dto;

import org.opensearch.common.xcontent.XContentType;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.index.mapper.MapperParsingException;
import org.opensearch.neuralsearch.processor.chunker.ChunkerValidatorFactory;
import org.opensearch.neuralsearch.processor.chunker.DelimiterChunker;
import org.opensearch.neuralsearch.processor.chunker.FixedTokenLengthChunker;
import org.opensearch.neuralsearch.util.TestUtils;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import static org.opensearch.neuralsearch.constants.SemanticFieldConstants.CHUNKING;
import static org.opensearch.neuralsearch.mapper.dto.ChunkingConfig.ALGORITHM_FIELD;
import static org.opensearch.neuralsearch.mapper.dto.ChunkingConfig.PARAMETERS_FIELD;
import static org.opensearch.neuralsearch.processor.chunker.FixedTokenLengthChunker.TOKEN_LIMIT_FIELD;

public class ChunkingConfigTests extends OpenSearchTestCase {
    private final ChunkerValidatorFactory chunkerValidatorFactory = new ChunkerValidatorFactory();

    public void testConstruct_whenValidBooleanConfig_thenSuccess() {
        final ChunkingConfig parsedChunkingConfig = new ChunkingConfig(CHUNKING, true, chunkerValidatorFactory);

        assertTrue(parsedChunkingConfig.isEnabled());
    }

    public void testConstruct_whenValidListConfig_thenSuccess() {
        final Map<String, Object> chunker1 = Map.of(
            ALGORITHM_FIELD,
            FixedTokenLengthChunker.ALGORITHM_NAME,
            PARAMETERS_FIELD,
            Map.of(TOKEN_LIMIT_FIELD, 100)
        );
        final Map<String, Object> chunker2 = Map.of(ALGORITHM_FIELD, DelimiterChunker.ALGORITHM_NAME);
        final List<Map<String, Object>> chunkerConfigs = List.of(chunker1, chunker2);
        final ChunkingConfig parsedChunkingConfig = new ChunkingConfig(CHUNKING, chunkerConfigs, chunkerValidatorFactory);

        assertTrue(parsedChunkingConfig.isEnabled());
        assertEquals(chunkerConfigs, parsedChunkingConfig.getConfigs());
    }

    public void testConstruct_whenUnsupportedParameters_thenException() {
        final List<Map<String, Object>> chunkerConfigs = List.of(Map.of("invalid", "invalid"));
        final MapperParsingException exception = assertThrows(
            MapperParsingException.class,
            () -> new ChunkingConfig(CHUNKING, chunkerConfigs, chunkerValidatorFactory)
        );

        final String expectedError = "[chunking] does not support parameters [[invalid]]";
        assertEquals(expectedError, exception.getMessage());
    }

    public void testConstruct_whenInvalidAlgorithmParameters_thenException() {
        final Map<String, Object> chunker1 = Map.of(
            ALGORITHM_FIELD,
            FixedTokenLengthChunker.ALGORITHM_NAME,
            PARAMETERS_FIELD,
            Map.of(TOKEN_LIMIT_FIELD, "invalid")
        );
        final List<Map<String, Object>> chunkerConfigs = List.of(chunker1);
        final IllegalArgumentException exception = assertThrows(
            IllegalArgumentException.class,
            () -> new ChunkingConfig(CHUNKING, chunkerConfigs, chunkerValidatorFactory)
        );

        final String expectedError = "Parameter [token_limit] must be of java.lang.Integer type";
        assertEquals(expectedError, exception.getMessage());
    }

    public void testConstruct_whenInvalidAlgorithm_thenException() {
        final Map<String, Object> chunker1 = Map.of(ALGORITHM_FIELD, "invalid");
        final List<Map<String, Object>> chunkerConfigs = List.of(chunker1);
        final MapperParsingException exception = assertThrows(
            MapperParsingException.class,
            () -> new ChunkingConfig(CHUNKING, chunkerConfigs, chunkerValidatorFactory)
        );

        assertTrue(exception.getMessage().contains("[algorithm] must be"));
    }

    public void testConstruct_whenValidBooleanFieldConfig_thenSuccess() {
        final ChunkingConfig parsedChunkingConfig = new ChunkingConfig(Map.of(CHUNKING, true));

        assertTrue(parsedChunkingConfig.isEnabled());
    }

    public void testConstruct_whenValidListFieldConfig_thenSuccess() {
        final Map<String, Object> chunker1 = Map.of(
            ALGORITHM_FIELD,
            FixedTokenLengthChunker.ALGORITHM_NAME,
            PARAMETERS_FIELD,
            Map.of(TOKEN_LIMIT_FIELD, 100)
        );
        final Map<String, Object> chunker2 = Map.of(ALGORITHM_FIELD, DelimiterChunker.ALGORITHM_NAME);
        final List<Map<String, Object>> chunkerConfigs = List.of(chunker1, chunker2);
        final ChunkingConfig parsedChunkingConfig = new ChunkingConfig(Map.of(CHUNKING, chunkerConfigs));

        assertTrue(parsedChunkingConfig.isEnabled());
        assertEquals(chunkerConfigs, parsedChunkingConfig.getConfigs());
    }

    public void testToXContent_whenBooleanConfig_thenBooleanField() throws IOException {
        final ChunkingConfig chunkingConfig = ChunkingConfig.builder().enabled(true).build();
        XContentBuilder builder = XContentBuilder.builder(XContentType.JSON.xContent());
        builder.startObject();
        chunkingConfig.toXContent(builder, CHUNKING);
        builder.endObject();

        final Map<String, Object> builderMap = TestUtils.xContentBuilderToMap(builder);
        assertEquals(true, builderMap.get(CHUNKING));
    }

    public void testToXContent_whenListConfig_thenBooleanField() throws IOException {
        final Map<String, Object> chunker1 = Map.of(
            ALGORITHM_FIELD,
            FixedTokenLengthChunker.ALGORITHM_NAME,
            PARAMETERS_FIELD,
            Map.of(TOKEN_LIMIT_FIELD, 100)
        );
        final Map<String, Object> chunker2 = Map.of(ALGORITHM_FIELD, DelimiterChunker.ALGORITHM_NAME);
        final List<Map<String, Object>> chunkerConfigs = List.of(chunker1, chunker2);
        final ChunkingConfig chunkingConfig = ChunkingConfig.builder().enabled(true).configs(chunkerConfigs).build();
        XContentBuilder builder = XContentBuilder.builder(XContentType.JSON.xContent());
        builder.startObject();
        chunkingConfig.toXContent(builder, CHUNKING);
        builder.endObject();

        final Map<String, Object> builderMap = TestUtils.xContentBuilderToMap(builder);
        assertEquals(chunkerConfigs, builderMap.get(CHUNKING));
    }

    public void testToString_whenBooleanConfig() {
        final ChunkingConfig chunkingConfig = ChunkingConfig.builder().enabled(true).build();

        assertEquals("true", chunkingConfig.toString());
    }

    public void testToString_whenListConfig() {
        final Map<String, Object> chunker1 = Map.of(ALGORITHM_FIELD, FixedTokenLengthChunker.ALGORITHM_NAME);
        final Map<String, Object> chunker2 = Map.of(ALGORITHM_FIELD, DelimiterChunker.ALGORITHM_NAME);
        final List<Map<String, Object>> chunkerConfigs = List.of(chunker1, chunker2);
        final ChunkingConfig chunkingConfig = ChunkingConfig.builder().enabled(true).configs(chunkerConfigs).build();

        assertEquals("[{algorithm=fixed_token_length}, {algorithm=delimiter}]", chunkingConfig.toString());
    }
}
