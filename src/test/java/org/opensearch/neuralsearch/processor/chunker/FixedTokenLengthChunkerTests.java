/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.processor.chunker;

import lombok.SneakyThrows;
import org.apache.lucene.tests.analysis.MockTokenizer;
import org.junit.Before;
import org.opensearch.common.settings.Settings;
import org.opensearch.env.Environment;
import org.opensearch.env.TestEnvironment;
import org.opensearch.index.analysis.TokenizerFactory;
import org.opensearch.index.analysis.AnalysisRegistry;
import org.opensearch.indices.analysis.AnalysisModule;
import org.opensearch.plugins.AnalysisPlugin;
import org.opensearch.test.OpenSearchTestCase;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static java.util.Collections.singletonList;
import static java.util.Collections.singletonMap;
import static org.opensearch.neuralsearch.processor.chunker.FixedTokenLengthChunker.TOKENIZER_FIELD;
import static org.opensearch.neuralsearch.processor.chunker.FixedTokenLengthChunker.TOKEN_LIMIT_FIELD;
import static org.opensearch.neuralsearch.processor.chunker.FixedTokenLengthChunker.OVERLAP_RATE_FIELD;
import static org.opensearch.neuralsearch.processor.chunker.FixedTokenLengthChunker.MAX_TOKEN_COUNT_FIELD;

public class FixedTokenLengthChunkerTests extends OpenSearchTestCase {

    private org.opensearch.neuralsearch.processor.chunker.FixedTokenLengthChunker FixedTokenLengthChunker;

    @Before
    @SneakyThrows
    public void setup() {
        Settings settings = Settings.builder().put(Environment.PATH_HOME_SETTING.getKey(), createTempDir().toString()).build();
        Environment environment = TestEnvironment.newEnvironment(settings);
        AnalysisPlugin plugin = new AnalysisPlugin() {

            @Override
            public Map<String, AnalysisModule.AnalysisProvider<TokenizerFactory>> getTokenizers() {
                return singletonMap(
                    "keyword",
                    (indexSettings, environment, name, settings) -> TokenizerFactory.newFactory(
                        name,
                        () -> new MockTokenizer(MockTokenizer.KEYWORD, false)
                    )
                );
            }
        };
        AnalysisRegistry analysisRegistry = new AnalysisModule(environment, singletonList(plugin)).getAnalysisRegistry();
        FixedTokenLengthChunker = new FixedTokenLengthChunker(analysisRegistry);
    }

    public void testValidateParameters_whenNoParams_thenSuccessful() {
        Map<String, Object> parameters = new HashMap<>();
        FixedTokenLengthChunker.validateParameters(parameters);
    }

    public void testValidateParameters_whenIllegalTokenLimitType_thenFail() {
        Map<String, Object> parameters = new HashMap<>();
        parameters.put(TOKEN_LIMIT_FIELD, "invalid token limit");
        IllegalArgumentException illegalArgumentException = assertThrows(
            IllegalArgumentException.class,
            () -> FixedTokenLengthChunker.validateParameters(parameters)
        );
        assertEquals(
            "fixed length parameter [" + TOKEN_LIMIT_FIELD + "] cannot be cast to [" + Number.class.getName() + "]",
            illegalArgumentException.getMessage()
        );
    }

    public void testValidateParameters_whenIllegalTokenLimitValue_thenFail() {
        Map<String, Object> parameters = new HashMap<>();
        parameters.put(TOKEN_LIMIT_FIELD, -1);
        IllegalArgumentException illegalArgumentException = assertThrows(
            IllegalArgumentException.class,
            () -> FixedTokenLengthChunker.validateParameters(parameters)
        );
        assertEquals("fixed length parameter [" + TOKEN_LIMIT_FIELD + "] must be positive", illegalArgumentException.getMessage());
    }

    public void testValidateParameters_whenIllegalOverlapRateType_thenFail() {
        Map<String, Object> parameters = new HashMap<>();
        parameters.put(OVERLAP_RATE_FIELD, "invalid overlap rate");
        IllegalArgumentException illegalArgumentException = assertThrows(
            IllegalArgumentException.class,
            () -> FixedTokenLengthChunker.validateParameters(parameters)
        );
        assertEquals(
            "fixed length parameter [" + OVERLAP_RATE_FIELD + "] cannot be cast to [" + Number.class.getName() + "]",
            illegalArgumentException.getMessage()
        );
    }

    public void testValidateParameters_whenIllegalOverlapRateValue_thenFail() {
        Map<String, Object> parameters = new HashMap<>();
        parameters.put(OVERLAP_RATE_FIELD, 0.6);
        IllegalArgumentException illegalArgumentException = assertThrows(
            IllegalArgumentException.class,
            () -> FixedTokenLengthChunker.validateParameters(parameters)
        );
        assertEquals(
            "fixed length parameter [" + OVERLAP_RATE_FIELD + "] must be between 0 and 0.5",
            illegalArgumentException.getMessage()
        );
    }

    public void testValidateParameters_whenIllegalTokenizerType_thenFail() {
        Map<String, Object> parameters = new HashMap<>();
        parameters.put(TOKENIZER_FIELD, 111);
        IllegalArgumentException illegalArgumentException = assertThrows(
            IllegalArgumentException.class,
            () -> FixedTokenLengthChunker.validateParameters(parameters)
        );
        assertEquals(
            "fixed length parameter [" + TOKENIZER_FIELD + "] cannot be cast to [" + String.class.getName() + "]",
            illegalArgumentException.getMessage()
        );
    }

    public void testChunk_withTokenLimit_10() {
        Map<String, Object> parameters = new HashMap<>();
        parameters.put(TOKEN_LIMIT_FIELD, 10);
        parameters.put(TOKENIZER_FIELD, "standard");
        parameters.put(MAX_TOKEN_COUNT_FIELD, 10000);
        String content =
            "This is an example document to be chunked. The document contains a single paragraph, two sentences and 24 tokens by standard tokenizer in OpenSearch.";
        List<String> passages = FixedTokenLengthChunker.chunk(content, parameters);
        List<String> expectedPassages = new ArrayList<>();
        expectedPassages.add("This is an example document to be chunked. The document");
        expectedPassages.add("contains a single paragraph, two sentences and 24 tokens by");
        expectedPassages.add("standard tokenizer in OpenSearch");
        assertEquals(expectedPassages, passages);
    }

    public void testChunk_withTokenLimit_20() {
        Map<String, Object> parameters = new HashMap<>();
        parameters.put(TOKEN_LIMIT_FIELD, 20);
        parameters.put(TOKENIZER_FIELD, "standard");
        parameters.put(MAX_TOKEN_COUNT_FIELD, 10000);
        String content =
            "This is an example document to be chunked. The document contains a single paragraph, two sentences and 24 tokens by standard tokenizer in OpenSearch.";
        List<String> passages = FixedTokenLengthChunker.chunk(content, parameters);
        List<String> expectedPassages = new ArrayList<>();
        expectedPassages.add(
            "This is an example document to be chunked. The document contains a single paragraph, two sentences and 24 tokens by"
        );
        expectedPassages.add("standard tokenizer in OpenSearch");
        assertEquals(expectedPassages, passages);
    }

    public void testChunk_withOverlapRate_half() {
        Map<String, Object> parameters = new HashMap<>();
        parameters.put(TOKEN_LIMIT_FIELD, 10);
        parameters.put(OVERLAP_RATE_FIELD, 0.5);
        String content =
            "This is an example document to be chunked. The document contains a single paragraph, two sentences and 24 tokens by standard tokenizer in OpenSearch.";
        List<String> passages = FixedTokenLengthChunker.chunk(content, parameters);
        List<String> expectedPassages = new ArrayList<>();
        expectedPassages.add("This is an example document to be chunked. The document");
        expectedPassages.add("to be chunked. The document contains a single paragraph, two");
        expectedPassages.add("contains a single paragraph, two sentences and 24 tokens by");
        expectedPassages.add("sentences and 24 tokens by standard tokenizer in OpenSearch");
        assertEquals(expectedPassages, passages);
    }
}
