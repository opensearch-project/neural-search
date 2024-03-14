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
import java.util.Locale;
import java.util.Map;
import static java.util.Collections.singletonList;
import static java.util.Collections.singletonMap;
import static org.opensearch.neuralsearch.processor.chunker.FixedTokenLengthChunker.ALGORITHM_NAME;
import static org.opensearch.neuralsearch.processor.chunker.FixedTokenLengthChunker.ANALYSIS_REGISTRY_FIELD;
import static org.opensearch.neuralsearch.processor.chunker.FixedTokenLengthChunker.TOKEN_LIMIT_FIELD;
import static org.opensearch.neuralsearch.processor.chunker.FixedTokenLengthChunker.OVERLAP_RATE_FIELD;
import static org.opensearch.neuralsearch.processor.chunker.FixedTokenLengthChunker.TOKENIZER_FIELD;
import static org.opensearch.neuralsearch.processor.chunker.FixedTokenLengthChunker.MAX_TOKEN_COUNT_FIELD;

public class FixedTokenLengthChunkerTests extends OpenSearchTestCase {

    private FixedTokenLengthChunker fixedTokenLengthChunker;

    @Before
    public void setup() {
        fixedTokenLengthChunker = createFixedTokenLengthChunker(Map.of());
    }

    @SneakyThrows
    public FixedTokenLengthChunker createFixedTokenLengthChunker(Map<String, Object> parameters) {
        Map<String, Object> nonRuntimeParameters = new HashMap<>(parameters);
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
        nonRuntimeParameters.put(ANALYSIS_REGISTRY_FIELD, analysisRegistry);
        return new FixedTokenLengthChunker(nonRuntimeParameters);
    }

    public void testValidateAndParseParameters_whenNoParams_thenSuccessful() {
        fixedTokenLengthChunker.validateParameters(Map.of());
    }

    public void testValidateAndParseParameters_whenIllegalTokenLimitType_thenFail() {
        Map<String, Object> parameters = new HashMap<>();
        parameters.put(TOKEN_LIMIT_FIELD, "invalid token limit");
        IllegalArgumentException illegalArgumentException = assertThrows(
            IllegalArgumentException.class,
            () -> fixedTokenLengthChunker.validateParameters(parameters)
        );
        assertEquals(
            String.format(Locale.ROOT, "Parameter [%s] must be of %s type", TOKEN_LIMIT_FIELD, Number.class.getName()),
            illegalArgumentException.getMessage()
        );
    }

    public void testValidateAndParseParameters_whenIllegalTokenLimitValue_thenFail() {
        Map<String, Object> parameters = new HashMap<>();
        parameters.put(TOKEN_LIMIT_FIELD, -1);
        IllegalArgumentException illegalArgumentException = assertThrows(
            IllegalArgumentException.class,
            () -> fixedTokenLengthChunker.validateParameters(parameters)
        );
        assertEquals(
            String.format(Locale.ROOT, "Parameter [%s] must be positive.", TOKEN_LIMIT_FIELD),
            illegalArgumentException.getMessage()
        );
    }

    public void testValidateAndParseParameters_whenIllegalOverlapRateType_thenFail() {
        Map<String, Object> parameters = new HashMap<>();
        parameters.put(OVERLAP_RATE_FIELD, "invalid overlap rate");
        IllegalArgumentException illegalArgumentException = assertThrows(
            IllegalArgumentException.class,
            () -> fixedTokenLengthChunker.validateParameters(parameters)
        );
        assertEquals(
            String.format(Locale.ROOT, "Parameter [%s] must be of %s type", OVERLAP_RATE_FIELD, Number.class.getName()),
            illegalArgumentException.getMessage()
        );
    }

    public void testValidateAndParseParameters_whenIllegalOverlapRateValue_thenFail() {
        Map<String, Object> parameters = new HashMap<>();
        parameters.put(OVERLAP_RATE_FIELD, 0.6);
        IllegalArgumentException illegalArgumentException = assertThrows(
            IllegalArgumentException.class,
            () -> fixedTokenLengthChunker.validateParameters(parameters)
        );
        assertEquals(
            String.format(Locale.ROOT, "Parameter [%s] must be between %s and %s", OVERLAP_RATE_FIELD, 0.0, 0.5),
            illegalArgumentException.getMessage()
        );
    }

    public void testValidateAndParseParameters_whenIllegalTokenizerType_thenFail() {
        Map<String, Object> parameters = new HashMap<>();
        parameters.put(TOKENIZER_FIELD, 111);
        IllegalArgumentException illegalArgumentException = assertThrows(
            IllegalArgumentException.class,
            () -> fixedTokenLengthChunker.validateParameters(parameters)
        );
        assertEquals(
            String.format(Locale.ROOT, "Parameter [%s] must be of %s type", TOKENIZER_FIELD, String.class.getName()),
            illegalArgumentException.getMessage()
        );
    }

    public void testValidateAndParseParameters_whenUnsupportedTokenizer_thenFail() {
        String ngramTokenizer = "ngram";
        Map<String, Object> parameters = Map.of(TOKENIZER_FIELD, ngramTokenizer);
        IllegalArgumentException illegalArgumentException = assertThrows(
            IllegalArgumentException.class,
            () -> fixedTokenLengthChunker.validateParameters(parameters)
        );
        assert (illegalArgumentException.getMessage()
            .contains(String.format(Locale.ROOT, "tokenizer [%s] is not supported for [%s] algorithm.", ngramTokenizer, ALGORITHM_NAME)));
    }

    public void testChunk_withTokenLimit_10() {
        Map<String, Object> parameters = new HashMap<>();
        parameters.put(TOKEN_LIMIT_FIELD, 10);
        parameters.put(TOKENIZER_FIELD, "standard");
        FixedTokenLengthChunker fixedTokenLengthChunker = createFixedTokenLengthChunker(parameters);
        Map<String, Object> runtimeParameters = new HashMap<>();
        runtimeParameters.put(MAX_TOKEN_COUNT_FIELD, 10000);
        String content =
            "This is an example document to be chunked. The document contains a single paragraph, two sentences and 24 tokens by standard tokenizer in OpenSearch.";
        List<String> passages = fixedTokenLengthChunker.chunk(content, runtimeParameters);
        List<String> expectedPassages = new ArrayList<>();
        expectedPassages.add("This is an example document to be chunked. The document ");
        expectedPassages.add("contains a single paragraph, two sentences and 24 tokens by ");
        expectedPassages.add("standard tokenizer in OpenSearch.");
        assertEquals(expectedPassages, passages);
    }

    public void testChunk_withTokenLimit_20() {
        Map<String, Object> parameters = new HashMap<>();
        parameters.put(TOKEN_LIMIT_FIELD, 20);
        parameters.put(TOKENIZER_FIELD, "standard");
        FixedTokenLengthChunker fixedTokenLengthChunker = createFixedTokenLengthChunker(parameters);
        Map<String, Object> runtimeParameters = new HashMap<>();
        runtimeParameters.put(MAX_TOKEN_COUNT_FIELD, 10000);
        String content =
            "This is an example document to be chunked. The document contains a single paragraph, two sentences and 24 tokens by standard tokenizer in OpenSearch.";
        List<String> passages = fixedTokenLengthChunker.chunk(content, runtimeParameters);
        List<String> expectedPassages = new ArrayList<>();
        expectedPassages.add(
            "This is an example document to be chunked. The document contains a single paragraph, two sentences and 24 tokens by "
        );
        expectedPassages.add("standard tokenizer in OpenSearch.");
        assertEquals(expectedPassages, passages);
    }

    public void testChunk_withOverlapRate_half() {
        Map<String, Object> parameters = new HashMap<>();
        parameters.put(TOKEN_LIMIT_FIELD, 10);
        parameters.put(OVERLAP_RATE_FIELD, 0.5);
        FixedTokenLengthChunker fixedTokenLengthChunker = createFixedTokenLengthChunker(parameters);
        String content =
            "This is an example document to be chunked. The document contains a single paragraph, two sentences and 24 tokens by standard tokenizer in OpenSearch.";
        List<String> passages = fixedTokenLengthChunker.chunk(content, Map.of());
        List<String> expectedPassages = new ArrayList<>();
        expectedPassages.add("This is an example document to be chunked. The document ");
        expectedPassages.add("to be chunked. The document contains a single paragraph, two ");
        expectedPassages.add("contains a single paragraph, two sentences and 24 tokens by ");
        expectedPassages.add("sentences and 24 tokens by standard tokenizer in OpenSearch.");
        assertEquals(expectedPassages, passages);
    }
}
