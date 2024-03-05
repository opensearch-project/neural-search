/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.processor;

import java.util.Map;
import java.util.Set;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.stream.IntStream;
import java.util.function.BiConsumer;

import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.index.IndexService;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.settings.Settings;
import org.opensearch.env.Environment;
import org.opensearch.index.analysis.AnalysisRegistry;
import org.opensearch.indices.IndicesService;
import org.opensearch.index.IndexSettings;
import org.opensearch.ingest.IngestDocument;
import org.opensearch.ingest.Processor;
import org.opensearch.neuralsearch.processor.chunker.ChunkerFactory;
import org.opensearch.neuralsearch.processor.chunker.IFieldChunker;
import org.opensearch.index.mapper.IndexFieldMapper;
import org.opensearch.neuralsearch.processor.chunker.FixedTokenLengthChunker;

import static org.opensearch.ingest.ConfigurationUtils.readMap;
import static org.opensearch.neuralsearch.processor.chunker.ChunkerFactory.DELIMITER_ALGORITHM;
import static org.opensearch.neuralsearch.processor.chunker.ChunkerFactory.FIXED_LENGTH_ALGORITHM;

public final class DocumentChunkingProcessor extends InferenceProcessor {

    public static final String TYPE = "chunking";

    public static final String FIELD_MAP_FIELD = "field_map";

    public static final String ALGORITHM_FIELD = "algorithm";

    private final Set<String> supportedChunkers = ChunkerFactory.getAllChunkers();

    private final Settings settings;

    private String chunkerType;

    private Map<String, Object> chunkerParameters;

    private final ClusterService clusterService;

    private final IndicesService indicesService;

    private final AnalysisRegistry analysisRegistry;

    public DocumentChunkingProcessor(
        String tag,
        String description,
        Map<String, Object> fieldMap,
        Map<String, Object> algorithmMap,
        Settings settings,
        ClusterService clusterService,
        IndicesService indicesService,
        AnalysisRegistry analysisRegistry,
        Environment environment,
        ProcessorInputValidator processorInputValidator
    ) {
        super(tag, description, TYPE, null, null, fieldMap, null, environment, processorInputValidator);
        validateAndParseAlgorithmMap(algorithmMap);
        this.settings = settings;
        this.clusterService = clusterService;
        this.indicesService = indicesService;
        this.analysisRegistry = analysisRegistry;
    }

    public String getType() {
        return TYPE;
    }

    private List<String> chunk(String content) {
        // assume that content is either a map, list or string
        IFieldChunker chunker = ChunkerFactory.create(chunkerType, analysisRegistry);
        return chunker.chunk(content, chunkerParameters);
    }

    @SuppressWarnings("unchecked")
    private void validateAndParseAlgorithmMap(Map<String, Object> algorithmMap) {
        if (algorithmMap.size() != 1) {
            throw new IllegalArgumentException(
                "Unable to create the processor as [" + ALGORITHM_FIELD + "] must contain and only contain 1 algorithm"
            );
        }

        for (Map.Entry<String, Object> algorithmEntry : algorithmMap.entrySet()) {
            String algorithmKey = algorithmEntry.getKey();
            Object algorithmValue = algorithmEntry.getValue();
            if (!supportedChunkers.contains(algorithmKey)) {
                throw new IllegalArgumentException(
                    "Unable to create the processor as chunker algorithm ["
                        + algorithmKey
                        + "] is not supported. Supported chunkers types are ["
                        + FIXED_LENGTH_ALGORITHM
                        + ", "
                        + DELIMITER_ALGORITHM
                        + "]"
                );
            }
            if (!(algorithmValue instanceof Map)) {
                throw new IllegalArgumentException(
                    "Unable to create the processor as [" + ALGORITHM_FIELD + "] cannot be cast to [" + Map.class.getName() + "]"
                );
            }
            this.chunkerType = algorithmKey;
            this.chunkerParameters = (Map<String, Object>) algorithmValue;
        }
    }

    @Override
    protected List<?> buildResultForListType(List<Object> sourceValue, List<?> results, IndexWrapper indexWrapper) {
        Object peek = sourceValue.get(0);
        if (peek instanceof String) {
            List<Object> keyToResult = new ArrayList<>();
            IntStream.range(0, sourceValue.size()).forEachOrdered(x -> keyToResult.add(results.get(indexWrapper.index++)));
            return keyToResult;
        } else {
            List<List<Object>> keyToResult = new ArrayList<>();
            for (Object nestedList : sourceValue) {
                List<Object> nestedResult = new ArrayList<>();
                IntStream.range(0, ((List) nestedList).size()).forEachOrdered(x -> nestedResult.add(results.get(indexWrapper.index++)));
                keyToResult.add(nestedResult);
            }
            return keyToResult;
        }
    }

    @Override
    public void doExecute(
        IngestDocument ingestDocument,
        Map<String, Object> ProcessMap,
        List<String> inferenceList,
        BiConsumer<IngestDocument, Exception> handler
    ) {
        try {
            processorInputValidator.validateFieldsValue(fieldMap, environment, ingestDocument, true);
            if (Objects.equals(chunkerType, FIXED_LENGTH_ALGORITHM)) {
                // add maxTokenCount setting from index metadata to chunker parameters
                Map<String, Object> sourceAndMetadataMap = ingestDocument.getSourceAndMetadata();
                String indexName = sourceAndMetadataMap.get(IndexFieldMapper.NAME).toString();
                int maxTokenCount = IndexSettings.MAX_TOKEN_COUNT_SETTING.get(settings);
                IndexMetadata indexMetadata = clusterService.state().metadata().index(indexName);
                if (indexMetadata != null) {
                    // if the index exists, read maxTokenCount from the index setting
                    IndexService indexService = indicesService.indexServiceSafe(indexMetadata.getIndex());
                    maxTokenCount = indexService.getIndexSettings().getMaxTokenCount();
                }
                chunkerParameters.put(FixedTokenLengthChunker.MAX_TOKEN_COUNT_FIELD, maxTokenCount);
            }

            List<List<String>> chunkedResults = new ArrayList<>();
            for (String inferenceString : inferenceList) {
                chunkedResults.add(chunk(inferenceString));
            }
            setTargetFieldsToDocument(ingestDocument, ProcessMap, chunkedResults);
            handler.accept(ingestDocument, null);
        } catch (Exception e) {
            handler.accept(null, e);
        }
    }

    public static class Factory implements Processor.Factory {

        private final Settings settings;

        private final ClusterService clusterService;

        private final IndicesService indicesService;

        private final Environment environment;

        private final AnalysisRegistry analysisRegistry;

        private final ProcessorInputValidator processorInputValidator;

        public Factory(
            Settings settings,
            ClusterService clusterService,
            IndicesService indicesService,
            AnalysisRegistry analysisRegistry,
            Environment environment,
            ProcessorInputValidator processorInputValidator
        ) {
            this.settings = settings;
            this.clusterService = clusterService;
            this.indicesService = indicesService;
            this.analysisRegistry = analysisRegistry;
            this.environment = environment;
            this.processorInputValidator = processorInputValidator;
        }

        @Override
        public DocumentChunkingProcessor create(
            Map<String, Processor.Factory> registry,
            String processorTag,
            String description,
            Map<String, Object> config
        ) throws Exception {
            Map<String, Object> fieldMap = readMap(TYPE, processorTag, config, FIELD_MAP_FIELD);
            Map<String, Object> algorithmMap = readMap(TYPE, processorTag, config, ALGORITHM_FIELD);
            return new DocumentChunkingProcessor(
                processorTag,
                description,
                fieldMap,
                algorithmMap,
                settings,
                clusterService,
                indicesService,
                analysisRegistry,
                environment,
                processorInputValidator
            );
        }

    }
}
