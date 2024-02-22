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

import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.index.Index;
import org.opensearch.index.IndexService;
import org.opensearch.index.IndexSettings;
import org.opensearch.index.analysis.AnalysisRegistry;
import org.opensearch.indices.IndicesService;
import org.opensearch.ingest.IngestDocument;
import org.opensearch.ingest.Processor;
import org.opensearch.ingest.AbstractProcessor;
import org.opensearch.neuralsearch.processor.chunker.ChunkerFactory;
import org.opensearch.neuralsearch.processor.chunker.FixedTokenLengthChunker;
import org.opensearch.neuralsearch.processor.chunker.IFieldChunker;
import org.opensearch.index.mapper.IndexFieldMapper;

import static org.opensearch.ingest.ConfigurationUtils.readMap;
import static org.opensearch.neuralsearch.processor.InferenceProcessor.FIELD_MAP_FIELD;

public final class DocumentChunkingProcessor extends AbstractProcessor {

    public static final String TYPE = "chunking";
    public static final String OUTPUT_FIELD = "output_field";

    private final Map<String, Object> fieldMap;

    private final Set<String> supportedChunkers = ChunkerFactory.getChunkers();

    private final Settings settings;

    private final ClusterService clusterService;

    private final IndicesService indicesService;

    private final AnalysisRegistry analysisRegistry;

    public DocumentChunkingProcessor(
        String tag,
        String description,
        Map<String, Object> fieldMap,
        Settings settings,
        ClusterService clusterService,
        IndicesService indicesService,
        AnalysisRegistry analysisRegistry
    ) {
        super(tag, description);
        validateDocumentChunkingFieldMap(fieldMap);
        this.fieldMap = fieldMap;
        this.settings = settings;
        this.clusterService = clusterService;
        this.indicesService = indicesService;
        this.analysisRegistry = analysisRegistry;
    }

    public String getType() {
        return TYPE;
    }

    @SuppressWarnings("unchecked")
    private void validateDocumentChunkingFieldMap(Map<String, Object> fieldMap) {
        if (fieldMap == null || fieldMap.isEmpty()) {
            throw new IllegalArgumentException("Unable to create the processor as field_map is null or empty");
        }

        for (Map.Entry<String, Object> fieldMapEntry : fieldMap.entrySet()) {
            String inputField = fieldMapEntry.getKey();
            Object parameters = fieldMapEntry.getValue();

            if (parameters == null) {
                throw new IllegalArgumentException("parameters for input field [" + inputField + "] is null, cannot process it.");
            }

            if (!(parameters instanceof Map)) {
                throw new IllegalArgumentException(
                    "parameters for input field [" + inputField + "] cannot be cast to [" + String.class.getName() + "]"
                );
            }

            Map<String, Object> parameterMap = (Map<String, Object>) parameters;

            // output field must be string
            if (!(parameterMap.containsKey(OUTPUT_FIELD))) {
                throw new IllegalArgumentException("parameters for output field [" + OUTPUT_FIELD + "] is null, cannot process it.");
            }

            Object outputField = parameterMap.get(OUTPUT_FIELD);

            if (!(outputField instanceof String)) {
                throw new IllegalArgumentException(
                    "parameters for output field [" + OUTPUT_FIELD + "] cannot be cast to [" + String.class.getName() + "]"
                );
            }

            // check non string parameters
            int chunkingAlgorithmCount = 0;
            Map<String, Object> chunkerParameters;
            for (Map.Entry<?, ?> parameterEntry : parameterMap.entrySet()) {
                if (!(parameterEntry.getKey() instanceof String)) {
                    throw new IllegalArgumentException("found parameter entry with non-string key");
                }
                String parameterKey = (String) parameterEntry.getKey();
                if (supportedChunkers.contains(parameterKey)) {
                    chunkingAlgorithmCount += 1;
                    chunkerParameters = (Map<String, Object>) parameterEntry.getValue();
                    IFieldChunker chunker = ChunkerFactory.create(parameterKey, analysisRegistry);
                    chunker.validateParameters(chunkerParameters);
                }
            }

            // should only define one algorithm
            if (chunkingAlgorithmCount == 0) {
                throw new IllegalArgumentException("chunking algorithm not defined for input field [" + inputField + "]");
            }
            if (chunkingAlgorithmCount > 1) {
                throw new IllegalArgumentException("multiple chunking algorithms defined for input field [" + inputField + "]");
            }
        }
    }

    @Override
    public IngestDocument execute(IngestDocument document) {
        for (Map.Entry<String, Object> fieldMapEntry : fieldMap.entrySet()) {
            String inputField = fieldMapEntry.getKey();
            Object content = document.getFieldValue(inputField, Object.class);

            if (content == null) {
                throw new IllegalArgumentException("input field in document [" + inputField + "] is null, cannot process it.");
            }

            if (content instanceof List<?>) {
                List<?> contentList = (List<?>) content;
                for (Object contentElement : contentList) {
                    if (!(contentElement instanceof String)) {
                        throw new IllegalArgumentException(
                            "element in input field list ["
                                + inputField
                                + "] of type ["
                                + contentElement.getClass().getName()
                                + "] cannot be cast to ["
                                + String.class.getName()
                                + "]"
                        );
                    }
                }
            } else if (!(content instanceof String)) {
                throw new IllegalArgumentException(
                    "input field ["
                        + inputField
                        + "] of type ["
                        + content.getClass().getName()
                        + "] cannot be cast to ["
                        + String.class.getName()
                        + "]"
                );
            }



            Map<?, ?> parameters = (Map<?, ?>) fieldMapEntry.getValue();
            String outputField = (String) parameters.get(OUTPUT_FIELD);
            List<String> chunkedPassages = new ArrayList<>();

            // we have validated that there is one chunking algorithm
            // and that chunkerParameters is of type Map<String, Object>
            for (Map.Entry<?, ?> parameterEntry : parameters.entrySet()) {
                String parameterKey = (String) parameterEntry.getKey();
                if (supportedChunkers.contains(parameterKey)) {
                    @SuppressWarnings("unchecked")
                    Map<String, Object> chunkerParameters = (Map<String, Object>) parameterEntry.getValue();
                    if (Objects.equals(parameterKey, ChunkerFactory.FIXED_LENGTH_ALGORITHM)) {
                        // add maxTokenCount setting from index metadata to chunker parameters
                        Map<String, Object> sourceAndMetadataMap = document.getSourceAndMetadata();
                        String indexName = sourceAndMetadataMap.get(IndexFieldMapper.NAME).toString();
                        Index index = clusterService.state().metadata().index(indexName).getIndex();
                        IndexService indexService = indicesService.indexServiceSafe(index);
                        final int maxTokenCount = indexService == null
                                ? IndexSettings.MAX_TOKEN_COUNT_SETTING.get(settings)
                                : indexService.getIndexSettings().getMaxTokenCount();
                        chunkerParameters.put(FixedTokenLengthChunker.MAX_TOKEN_COUNT, maxTokenCount);
                    }
                    IFieldChunker chunker = ChunkerFactory.create(parameterKey, analysisRegistry);
                    if (content instanceof String) {
                        chunkedPassages = chunker.chunk((String) content, chunkerParameters);
                    } else {
                        for (Object contentElement : (List<?>) content) {
                            chunkedPassages.addAll(chunker.chunk((String) contentElement, chunkerParameters));
                        }
                    }
                }
            }
            document.setFieldValue(outputField, chunkedPassages);
        }
        return document;
    }

    public static class Factory implements Processor.Factory {

        private final Settings settings;

        private final ClusterService clusterService;

        private final IndicesService indicesService;
        
        private final AnalysisRegistry analysisRegistry;

        public Factory(Settings settings, ClusterService clusterService, IndicesService indicesService, AnalysisRegistry analysisRegistry) {
            this.settings = settings;
            this.clusterService = clusterService;
            this.indicesService = indicesService;
            this.analysisRegistry = analysisRegistry;
        }

        @Override
        public DocumentChunkingProcessor create(
            Map<String, Processor.Factory> registry,
            String processorTag,
            String description,
            Map<String, Object> config
        ) throws Exception {
            Map<String, Object> fieldMap = readMap(TYPE, processorTag, config, FIELD_MAP_FIELD);
            return new DocumentChunkingProcessor(
                processorTag,
                description,
                fieldMap,
                settings,
                clusterService,
                indicesService,
                analysisRegistry
            );
        }

    }
}
