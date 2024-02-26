/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.processor;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.settings.Settings;
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

    private final Set<String> supportedChunkers = ChunkerFactory.getAllChunkers();

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
                    "parameters for input field [" + inputField + "] cannot be cast to [" + Map.class.getName() + "]"
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
            if (chunkingAlgorithmCount != 1) {
                throw new IllegalArgumentException("input field [" + inputField + "] should has and only has 1 chunking algorithm");
            }
        }
    }

    private void validateContent(Object content, String inputField) {
        // content can be a map, a list of strings or a list
        if (content instanceof Map) {
            System.out.println("map type");
            @SuppressWarnings("unchecked")
            Map<String, Object> contentMap = (Map<String, Object>) content;
            for (Map.Entry<String, Object> contentEntry : contentMap.entrySet()) {
                String contentKey = contentEntry.getKey();
                Object contentValue = contentEntry.getValue();
                // the map value can also be a map, list or string
                validateContent(contentValue, inputField + "." + contentKey);
            }
        } else if (content instanceof List) {
            List<?> contentList = (List<?>) content;
            for (Object contentElement : contentList) {
                if (!(contentElement instanceof String)) {
                    throw new IllegalArgumentException(
                        "some element in input field list ["
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
    }

    private Object chunk(IFieldChunker chunker, Object content, Map<String, Object> chunkerParameters) {
        // assume that content is either a map, list or string
        if (content instanceof Map) {
            Map<String, Object> chunkedPassageMap = new HashMap<>();
            Map<String, Object> contentMap = (Map<String, Object>) content;
            for (Map.Entry<String, Object> contentEntry : contentMap.entrySet()) {
                String contentKey = contentEntry.getKey();
                Object contentValue = contentEntry.getValue();
                // contentValue can also be a map, list or string
                chunkedPassageMap.put(contentKey, chunk(chunker, contentValue, chunkerParameters));
            }
            return chunkedPassageMap;
        } else if (content instanceof List) {
            List<String> chunkedPassageList = new ArrayList<>();
            List<?> contentList = (List<?>) content;
            for (Object contentElement : contentList) {
                chunkedPassageList.addAll(chunker.chunk((String) contentElement, chunkerParameters));
            }
            return chunkedPassageList;
        } else {
            return chunker.chunk((String) content, chunkerParameters);
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

            validateContent(content, inputField);

            @SuppressWarnings("unchecked")
            Map<String, Object> parameters = (Map<String, Object>) fieldMapEntry.getValue();
            String outputField = (String) parameters.get(OUTPUT_FIELD);

            // we have validated that there is one chunking algorithm
            // and that chunkerParameters is of type Map<String, Object>
            for (Map.Entry<String, Object> parameterEntry : parameters.entrySet()) {
                String parameterKey = parameterEntry.getKey();
                if (supportedChunkers.contains(parameterKey)) {
                    @SuppressWarnings("unchecked")
                    Map<String, Object> chunkerParameters = (Map<String, Object>) parameterEntry.getValue();
                    if (Objects.equals(parameterKey, ChunkerFactory.FIXED_LENGTH_ALGORITHM)) {
                        // add maxTokenCount to chunker parameters
                        Map<String, Object> sourceAndMetadataMap = document.getSourceAndMetadata();
                        int maxTokenCount = IndexSettings.MAX_TOKEN_COUNT_SETTING.get(settings);
                        String indexName = sourceAndMetadataMap.get(IndexFieldMapper.NAME).toString();
                        IndexMetadata indexMetadata = clusterService.state().metadata().index(indexName);
                        if (indexMetadata != null) {
                            // if the index exists, read maxTokenCount from the index setting
                            IndexService indexService = indicesService.indexServiceSafe(indexMetadata.getIndex());
                            maxTokenCount = indexService.getIndexSettings().getMaxTokenCount();
                        }
                        chunkerParameters.put(FixedTokenLengthChunker.MAX_TOKEN_COUNT, maxTokenCount);
                    }
                    IFieldChunker chunker = ChunkerFactory.create(parameterKey, analysisRegistry);
                    document.setFieldValue(outputField, chunk(chunker, content, chunkerParameters));
                }
            }
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
