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
import java.util.function.BiConsumer;

import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.settings.Settings;
import org.opensearch.env.Environment;
import org.opensearch.index.IndexService;
import org.opensearch.index.IndexSettings;
import org.opensearch.index.analysis.AnalysisRegistry;
import org.opensearch.indices.IndicesService;
import org.opensearch.ingest.IngestDocument;
import org.opensearch.ingest.Processor;
import org.opensearch.neuralsearch.processor.chunker.ChunkerFactory;
import org.opensearch.neuralsearch.processor.chunker.FixedTokenLengthChunker;
import org.opensearch.neuralsearch.processor.chunker.IFieldChunker;
import org.opensearch.index.mapper.IndexFieldMapper;

import static org.opensearch.ingest.ConfigurationUtils.readMap;

public final class DocumentChunkingProcessor extends InferenceProcessor {

    public static final String TYPE = "chunking";
    public static final String OUTPUT_FIELD = "output_field";

    public static final String FIELD_MAP_FIELD = "field_map";

    public static final String LIST_TYPE_NESTED_MAP_KEY = "chunking";

    private final Map<String, Object> originalFieldMap;

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
        AnalysisRegistry analysisRegistry,
        Environment environment
    ) {
        super(tag, description, TYPE, LIST_TYPE_NESTED_MAP_KEY, "", tranferFieldMap(fieldMap), null, environment);
        this.originalFieldMap = fieldMap;
        this.settings = settings;
        this.clusterService = clusterService;
        this.indicesService = indicesService;
        this.analysisRegistry = analysisRegistry;
    }

    public String getType() {
        return TYPE;
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

    private static Map<String, Object> tranferFieldMap(Map<String, Object> orignalMap) {
        // The original map should be
        Map<String, Object> transferFieldMap = new HashMap<>();
        Map<String, Object> tmpParameters = (Map<String, Object>) orignalMap.entrySet().iterator().next().getValue();
        String inputField = orignalMap.entrySet().iterator().next().getKey();
        Object outputField = tmpParameters.get(OUTPUT_FIELD);
        transferFieldMap.put(inputField, outputField);
        return transferFieldMap;
    }

    @Override
    public void doExecute(
        IngestDocument ingestDocument,
        Map<String, Object> ProcessMap,
        List<String> inferenceList,
        BiConsumer<IngestDocument, Exception> handler
    ) {
        List<Object> results = new ArrayList<>();
        for (Map.Entry<String, Object> fieldMapEntry : originalFieldMap.entrySet()) {
            String inputField = fieldMapEntry.getKey();
            Object content = ingestDocument.getFieldValue(inputField, Object.class);

            if (content == null) {
                throw new IllegalArgumentException("input field in document [" + inputField + "] is null, cannot process it.");
            }


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
                        // for fixed token length algorithm, add maxTokenCount to chunker parameters
                        Map<String, Object> sourceAndMetadataMap = ingestDocument.getSourceAndMetadata();
                        int maxTokenCount = IndexSettings.MAX_TOKEN_COUNT_SETTING.get(settings);
                        String indexName = sourceAndMetadataMap.get(IndexFieldMapper.NAME).toString();
                        IndexMetadata indexMetadata = clusterService.state().metadata().index(indexName);
                        if (indexMetadata != null) {
                            // if the index exists, read maxTokenCount from the index setting
                            IndexService indexService = indicesService.indexServiceSafe(indexMetadata.getIndex());
                            maxTokenCount = indexService.getIndexSettings().getMaxTokenCount();
                        }
                        chunkerParameters.put(FixedTokenLengthChunker.MAX_TOKEN_COUNT_FIELD, maxTokenCount);
                    }
                    IFieldChunker chunker = ChunkerFactory.create(parameterKey, analysisRegistry);
                    results.add(chunk(chunker, content, chunkerParameters));
                }
            }
        }
        try {
            setTargetFieldsToDocument(ingestDocument, ProcessMap, results);
            handler.accept(ingestDocument, null);
        } catch (Exception exception) {
            handler.accept(null, exception);
        }

    }

    public static class Factory implements Processor.Factory {

        private final Settings settings;

        private final ClusterService clusterService;

        private final IndicesService indicesService;

        private final Environment environment;

        private final AnalysisRegistry analysisRegistry;

        public Factory(
            Settings settings,
            ClusterService clusterService,
            IndicesService indicesService,
            AnalysisRegistry analysisRegistry,
            Environment environment
        ) {
            this.settings = settings;
            this.clusterService = clusterService;
            this.indicesService = indicesService;
            this.analysisRegistry = analysisRegistry;
            this.environment = environment;
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
                analysisRegistry,
                environment
            );
        }

    }
}
