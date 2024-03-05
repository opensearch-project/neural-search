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
import java.util.LinkedHashMap;
import java.util.function.BiConsumer;
import java.util.stream.Collectors;

import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.settings.Settings;
import org.opensearch.env.Environment;
import org.opensearch.index.analysis.AnalysisRegistry;
import org.opensearch.indices.IndicesService;
import org.opensearch.ingest.IngestDocument;
import org.opensearch.ingest.Processor;
import org.opensearch.neuralsearch.processor.chunker.ChunkerFactory;
import org.opensearch.neuralsearch.processor.chunker.IFieldChunker;

import static org.opensearch.ingest.ConfigurationUtils.readMap;

public final class DocumentChunkingProcessor extends InferenceProcessor {

    public static final String TYPE = "chunking";
    public static final String OUTPUT_FIELD = "output_field";

    public static final String FIELD_MAP_FIELD = "field_map";

    public static final String LIST_TYPE_NESTED_MAP_KEY = "chunking";

    private final Map<String, Object> chunkingFieldMap;

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
        Environment environment,
        ProcessorInputValidator processorInputValidator
    ) {
        super(
            tag,
            description,
            TYPE,
            LIST_TYPE_NESTED_MAP_KEY,
            null,
            transformFieldMap(fieldMap),
            null,
            environment,
            processorInputValidator
        );
        validateFieldMap(fieldMap, "");
        this.settings = settings;
        this.chunkingFieldMap = fieldMap;
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

    @SuppressWarnings("unchecked")
    private void validateFieldMap(Map<String, Object> fieldMap, String inputPrefix) {
        for (Map.Entry<String, Object> fieldMapEntry : fieldMap.entrySet()) {
            String inputField = fieldMapEntry.getKey();
            if (fieldMapEntry.getValue() instanceof Map) {
                Map<String, Object> insideFieldMap = (Map<String, Object>) fieldMapEntry.getValue();
                if (insideFieldMap.containsKey(OUTPUT_FIELD)) {
                    validateChunkingFieldMap(insideFieldMap, inputPrefix + "." + inputField);
                } else {
                    validateFieldMap(insideFieldMap, inputPrefix + "." + inputField);
                }
            }
        }
    }

    @SuppressWarnings("unchecked")
    private void validateChunkingFieldMap(Map<String, Object> fieldMap, String inputField) {
        // this function validates the parameters for chunking processors with:
        // 1. the output field is a string
        // 2. the chunker parameters must include and only include 1 type of chunker
        // 3. the chunker parameters must be validated by each algorithm
        Object outputField = fieldMap.get(OUTPUT_FIELD);

        if (!(outputField instanceof String)) {
            throw new IllegalArgumentException(
                "parameters for output field [" + OUTPUT_FIELD + "] cannot be cast to [" + String.class.getName() + "]"
            );
        }

        // check non string parameter key
        // validate each algorithm
        int chunkingAlgorithmCount = 0;
        Map<String, Object> chunkerParameters;
        for (Map.Entry<?, ?> parameterEntry : fieldMap.entrySet()) {
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

    private static Map<String, Object> transformFieldMap(Map<String, Object> fieldMap) {
        // transform the into field map for inference processor
        Map<String, Object> transformedFieldMap = new HashMap<>();
        for (Map.Entry<String, Object> fieldMapEntry : fieldMap.entrySet()) {
            String inputField = fieldMapEntry.getKey();
            if (fieldMapEntry.getValue() instanceof Map) {
                Map<String, Object> insideFieldMap = (Map<String, Object>) fieldMapEntry.getValue();
                if (insideFieldMap.containsKey(OUTPUT_FIELD)) {
                    Object outputField = insideFieldMap.get(OUTPUT_FIELD);
                    transformedFieldMap.put(inputField, outputField);
                } else {
                    transformedFieldMap.put(inputField, transformFieldMap(insideFieldMap));
                }
            }
        }
        return transformedFieldMap;
    }

    private List<List<String>> chunk(List<String> contents, Map<String, Map<String, Object>> parameter) {
        // parameter only contains 1 key defining chunker type
        // its value should be chunking parameters
        List<List<String>> chunkedContents = new ArrayList<>();
        for (Map.Entry<String, Map<String, Object>> parameterEntry : parameter.entrySet()) {
            String type = parameterEntry.getKey();
            Map<String, Object> chunkerParameters = parameterEntry.getValue();
            IFieldChunker chunker = ChunkerFactory.create(type, analysisRegistry);
            for (String content : contents) {
                chunkedContents.add(chunker.chunk(content, chunkerParameters));
            }
        }
        return chunkedContents;
    }

    @Override
    public void doExecute(
        IngestDocument ingestDocument,
        Map<String, Object> ProcessMap,
        List<String> inferenceList,
        BiConsumer<IngestDocument, Exception> handler
    ) {
        throw new RuntimeException("method doExecute() not implemented in document chunking processor");
    }

    @Override
    public void execute(IngestDocument ingestDocument, BiConsumer<IngestDocument, Exception> handler) {
        try {
            processorInputValidator.validateFieldsValue(fieldMap, environment, ingestDocument, false);
            Map<String, Object> sourceAndMetadataMap = ingestDocument.getSourceAndMetadata();
            Map<String, Object> processMap = buildMapWithProcessorKeyAndOriginalValue(sourceAndMetadataMap, chunkingFieldMap);
            // List<Object> inferenceList = createInferenceList(processMap);
            // List<List<String>> results = chunk(processMap);
            // setTargetFieldsToDocument(ingestDocument, processMap, results);
            handler.accept(ingestDocument, null);
            /*
            if (inferenceList.isEmpty()) {
                handler.accept(ingestDocument, null);
            } else {
                // perform chunking
                List<List<String>> results = chunk(inferenceList, processMap);
                setTargetFieldsToDocument(ingestDocument, processMap, results);
                doExecute(ingestDocument, processMap, inferenceList, handler);
                handler.accept(ingestDocument, null);
            }
            */
        } catch (Exception e) {
            handler.accept(null, e);
        }
    }

    Map<String, Object> buildMapWithProcessorKeyAndOriginalValue(
        Map<String, Object> sourceAndMetadataMap,
        Map<String, Object> chunkingFieldMap
    ) {
        // the leaf map for processMap contains two key value pairs
        // parameters: the chunker parameters, Map<String, Object>
        // inferenceList: a list of strings to be chunked, List<String>
        Map<String, Object> mapWithProcessorKeys = new LinkedHashMap<>();
        for (Map.Entry<String, Object> fieldMapEntry : fieldMap.entrySet()) {
            String originalKey = fieldMapEntry.getKey();
            Object targetKey = fieldMapEntry.getValue();
            if (targetKey instanceof Map) {
                Map<String, Object> treeRes = new LinkedHashMap<>();
                buildMapWithProcessorKeyAndOriginalValueForMapType(originalKey, targetKey, sourceAndMetadataMap, chunkingFieldMap, treeRes);
                mapWithProcessorKeys.put(originalKey, treeRes.get(originalKey));
            } else {
                Map<String, Object> leafMap = new HashMap<>();
                leafMap.put("parameters", chunkingFieldMap.get(originalKey));
                Object inferenceObject = sourceAndMetadataMap.get(originalKey);
                // inferenceObject is either a string or a list of strings
                if (inferenceObject instanceof List) {
                    leafMap.put("inferenceList", inferenceObject);
                } else {
                    leafMap.put("inferenceList", stringToList((String) inferenceObject));
                }
                mapWithProcessorKeys.put(String.valueOf(targetKey), leafMap);
            }
        }
        return mapWithProcessorKeys;
    }

    @SuppressWarnings("unchecked")
    private void buildMapWithProcessorKeyAndOriginalValueForMapType(
        String parentKey,
        Object processorKey,
        Map<String, Object> sourceAndMetadataMap,
        Map<String, Object> chunkingFieldMap,
        Map<String, Object> treeRes
    ) {
        if (processorKey == null || sourceAndMetadataMap == null || chunkingFieldMap == null) return;
        if (processorKey instanceof Map) {
            Map<String, Object> next = new LinkedHashMap<>();
            if (sourceAndMetadataMap.get(parentKey) instanceof Map) {
                for (Map.Entry<String, Object> nestedFieldMapEntry : ((Map<String, Object>) processorKey).entrySet()) {
                    buildMapWithProcessorKeyAndOriginalValueForMapType(
                        nestedFieldMapEntry.getKey(),
                        nestedFieldMapEntry.getValue(),
                        (Map<String, Object>) sourceAndMetadataMap.get(parentKey),
                        (Map<String, Object>) chunkingFieldMap.get(parentKey),
                        next
                    );
                }
            } else if (sourceAndMetadataMap.get(parentKey) instanceof List) {
                for (Map.Entry<String, Object> nestedFieldMapEntry : ((Map<String, Object>) processorKey).entrySet()) {
                    List<Map<String, Object>> list = (List<Map<String, Object>>) sourceAndMetadataMap.get(parentKey);
                    List<Object> listOfStrings = list.stream().map(x -> x.get(nestedFieldMapEntry.getKey())).collect(Collectors.toList());
                    Map<String, Object> map = new LinkedHashMap<>();
                    map.put(nestedFieldMapEntry.getKey(), listOfStrings);
                    buildMapWithProcessorKeyAndOriginalValueForMapType(
                        nestedFieldMapEntry.getKey(),
                        nestedFieldMapEntry.getValue(),
                        map,
                        (Map<String, Object>) chunkingFieldMap.get(nestedFieldMapEntry.getKey()),
                        next
                    );
                }
            }
            treeRes.put(parentKey, next);
        } else {
            Map<String, Object> leafMap = new HashMap<>();
            leafMap.put("parameters", chunkingFieldMap.get(parentKey));
            Object inferenceObject = sourceAndMetadataMap.get(parentKey);
            // inferenceObject is either a string or a list of strings
            if (inferenceObject instanceof List) {
                leafMap.put("inferenceList", inferenceObject);
            } else {
                leafMap.put("inferenceList", stringToList((String) inferenceObject));
            }
            treeRes.put(parentKey, leafMap);
        }
    }

    private static List<String> stringToList(String string) {
        List<String> list = new ArrayList<>();
        list.add(string);
        return list;
    }

    @SuppressWarnings("unchecked")
    private List<List<String>> createInferenceList(Map<String, Object> processMap) {
        List<List<String>> texts = new ArrayList<>();
        processMap.entrySet().stream().filter(processMapEntry -> processMapEntry.getValue() != null).forEach(processMapEntry -> {
            Map<String, Object> sourceValue = (Map<String, Object>) processMapEntry.getValue();
            // get "inferenceList" key
            createInferenceListForMapTypeInput(sourceValue, texts);
        });
        return texts;
    }

    @SuppressWarnings("unchecked")
    private void createInferenceListForMapTypeInput(Map<String, Object> mapInput, List<List<String>> texts) {
        if (mapInput.containsKey("inferenceList")) {
            texts.add((List<String>) mapInput.get("inferenceList"));
            return;
        }
        for (Map.Entry<String, Object> nestedFieldMapEntry : mapInput.entrySet()) {
            Map<String, Object> nestedMapInput = (Map<String, Object>) nestedFieldMapEntry.getValue();
            createInferenceListForMapTypeInput(nestedMapInput, texts);
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
            return new DocumentChunkingProcessor(
                processorTag,
                description,
                fieldMap,
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
