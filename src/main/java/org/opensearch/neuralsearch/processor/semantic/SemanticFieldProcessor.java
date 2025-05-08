/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.processor.semantic;

import lombok.NonNull;
import lombok.extern.log4j.Log4j2;
import org.apache.commons.lang3.tuple.Pair;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.Nullable;
import org.opensearch.core.action.ActionListener;
import org.opensearch.env.Environment;
import org.opensearch.ingest.AbstractBatchingSystemProcessor;
import org.opensearch.ingest.IngestDocument;
import org.opensearch.ingest.IngestDocumentWrapper;
import org.opensearch.ml.common.MLModel;
import org.opensearch.neuralsearch.ml.MLCommonsClientAccessor;
import org.opensearch.neuralsearch.processor.TextInferenceRequest;
import org.opensearch.neuralsearch.processor.chunker.Chunker;
import org.opensearch.neuralsearch.processor.chunker.FixedTokenLengthChunker;
import org.opensearch.neuralsearch.processor.dto.SemanticFieldInfo;
import org.opensearch.neuralsearch.util.TokenWeightUtil;
import org.opensearch.neuralsearch.util.prune.PruneType;
import org.opensearch.neuralsearch.util.prune.PruneUtils;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import static org.opensearch.neuralsearch.constants.MappingConstants.PATH_SEPARATOR;
import static org.opensearch.neuralsearch.constants.SemanticFieldConstants.DEFAULT_SEMANTIC_INFO_FIELD_NAME_SUFFIX;
import static org.opensearch.neuralsearch.constants.SemanticInfoFieldConstants.CHUNKS_TEXT_FIELD_NAME;
import static org.opensearch.neuralsearch.constants.SemanticInfoFieldConstants.MODEL_ID_FIELD_NAME;
import static org.opensearch.neuralsearch.constants.SemanticInfoFieldConstants.MODEL_NAME_FIELD_NAME;
import static org.opensearch.neuralsearch.constants.SemanticInfoFieldConstants.MODEL_TYPE_FIELD_NAME;
import static org.opensearch.neuralsearch.processor.chunker.Chunker.CHUNK_STRING_COUNT_FIELD;
import static org.opensearch.neuralsearch.processor.chunker.Chunker.DEFAULT_MAX_CHUNK_LIMIT;
import static org.opensearch.neuralsearch.processor.chunker.Chunker.MAX_CHUNK_LIMIT_FIELD;
import static org.opensearch.neuralsearch.processor.util.ProcessorUtils.getMaxTokenCount;
import static org.opensearch.neuralsearch.util.ProcessorDocumentUtils.unflattenIngestDoc;
import static org.opensearch.neuralsearch.util.SemanticMLModelUtils.getModelType;
import static org.opensearch.neuralsearch.util.SemanticMLModelUtils.isDenseModel;
import static org.opensearch.neuralsearch.util.SemanticMappingUtils.getModelId;
import static org.opensearch.neuralsearch.util.SemanticMappingUtils.getSemanticInfoFieldName;

/**
 * Processor to ingest the semantic fields. It will do text chunking and embedding generation for the semantic field.
 *
 * This processor is for internal usage and will be systematically invoked if we detect there are semantic fields
 * defined. Users should not be able to define this processor in a regular ingest pipeline.
 */
@Log4j2
public class SemanticFieldProcessor extends AbstractBatchingSystemProcessor {

    public static final String PROCESSOR_TYPE = "system_ingest_processor_semantic_field";

    private final Map<String, Map<String, Object>> pathToFieldConfig;
    private final Map<String, MLModel> modelIdToModelMap = new ConcurrentHashMap<>();
    private final Map<String, String> modelIdToModelTypeMap = new ConcurrentHashMap<>();

    protected final MLCommonsClientAccessor mlCommonsClientAccessor;
    private final Environment environment;
    private final ClusterService clusterService;

    private final Chunker chunker;

    private final static float DEFAULT_PRUNE_RATIO = 0.1f;

    public SemanticFieldProcessor(
        @Nullable final String tag,
        @Nullable final String description,
        final int batchSize,
        @NonNull final Map<String, Map<String, Object>> pathToFieldConfig,
        @NonNull final MLCommonsClientAccessor mlClientAccessor,
        @NonNull final Environment environment,
        @NonNull final ClusterService clusterService,
        @NonNull final Chunker chunker
    ) {
        super(tag, description, batchSize);
        this.pathToFieldConfig = pathToFieldConfig;
        this.mlCommonsClientAccessor = mlClientAccessor;
        this.environment = environment;
        this.clusterService = clusterService;
        this.chunker = chunker;
    }

    /**
     * Since we need to do async work in this processor we will not invoke this function.
     * @param ingestDocument {@link IngestDocument} which is the document passed to processor.
     * @return {@link IngestDocument} document unchanged
     */
    @Override
    public IngestDocument execute(IngestDocument ingestDocument) throws Exception {
        throw new UnsupportedOperationException(
            String.format(Locale.ROOT, "Should not try to use %s to ingest a doc synchronously.", PROCESSOR_TYPE)
        );
    }

    /**
     * This method will be invoked by PipelineService to make async inference and then delegate the handler to
     * process the inference response or failure.
     * @param ingestDocument {@link IngestDocument} which is the document passed to processor.
     * @param handler {@link BiConsumer} which is the handler which can be used after the inference task is done.
     */
    @Override
    public void execute(IngestDocument ingestDocument, BiConsumer<IngestDocument, Exception> handler) {
        try {
            unflattenIngestDoc(ingestDocument);
            // Collect all the semantic field info based on the path of semantic fields found in the index mapping
            final List<SemanticFieldInfo> semanticFieldInfoList = getSemanticFieldInfo(ingestDocument);

            if (semanticFieldInfoList.isEmpty()) {
                handler.accept(ingestDocument, null);
            } else {
                fetchModelInfoThenProcess(ingestDocument, semanticFieldInfoList, handler);
            }
        } catch (Exception e) {
            handler.accept(null, e);
        }
    }

    private void fetchModelInfoThenProcess(
        @NonNull final IngestDocument ingestDocument,
        @NonNull final List<SemanticFieldInfo> semanticFieldInfoList,
        @NonNull final BiConsumer<IngestDocument, Exception> handler
    ) {
        final Set<String> modelIdsToGetModelInfo = semanticFieldInfoList.stream()
            .map(SemanticFieldInfo::getModelId)
            .collect(Collectors.toSet());
        // In P0, we do not handle re-fetching model configurations if they are updated via the
        // ML Commons Update Model API. If the update is not backward-compatible (e.g., the embedding
        // dimension of a dense model changes and no longer matches the index mapping), errors will be
        // thrown at runtime—for example, during document ingestion or query execution.
        //
        // Users must update the index mapping to reflect the new model config. Simply updating the
        // model is not enough; a dummy mapping update that includes the semantic field (even without
        // actual changes) can trigger the processor to reload the latest model config.
        //
        // Since model config is already validated during index creation, we cache it here for better
        // performance to avoid fetching it on every ingest request.
        // TODO: Handle the model config update case more gracefully.
        for (final String existingModelId : modelIdToModelMap.keySet()) {
            modelIdsToGetModelInfo.remove(existingModelId);
        }
        if (modelIdsToGetModelInfo.isEmpty()) {
            process(ingestDocument, semanticFieldInfoList, handler);
        } else {
            mlCommonsClientAccessor.getModels(modelIdsToGetModelInfo, modelIdToConfigMap -> {
                modelIdToModelMap.putAll(modelIdToConfigMap);
                process(ingestDocument, semanticFieldInfoList, handler);
            }, e -> handler.accept(null, e));
        }
    }

    private void process(
        @NonNull final IngestDocument ingestDocument,
        @NonNull final List<SemanticFieldInfo> semanticFieldInfoList,
        @NonNull final BiConsumer<IngestDocument, Exception> handler
    ) {
        setModelInfo(ingestDocument, semanticFieldInfoList);

        chunk(ingestDocument, semanticFieldInfoList);

        generateAndSetEmbedding(ingestDocument, semanticFieldInfoList, handler);
    }

    private void setModelInfo(@NonNull final IngestDocument ingestDocument, @NonNull final List<SemanticFieldInfo> semanticFieldInfoList) {
        final Map<String, Map<String, Object>> modelIdToInfoMap = new HashMap<>();
        for (final Map.Entry<String, MLModel> entry : modelIdToModelMap.entrySet()) {
            final Map<String, Object> modelInfo = new HashMap<>();
            final String modelId = entry.getKey();
            final MLModel mlModel = entry.getValue();

            String modelType;
            if (modelIdToModelTypeMap.containsKey(modelId)) {
                modelType = modelIdToModelTypeMap.get(modelId);
            } else {
                modelType = getModelType(mlModel);
                modelIdToModelTypeMap.put(modelId, modelType);
            }

            modelInfo.put(MODEL_ID_FIELD_NAME, modelId);
            modelInfo.put(MODEL_TYPE_FIELD_NAME, modelType);
            modelInfo.put(MODEL_NAME_FIELD_NAME, mlModel.getName());

            modelIdToInfoMap.put(modelId, modelInfo);
        }

        for (final SemanticFieldInfo semanticFieldInfo : semanticFieldInfoList) {
            ingestDocument.setFieldValue(semanticFieldInfo.getFullPathForModelInfo(), modelIdToInfoMap.get(semanticFieldInfo.getModelId()));
        }
    }

    @SuppressWarnings("unchecked")
    private void generateAndSetEmbedding(
        @NonNull final IngestDocument ingestDocument,
        @NonNull final List<SemanticFieldInfo> semanticFieldInfoList,
        @NonNull final BiConsumer<IngestDocument, Exception> handler
    ) {
        final Map<String, Set<String>> modelIdToRawDataMap = groupRawDataByModelId(semanticFieldInfoList);

        generateEmbedding(modelIdToRawDataMap, modelIdValueToEmbeddingMap -> {
            try {
                setInference(ingestDocument, semanticFieldInfoList, modelIdValueToEmbeddingMap, DEFAULT_PRUNE_RATIO);
            } catch (Exception e) {
                handler.accept(null, e);
            }
            handler.accept(ingestDocument, null);
        });
    }

    private Map<String, Set<String>> groupRawDataByModelId(@NonNull final Collection<List<SemanticFieldInfo>> semanticFieldInfoLists) {
        final Map<String, Set<String>> modelIdToRawDataMap = new HashMap<>();
        for (final List<SemanticFieldInfo> semanticFieldInfoList : semanticFieldInfoLists) {
            for (final SemanticFieldInfo semanticFieldInfo : semanticFieldInfoList) {
                modelIdToRawDataMap.computeIfAbsent(semanticFieldInfo.getModelId(), k -> new HashSet<>())
                    .addAll(semanticFieldInfo.getChunks());
            }
        }
        return modelIdToRawDataMap;
    }

    private Map<String, Set<String>> groupRawDataByModelId(@NonNull final List<SemanticFieldInfo> semanticFieldInfoList) {
        return groupRawDataByModelId(Collections.singleton(semanticFieldInfoList));
    }

    @SuppressWarnings("unchecked")
    private void setInference(
        @NonNull final IngestDocument ingestDocument,
        @NonNull final List<SemanticFieldInfo> semanticFieldInfoList,
        @NonNull final Map<Pair<String, String>, Pair<Object, Exception>> modelIdValueToEmbeddingMap,
        @NonNull final Float pruneRatio
    ) throws Exception {
        for (final SemanticFieldInfo semanticFieldInfo : semanticFieldInfoList) {
            final String modelId = semanticFieldInfo.getModelId();
            final boolean isDenseModel = isDenseModel(modelIdToModelTypeMap.get(modelId));
            final List<String> chunks = semanticFieldInfo.getChunks();
            for (int i = 0; i < chunks.size(); i++) {
                final String chunk = chunks.get(i);
                final Exception exception = modelIdValueToEmbeddingMap.get(Pair.of(modelId, chunk)).getRight();
                if (exception != null) {
                    throw exception;
                }
                Object embedding = modelIdValueToEmbeddingMap.get(Pair.of(modelId, chunk)).getLeft();
                // TODO: In future we should allow user to configure how we should prune the sparse embedding
                // for each semantic field. Then we can pull the config from the semantic config and use it here.
                if (!isDenseModel) {
                    embedding = PruneUtils.pruneSparseVector(PruneType.MAX_RATIO, pruneRatio, (Map<String, Float>) embedding);
                }
                final String embeddingFullPath = semanticFieldInfo.getFullPathForEmbedding(i);
                ingestDocument.setFieldValue(embeddingFullPath, embedding);
            }
        }
    }

    private List<SemanticFieldInfo> getSemanticFieldInfo(IngestDocument ingestDocument) {
        final List<SemanticFieldInfo> semanticFieldInfos = new ArrayList<>();
        final Object doc = ingestDocument.getSourceAndMetadata();
        pathToFieldConfig.forEach((path, config) -> collectSemanticFieldInfo(doc, path.split("\\."), config, 0, "", semanticFieldInfos));
        return semanticFieldInfos;
    }

    private void chunk(@NonNull final IngestDocument ingestDocument, @NonNull final List<SemanticFieldInfo> semanticFieldInfo) {
        final Map<String, Object> sourceAndMetadataMap = ingestDocument.getSourceAndMetadata();
        final Map<String, Object> runtimeParameters = new HashMap<>();
        int maxTokenCount = getMaxTokenCount(sourceAndMetadataMap, environment.settings(), clusterService);
        int chunkStringCount = semanticFieldInfo.size();
        runtimeParameters.put(FixedTokenLengthChunker.MAX_TOKEN_COUNT_FIELD, maxTokenCount);
        runtimeParameters.put(MAX_CHUNK_LIMIT_FIELD, DEFAULT_MAX_CHUNK_LIMIT);
        runtimeParameters.put(CHUNK_STRING_COUNT_FIELD, chunkStringCount);
        for (final SemanticFieldInfo fieldInfo : semanticFieldInfo) {
            final List<String> chunkedText = chunker.chunk(fieldInfo.getValue(), runtimeParameters);
            fieldInfo.setChunks(chunkedText);
            final String chunksFullPath = fieldInfo.getFullPathForChunks();
            final List<Map<String, Object>> chunks = new ArrayList<>();

            for (final String text : chunkedText) {
                final Map<String, Object> chunk = new HashMap<>();
                chunk.put(CHUNKS_TEXT_FIELD_NAME, text);
                chunks.add(chunk);
            }

            ingestDocument.setFieldValue(chunksFullPath, chunks);
        }
    }

    /**
     * Recursively collects semantic field values from the document.
     */
    private void collectSemanticFieldInfo(
        @Nullable final Object node,
        @NonNull final String[] pathParts,
        @NonNull final Map<String, Object> fieldConfig,
        final int depth,
        @NonNull final String currentPath,
        @NonNull final List<SemanticFieldInfo> semanticFieldInfoList
    ) {
        if (depth > pathParts.length || node == null) {
            return;
        }

        // when depth == pathParts.length it means the current node should be the value of the semantic field so we
        // do not need to get the kep from the pathParts.
        final String key = depth < pathParts.length ? pathParts[depth] : null;

        if (depth < pathParts.length && node instanceof Map<?, ?> mapNode) {
            final Object nextNode = mapNode.get(key);
            final String newPath = currentPath.isEmpty() ? key : currentPath + PATH_SEPARATOR + key;
            collectSemanticFieldInfo(nextNode, pathParts, fieldConfig, depth + 1, newPath, semanticFieldInfoList);
        } else if (depth < pathParts.length && node instanceof List<?> listNode) {
            for (int i = 0; i < listNode.size(); i++) {
                final Object listItem = listNode.get(i);
                final String indexedPath = currentPath + PATH_SEPARATOR + i;
                collectSemanticFieldInfo(listItem, pathParts, fieldConfig, depth, indexedPath, semanticFieldInfoList);
            }
        } else if (depth == pathParts.length) {
            // the current node is the value of the semantic field and it should be a string value
            final String fullPath = String.join(PATH_SEPARATOR, pathParts);
            if (node instanceof String == false) {
                throw new IllegalArgumentException(
                    String.format(
                        Locale.ROOT,
                        "Expect the semantic field at path: %s to be a string but found: %s.",
                        fullPath,
                        node.getClass()
                    )
                );
            }
            final String modelId = getModelId(fieldConfig, fullPath);

            String semanticInfoFullPath = currentPath + DEFAULT_SEMANTIC_INFO_FIELD_NAME_SUFFIX;
            final String userDefinedFieldName = getSemanticInfoFieldName(fieldConfig, fullPath);
            if (userDefinedFieldName != null) {
                final String[] paths = currentPath.split("\\.");
                paths[paths.length - 1] = userDefinedFieldName;
                semanticInfoFullPath = String.join(PATH_SEPARATOR, paths);
            }

            final SemanticFieldInfo semanticFieldInfo = new SemanticFieldInfo();
            semanticFieldInfo.setValue(node.toString());
            semanticFieldInfo.setModelId(modelId);
            semanticFieldInfo.setFullPath(currentPath);
            semanticFieldInfo.setSemanticInfoFullPath(semanticInfoFullPath);

            semanticFieldInfoList.add(semanticFieldInfo);
        }
    }

    @Override
    public void subBatchExecute(List<IngestDocumentWrapper> ingestDocumentWrappers, Consumer<List<IngestDocumentWrapper>> handler) {
        if (ingestDocumentWrappers == null || ingestDocumentWrappers.isEmpty()) {
            handler.accept(ingestDocumentWrappers);
            return;
        }
        try {
            final Map<IngestDocumentWrapper, List<SemanticFieldInfo>> docToSemanticFieldInfoMap = new HashMap<>();
            for (final IngestDocumentWrapper ingestDocumentWrapper : ingestDocumentWrappers) {
                final IngestDocument ingestDocument = ingestDocumentWrapper.getIngestDocument();
                if (ingestDocument == null) {
                    continue;
                }
                unflattenIngestDoc(ingestDocument);
                final List<SemanticFieldInfo> semanticFieldInfoList = getSemanticFieldInfo(ingestDocument);
                if (semanticFieldInfoList.isEmpty() == false) {
                    docToSemanticFieldInfoMap.put(ingestDocumentWrapper, semanticFieldInfoList);
                }
            }

            if (docToSemanticFieldInfoMap.isEmpty()) {
                handler.accept(ingestDocumentWrappers);
            } else {
                try {
                    fetchModelInfoThenBatchProcess(ingestDocumentWrappers, docToSemanticFieldInfoMap, handler);
                } catch (Exception e) {
                    addExceptionToImpactedDocs(docToSemanticFieldInfoMap.keySet(), e);
                    handler.accept(ingestDocumentWrappers);
                }
            }
        } catch (Exception e) {
            addExceptionToImpactedDocs(new HashSet<>(ingestDocumentWrappers), e);
            handler.accept(ingestDocumentWrappers);
        }
    }

    private void fetchModelInfoThenBatchProcess(
        @NonNull final List<IngestDocumentWrapper> ingestDocumentWrappers,
        @NonNull final Map<IngestDocumentWrapper, List<SemanticFieldInfo>> docToSemanticFieldInfoMap,
        @NonNull final Consumer<List<IngestDocumentWrapper>> handler
    ) {
        final Set<String> modelIdsToGetConfig = new HashSet<>();
        docToSemanticFieldInfoMap.values().forEach(semanticFieldInfoList -> {
            semanticFieldInfoList.forEach(semanticFieldInfo -> modelIdsToGetConfig.add(semanticFieldInfo.getModelId()));
        });
        for (String existingModelId : modelIdToModelMap.keySet()) {
            modelIdsToGetConfig.remove(existingModelId);
        }

        if (modelIdsToGetConfig.isEmpty()) {
            batchProcess(ingestDocumentWrappers, docToSemanticFieldInfoMap, handler);
        } else {
            mlCommonsClientAccessor.getModels(modelIdsToGetConfig, modelIdToConfigMap -> {
                modelIdToModelMap.putAll(modelIdToConfigMap);
                batchProcess(ingestDocumentWrappers, docToSemanticFieldInfoMap, handler);
            }, e -> {
                addExceptionToImpactedDocs(docToSemanticFieldInfoMap.keySet(), e);
                handler.accept(ingestDocumentWrappers);
            });
        }
    }

    private void batchProcess(
        @NonNull final List<IngestDocumentWrapper> ingestDocumentWrappers,
        @NonNull final Map<IngestDocumentWrapper, List<SemanticFieldInfo>> docToSemanticFieldInfoMap,
        @NonNull final Consumer<List<IngestDocumentWrapper>> handler
    ) {

        for (Map.Entry<IngestDocumentWrapper, List<SemanticFieldInfo>> entry : docToSemanticFieldInfoMap.entrySet()) {
            final IngestDocumentWrapper ingestDocumentWrapper = entry.getKey();
            final IngestDocument ingestDocument = entry.getKey().getIngestDocument();
            final List<SemanticFieldInfo> semanticFieldInfoList = entry.getValue();
            try {
                setModelInfo(ingestDocument, semanticFieldInfoList);

                chunk(ingestDocument, semanticFieldInfoList);
            } catch (Exception e) {
                log.error(
                    String.format(
                        Locale.ROOT,
                        "Failed to set model info and chunk the semantic fields for the ingest document %s. Root cause: %s",
                        ingestDocument.toString(),
                        e.getMessage()
                    ),
                    e
                );
                if (ingestDocumentWrapper.getException() == null) {
                    ingestDocumentWrapper.update(ingestDocument, e);
                }
            }
        }

        batchGenerateAndSetEmbedding(ingestDocumentWrappers, docToSemanticFieldInfoMap, handler);
    }

    @SuppressWarnings("unchecked")
    private void generateEmbedding(
        @NonNull final Map<String, Set<String>> modelIdToRawDataMap,
        @NonNull final Consumer<Map<Pair<String, String>, Pair<Object, Exception>>> onComplete
    ) {
        final AtomicInteger counter = new AtomicInteger(modelIdToRawDataMap.size());
        final Map<Pair<String, String>, Pair<Object, Exception>> modelIdValueToEmbeddingMap = new ConcurrentHashMap<>();

        for (final Map.Entry<String, Set<String>> entry : modelIdToRawDataMap.entrySet()) {
            final String modelId = entry.getKey();
            final boolean isDenseModel = isDenseModel(modelIdToModelTypeMap.get(modelId));
            final List<String> values = new ArrayList<>(entry.getValue());

            final TextInferenceRequest textInferenceRequest = TextInferenceRequest.builder().inputTexts(values).modelId(modelId).build();

            final ActionListener<?> listener = ActionListener.wrap(embeddings -> {
                List<?> formattedEmbeddings = (List<?>) embeddings;
                if (isDenseModel == false) {
                    formattedEmbeddings = TokenWeightUtil.fetchListOfTokenWeightMap((List<Map<String, ?>>) embeddings);
                }
                for (int i = 0; i < values.size(); i++) {
                    modelIdValueToEmbeddingMap.put(Pair.of(modelId, values.get(i)), Pair.of(formattedEmbeddings.get(i), null));
                }
                if (counter.decrementAndGet() == 0) {
                    onComplete.accept(modelIdValueToEmbeddingMap);
                }
            }, e -> {
                for (String value : values) {
                    modelIdValueToEmbeddingMap.put(Pair.of(modelId, value), Pair.of(null, e));
                }
                if (counter.decrementAndGet() == 0) {
                    onComplete.accept(modelIdValueToEmbeddingMap);
                }
            });

            // For P0 we simply pass all the raw data grouped by the model id which is how our existing embedding
            // processors work. But we may want to make the size limit of input to the predict API configurable,
            // and we should chunk the input accordingly in the future.
            if (isDenseModel) {
                mlCommonsClientAccessor.inferenceSentences(textInferenceRequest, (ActionListener<List<List<Number>>>) listener);
            } else {
                mlCommonsClientAccessor.inferenceSentencesWithMapResult(
                    textInferenceRequest,
                    (ActionListener<List<Map<String, ?>>>) listener
                );
            }
        }
    }

    private void batchGenerateAndSetEmbedding(
        @NonNull final List<IngestDocumentWrapper> ingestDocumentWrappers,
        @NonNull final Map<IngestDocumentWrapper, List<SemanticFieldInfo>> docToSemanticFieldInfoMap,
        @NonNull final Consumer<List<IngestDocumentWrapper>> handler
    ) {
        final Map<String, Set<String>> modelIdToRawDataMap = groupRawDataByModelId(docToSemanticFieldInfoMap.values());

        generateEmbedding(modelIdToRawDataMap, modelIdValueToEmbeddingMap -> {
            batchSetInference(docToSemanticFieldInfoMap, modelIdValueToEmbeddingMap);
            handler.accept(ingestDocumentWrappers);
        });
    }

    private void batchSetInference(
        @NonNull final Map<IngestDocumentWrapper, List<SemanticFieldInfo>> docToSemanticFieldInfoMap,
        @NonNull final Map<Pair<String, String>, Pair<Object, Exception>> modelIdValueToEmbeddingMap
    ) {
        for (Map.Entry<IngestDocumentWrapper, List<SemanticFieldInfo>> entry : docToSemanticFieldInfoMap.entrySet()) {
            final IngestDocumentWrapper ingestDocumentWrapper = entry.getKey();
            final IngestDocument ingestDocument = ingestDocumentWrapper.getIngestDocument();
            final List<SemanticFieldInfo> semanticFieldInfoList = entry.getValue();
            try {
                setInference(ingestDocument, semanticFieldInfoList, modelIdValueToEmbeddingMap, DEFAULT_PRUNE_RATIO);
            } catch (Exception e) {
                ingestDocumentWrapper.update(ingestDocument, e);
            }
        }
    }

    private void addExceptionToImpactedDocs(@NonNull final Set<IngestDocumentWrapper> impactedDocs, @NonNull final Exception e) {

        for (final IngestDocumentWrapper ingestDocumentWrapper : impactedDocs) {
            // Do not override the previous exception. We do not filter out the doc with exception at the
            // beginning because previous processor may want to ignore the failure which means even the doc
            // already run into some exception we still want to apply this processor.
            //
            // Ideally we should persist all the exceptions the doc run into so that user can view them
            // together rather than fix one then retry and run into another. This is something we can
            // enhance in the future.
            if (ingestDocumentWrapper.getException() == null) {
                ingestDocumentWrapper.update(ingestDocumentWrapper.getIngestDocument(), e);
            }
        }

    }

    @Override
    public String getType() {
        return PROCESSOR_TYPE;
    }
}
