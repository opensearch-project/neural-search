/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.processor.semantic;

import com.google.common.annotations.VisibleForTesting;
import lombok.NonNull;
import lombok.extern.log4j.Log4j2;
import org.apache.commons.lang3.tuple.Pair;
import org.opensearch.action.get.MultiGetAction;
import org.opensearch.action.get.MultiGetItemResponse;
import org.opensearch.action.get.MultiGetRequest;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.Nullable;
import org.opensearch.core.action.ActionListener;
import org.opensearch.env.Environment;
import org.opensearch.index.analysis.AnalysisRegistry;
import org.opensearch.ingest.AbstractBatchingSystemProcessor;
import org.opensearch.ingest.IngestDocument;
import org.opensearch.ingest.IngestDocumentWrapper;
import org.opensearch.ml.common.MLModel;
import org.opensearch.neuralsearch.mapper.dto.SparseEncodingConfig;
import org.opensearch.neuralsearch.mapper.dto.ChunkingConfig;
import org.opensearch.neuralsearch.ml.MLCommonsClientAccessor;
import org.opensearch.neuralsearch.processor.TextInferenceRequest;
import org.opensearch.neuralsearch.processor.chunker.Chunker;
import org.opensearch.neuralsearch.processor.chunker.FixedTokenLengthChunker;
import org.opensearch.neuralsearch.processor.dto.SemanticFieldInfo;
import org.opensearch.neuralsearch.stats.events.EventStatName;
import org.opensearch.neuralsearch.stats.events.EventStatsManager;
import org.opensearch.neuralsearch.util.TokenWeightUtil;
import org.opensearch.neuralsearch.util.prune.PruneType;
import org.opensearch.neuralsearch.util.prune.PruneUtils;
import org.opensearch.transport.client.OpenSearchClient;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import static org.opensearch.neuralsearch.constants.DocFieldNames.ID_FIELD;
import static org.opensearch.neuralsearch.constants.DocFieldNames.INDEX_FIELD;
import static org.opensearch.neuralsearch.constants.MappingConstants.PATH_SEPARATOR;
import static org.opensearch.neuralsearch.constants.SemanticInfoFieldConstants.CHUNKS_TEXT_FIELD_NAME;
import static org.opensearch.neuralsearch.constants.SemanticInfoFieldConstants.MODEL_ID_FIELD_NAME;
import static org.opensearch.neuralsearch.constants.SemanticInfoFieldConstants.MODEL_NAME_FIELD_NAME;
import static org.opensearch.neuralsearch.constants.SemanticInfoFieldConstants.MODEL_TYPE_FIELD_NAME;
import static org.opensearch.neuralsearch.processor.chunker.Chunker.CHUNK_STRING_COUNT_FIELD;
import static org.opensearch.neuralsearch.processor.chunker.Chunker.MAX_CHUNK_LIMIT_FIELD;
import static org.opensearch.neuralsearch.processor.util.ChunkUtils.chunkList;
import static org.opensearch.neuralsearch.processor.util.ChunkUtils.chunkString;
import static org.opensearch.neuralsearch.processor.util.ProcessorUtils.getMaxTokenCount;
import static org.opensearch.neuralsearch.processor.util.ProcessorUtils.getValueFromSourceByFullPath;
import static org.opensearch.neuralsearch.util.ProcessorDocumentUtils.unflattenIngestDoc;
import static org.opensearch.neuralsearch.util.SemanticMLModelUtils.getModelType;
import static org.opensearch.neuralsearch.util.SemanticMLModelUtils.isDenseModel;
import static org.opensearch.neuralsearch.util.SemanticMappingUtils.getModelId;
import static org.opensearch.neuralsearch.util.SemanticMappingUtils.getSemanticInfoFieldFullPath;
import static org.opensearch.neuralsearch.util.SemanticMappingUtils.isChunkingEnabled;
import static org.opensearch.neuralsearch.util.SemanticMappingUtils.isSkipExistingEmbeddingEnabled;

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
    private final AnalysisRegistry analysisRegistry;

    private final Chunker defaultTextChunker;

    private final static float DEFAULT_PRUNE_RATIO = 0.1f;

    private final OpenSearchClient openSearchClient;

    public SemanticFieldProcessor(
        @Nullable final String tag,
        @Nullable final String description,
        final int batchSize,
        @NonNull final Map<String, Map<String, Object>> pathToFieldConfig,
        @NonNull final MLCommonsClientAccessor mlClientAccessor,
        @NonNull final Environment environment,
        @NonNull final ClusterService clusterService,
        @NonNull final Chunker defaultTextChunker,
        @NonNull final AnalysisRegistry analysisRegistry,
        @NonNull final OpenSearchClient openSearchClient
    ) {
        super(tag, description, batchSize);
        this.pathToFieldConfig = pathToFieldConfig;
        this.mlCommonsClientAccessor = mlClientAccessor;
        this.environment = environment;
        this.clusterService = clusterService;
        this.defaultTextChunker = defaultTextChunker;
        this.openSearchClient = openSearchClient;
        this.analysisRegistry = analysisRegistry;
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
        EventStatsManager.increment(EventStatName.SEMANTIC_FIELD_PROCESSOR_EXECUTIONS);
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
        boolean isChunked = chunk(ingestDocument, semanticFieldInfoList);
        if (isChunked) {
            EventStatsManager.increment(EventStatName.SEMANTIC_FIELD_PROCESSOR_CHUNKING_EXECUTIONS);
        }

        final Set<String> docIdsToCheckReuse = getDocIdsToCheckReuse(List.of(semanticFieldInfoList));
        final Object index = ingestDocument.getSourceAndMetadata().get(INDEX_FIELD);
        if (shouldCheckExistDoc(docIdsToCheckReuse, index)) {
            getExistingDocs(docIdsToCheckReuse, (String) index, (existingDocs, exception) -> {
                if (exception != null) {
                    handler.accept(null, wrapGetExistDocException(exception));
                    return;
                }
                final List<SemanticFieldInfo> semanticFieldInfoToGenerateEmbedding = applyReusableEmbeddingsAndFilterUnprocessedFields(
                    ingestDocument,
                    semanticFieldInfoList,
                    existingDocs
                );
                if (semanticFieldInfoToGenerateEmbedding.isEmpty()) {
                    handler.accept(ingestDocument, null);
                } else {
                    setModelInfo(ingestDocument, semanticFieldInfoToGenerateEmbedding);
                    generateAndSetEmbedding(ingestDocument, semanticFieldInfoToGenerateEmbedding, handler);
                }
            });
            return;
        }

        setModelInfo(ingestDocument, semanticFieldInfoList);
        generateAndSetEmbedding(ingestDocument, semanticFieldInfoList, handler);
    }

    private Exception wrapGetExistDocException(@NonNull final Exception exception) {
        return new RuntimeException(
            String.format(
                Locale.ROOT,
                "Failed to get existing docs to check embedding reusability for the semantic field. Error: %s",
                exception.getMessage()
            ),
            exception
        );
    }

    private List<SemanticFieldInfo> applyReusableEmbeddingsAndFilterUnprocessedFields(
        @NonNull IngestDocument ingestDocument,
        @NonNull List<SemanticFieldInfo> semanticFieldInfoList,
        @NonNull final Map<String, Map<String, Object>> existingDocs
    ) {
        return semanticFieldInfoList.stream().filter(info -> {
            // If skip_exist_embedding is not enabled or no doc ID, we need to generate embedding
            if (info.getSkipExistingEmbedding() == false || info.getDocId() == null) {
                return true;
            }

            // If no existing doc then no reuse
            final Map<String, Object> existingDoc = existingDocs.get(info.getDocId());
            if (existingDoc == null) {
                return true;
            }

            // If original value is changed then no reuse
            final Object existingValue = getValueFromSourceByFullPath(existingDoc, info.getSemanticFieldFullPathInDoc());
            if (Objects.equals(info.getValue(), existingValue) == false) {
                return true;
            }

            // If the model id is changed then no reuse
            final Object modelInfo = getValueFromSourceByFullPath(existingDoc, info.getFullPathForModelInfoInDoc());
            if (!(modelInfo instanceof Map<?, ?> modelMap)
                || Objects.equals(modelMap.get(MODEL_ID_FIELD_NAME), info.getModelId()) == false) {
                return true;
            }

            // If the chunked texts are changed then no reuse
            if (info.getChunkingEnabled()) {
                final Object chunksObj = getValueFromSourceByFullPath(existingDoc, info.getFullPathForChunksInDoc());
                if (!(chunksObj instanceof List<?> chunks)) {
                    return true;
                }

                final List<Object> existingChunkedTexts = new ArrayList<>();
                for (final Object chunk : chunks) {
                    if (chunk instanceof Map<?, ?> chunkMap) {
                        existingChunkedTexts.add(chunkMap.get(CHUNKS_TEXT_FIELD_NAME));
                    }
                }

                if (Objects.equals(existingChunkedTexts, info.getChunks()) == false) {
                    return true;
                }
            }

            // All checks passed — reuse existing embedding and skip generation
            final Object semanticInfo = getValueFromSourceByFullPath(existingDoc, info.getSemanticInfoFullPathInDoc());
            ingestDocument.setFieldValue(info.getSemanticInfoFullPathInDoc(), semanticInfo);
            return false;
        }).toList();
    }

    private boolean shouldCheckExistDoc(@NonNull final Set<String> docIdsToCheckReuse, final Object index) {
        return docIdsToCheckReuse.isEmpty() == false && index instanceof String;
    }

    private Set<String> getDocIdsToCheckReuse(@NonNull final Collection<List<SemanticFieldInfo>> semanticFieldInfoList) {
        final Set<String> docIdsToCheckReuse = new HashSet<>();
        for (List<SemanticFieldInfo> semanticFieldInfos : semanticFieldInfoList) {
            for (SemanticFieldInfo semanticFieldInfo : semanticFieldInfos) {
                if (semanticFieldInfo.getSkipExistingEmbedding() && Objects.nonNull(semanticFieldInfo.getDocId())) {
                    docIdsToCheckReuse.add(semanticFieldInfo.getDocId());
                }
            }
        }
        return docIdsToCheckReuse;
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
            ingestDocument.setFieldValue(
                semanticFieldInfo.getFullPathForModelInfoInDoc(),
                modelIdToInfoMap.get(semanticFieldInfo.getModelId())
            );
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
                setInference(ingestDocument, semanticFieldInfoList, modelIdValueToEmbeddingMap);
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
        @NonNull final Map<Pair<String, String>, Pair<Object, Exception>> modelIdValueToEmbeddingMap
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
                if (!isDenseModel) {
                    final SparseEncodingConfig sparseEncodingConfig = semanticFieldInfo.getSparseEncodingConfig();
                    final PruneType pruneType = (sparseEncodingConfig != null && sparseEncodingConfig.getPruneType() != null)
                        ? sparseEncodingConfig.getPruneType()
                        : PruneType.MAX_RATIO;
                    if (PruneType.NONE.equals(pruneType) == false) {
                        final Float pruneRatio = (sparseEncodingConfig != null && sparseEncodingConfig.getPruneRatio() != null)
                            ? sparseEncodingConfig.getPruneRatio()
                            : DEFAULT_PRUNE_RATIO;
                        embedding = PruneUtils.pruneSparseVector(pruneType, pruneRatio, (Map<String, Float>) embedding);
                    }
                }
                final String embeddingFullPath = semanticFieldInfo.getFullPathForEmbeddingInDoc(i);
                ingestDocument.setFieldValue(embeddingFullPath, embedding);
            }
        }
    }

    private List<SemanticFieldInfo> getSemanticFieldInfo(IngestDocument ingestDocument) {
        final List<SemanticFieldInfo> semanticFieldInfos = new ArrayList<>();
        final Object doc = ingestDocument.getSourceAndMetadata();
        String docId = null;
        if (doc instanceof Map<?, ?> docMap) {
            docId = (String) docMap.get(ID_FIELD);
        }
        for (Map.Entry<String, Map<String, Object>> entry : pathToFieldConfig.entrySet()) {
            final String path = entry.getKey();
            final Map<String, Object> config = entry.getValue();
            collectSemanticFieldInfo(doc, path.split("\\."), config, 0, "", semanticFieldInfos, docId);
        }
        return semanticFieldInfos;
    }

    private boolean chunk(@NonNull final IngestDocument ingestDocument, @NonNull final List<SemanticFieldInfo> semanticFieldInfoList) {
        final Map<String, Object> sourceAndMetadataMap = ingestDocument.getSourceAndMetadata();
        int maxTokenCount = getMaxTokenCount(sourceAndMetadataMap, environment.settings(), clusterService);
        boolean isChunked = false;
        for (SemanticFieldInfo semanticFieldInfo : semanticFieldInfoList) {
            if (semanticFieldInfo.getChunkingEnabled()) {
                isChunked = true;
                if (semanticFieldInfo.getChunkers() == null || semanticFieldInfo.getChunkers().isEmpty()) {
                    semanticFieldInfo.setChunkers(List.of(defaultTextChunker));
                }
                executeChunkers(semanticFieldInfo, maxTokenCount);

                setChunkedText(ingestDocument, semanticFieldInfo);
            } else {
                // When chunking is disabled, treat the original text as a single chunk to keep the subsequent logic consistent.
                semanticFieldInfo.setChunks(List.of(semanticFieldInfo.getValue()));
            }
        }
        return isChunked;
    }

    private void setChunkedText(@NonNull final IngestDocument ingestDocument, @NonNull final SemanticFieldInfo semanticFieldInfo) {
        final List<Map<String, Object>> chunks = new ArrayList<>();

        for (final String text : semanticFieldInfo.getChunks()) {
            final Map<String, Object> chunk = new HashMap<>();
            chunk.put(CHUNKS_TEXT_FIELD_NAME, text);
            chunks.add(chunk);
        }
        ingestDocument.setFieldValue(semanticFieldInfo.getFullPathForChunksInDoc(), chunks);
    }

    private void executeChunkers(@NonNull final SemanticFieldInfo semanticFieldInfo, int maxTokenCount) {
        for (Chunker chunker : semanticFieldInfo.getChunkers()) {
            final Map<String, Object> runtimeParameters = new HashMap<>();
            final List<String> chunks = semanticFieldInfo.getChunks();
            final boolean isFirstChunker = chunks == null;
            runtimeParameters.put(FixedTokenLengthChunker.MAX_TOKEN_COUNT_FIELD, maxTokenCount);
            runtimeParameters.put(CHUNK_STRING_COUNT_FIELD, isFirstChunker ? 1 : chunks.size());
            runtimeParameters.put(MAX_CHUNK_LIMIT_FIELD, chunker.getMaxChunkLimit());

            final List<String> chunkedText = new ArrayList<>();
            if (isFirstChunker) {
                chunkedText.addAll(chunkString(chunker, semanticFieldInfo.getValue(), runtimeParameters));
            } else {
                chunkedText.addAll(chunkList(chunker, chunks, runtimeParameters));
            }
            semanticFieldInfo.setChunks(chunkedText);
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
        @NonNull final List<SemanticFieldInfo> semanticFieldInfoList,
        @Nullable final String docId
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
            collectSemanticFieldInfo(nextNode, pathParts, fieldConfig, depth + 1, newPath, semanticFieldInfoList, docId);
        } else if (depth < pathParts.length && node instanceof List<?> listNode) {
            for (int i = 0; i < listNode.size(); i++) {
                final Object listItem = listNode.get(i);
                final String indexedPath = currentPath + PATH_SEPARATOR + i;
                collectSemanticFieldInfo(listItem, pathParts, fieldConfig, depth, indexedPath, semanticFieldInfoList, docId);
            }
        } else if (depth == pathParts.length) {
            // the current node is the value of the semantic field, and it should be a string value
            final String pathToSemanticField = String.join(PATH_SEPARATOR, pathParts);
            if (node instanceof String == false) {
                throw new IllegalArgumentException(
                    String.format(
                        Locale.ROOT,
                        "Expect the semantic field at path: %s to be a string but found: %s.",
                        pathToSemanticField,
                        node.getClass()
                    )
                );
            }

            final SemanticFieldInfo semanticFieldInfo = SemanticFieldInfo.builder()
                .value(node.toString())
                .modelId(getModelId(fieldConfig, pathToSemanticField))
                .semanticFieldFullPathInMapping(String.join(PATH_SEPARATOR, pathParts))
                .semanticFieldFullPathInDoc(currentPath)
                // Here we should use the currentPath because it has the inter index if there is any inter nested object
                // By using this path we can handle the nested object properly when we use it to set the data for the semantic field
                .semanticInfoFullPathInDoc(getSemanticInfoFieldFullPath(fieldConfig, currentPath, pathToSemanticField))
                .chunkingEnabled(isChunkingEnabled(fieldConfig, pathToSemanticField))
                .sparseEncodingConfig(new SparseEncodingConfig(fieldConfig))
                .skipExistingEmbedding(isSkipExistingEmbeddingEnabled(fieldConfig, pathToSemanticField))
                .docId(docId)
                .build();
            semanticFieldInfo.setChunkingConfig(new ChunkingConfig(fieldConfig), analysisRegistry);

            semanticFieldInfoList.add(semanticFieldInfo);
        }
    }

    @Override
    public void subBatchExecute(List<IngestDocumentWrapper> ingestDocumentWrappers, Consumer<List<IngestDocumentWrapper>> handler) {
        EventStatsManager.increment(EventStatName.SEMANTIC_FIELD_PROCESSOR_EXECUTIONS);
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
        boolean isChunked = false;

        for (Map.Entry<IngestDocumentWrapper, List<SemanticFieldInfo>> entry : docToSemanticFieldInfoMap.entrySet()) {
            try {
                final IngestDocument ingestDoc = entry.getKey().getIngestDocument();
                final List<SemanticFieldInfo> fields = entry.getValue();

                if (chunk(ingestDoc, fields)) {
                    isChunked = true;
                }
            } catch (Exception e) {
                logAndUpdate(entry.getKey(), "chunk", e);
            }
        }

        if (isChunked) {
            EventStatsManager.increment(EventStatName.SEMANTIC_FIELD_PROCESSOR_CHUNKING_EXECUTIONS);
        }

        final Set<String> docIdsToCheckReuse = getDocIdsToCheckReuse(docToSemanticFieldInfoMap.values());
        // All docs should be in the same index so simply get the index from the first doc.
        final Object index = ingestDocumentWrappers.getFirst().getIngestDocument().getSourceAndMetadata().get(INDEX_FIELD);

        if (shouldCheckExistDoc(docIdsToCheckReuse, index)) {
            getExistingDocs(docIdsToCheckReuse, (String) index, (existingDocs, exception) -> {
                if (exception != null) {
                    addExceptionToImpactedDocs(docToSemanticFieldInfoMap.keySet(), wrapGetExistDocException(exception));
                    handler.accept(ingestDocumentWrappers);
                    return;
                }

                Map<IngestDocumentWrapper, List<SemanticFieldInfo>> docToFieldsNeedingEmbedding = new HashMap<>();
                docToSemanticFieldInfoMap.forEach((docWrapper, infos) -> {
                    List<SemanticFieldInfo> fieldsNeedingEmbedding = applyReusableEmbeddingsAndFilterUnprocessedFields(
                        docWrapper.getIngestDocument(),
                        infos,
                        existingDocs
                    );
                    if (fieldsNeedingEmbedding.isEmpty() == false) {
                        docToFieldsNeedingEmbedding.put(docWrapper, fieldsNeedingEmbedding);
                    }
                });

                if (docToFieldsNeedingEmbedding.isEmpty()) {
                    handler.accept(ingestDocumentWrappers);
                } else {
                    batchSetModelInfo(docToFieldsNeedingEmbedding);
                    batchGenerateAndSetEmbedding(ingestDocumentWrappers, docToFieldsNeedingEmbedding, handler);
                }
            });
            return;
        }
        batchSetModelInfo(docToSemanticFieldInfoMap);
        batchGenerateAndSetEmbedding(ingestDocumentWrappers, docToSemanticFieldInfoMap, handler);
    }

    private void batchSetModelInfo(Map<IngestDocumentWrapper, List<SemanticFieldInfo>> docToFieldsNeedingEmbedding) {
        docToFieldsNeedingEmbedding.forEach((docWrapper, infos) -> {
            try {
                setModelInfo(docWrapper.getIngestDocument(), infos);
            } catch (Exception e) {
                logAndUpdate(docWrapper, "set model info", e);
            }
        });
    }

    private void logAndUpdate(@NonNull final IngestDocumentWrapper wrapper, @NonNull final String operation, @NonNull final Exception e) {
        final IngestDocument doc = wrapper.getIngestDocument();
        log.error(
            String.format(Locale.ROOT, "Failed to %s ingest document %s. Root cause: %s", operation, doc.toString(), e.getMessage()),
            e
        );
        if (wrapper.getException() == null) {
            wrapper.update(doc, e);
        }
    }

    private void getExistingDocs(
        @NonNull final Set<String> docIds,
        @NonNull final String index,
        @NonNull final BiConsumer<Map<String, Map<String, Object>>, Exception> handler
    ) {
        MultiGetRequest multiGetRequest = new MultiGetRequest();
        for (String docId : docIds) {
            multiGetRequest.add(index, docId);
        }
        openSearchClient.execute(MultiGetAction.INSTANCE, multiGetRequest, ActionListener.wrap(response -> {
            MultiGetItemResponse[] items = response.getResponses();
            if (items == null || items.length == 0) {
                handler.accept(Collections.emptyMap(), null);
                return;
            }
            Map<String, Map<String, Object>> existingDocs = new HashMap<>();
            for (MultiGetItemResponse item : items) {
                if (item.getResponse() != null && item.getResponse().isExists()) {
                    existingDocs.put(item.getId(), item.getResponse().getSourceAsMap());
                }
            }
            handler.accept(existingDocs, null);
        }, e -> handler.accept(null, e)));
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
                    null,
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
                setInference(ingestDocument, semanticFieldInfoList, modelIdValueToEmbeddingMap);
            } catch (Exception e) {
                ingestDocumentWrapper.update(ingestDocument, e);
            }
        }
    }

    private void addExceptionToImpactedDocs(@NonNull final Collection<IngestDocumentWrapper> impactedDocs, @NonNull final Exception e) {

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

    @VisibleForTesting
    public int getBatchSize() {
        return batchSize;
    }
}
