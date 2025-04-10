/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.processor;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.BiConsumer;

import org.apache.commons.lang3.StringUtils;
import org.opensearch.action.get.GetAction;
import org.opensearch.action.get.GetRequest;
import org.opensearch.action.get.GetResponse;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.core.action.ActionListener;
import org.opensearch.env.Environment;
import org.opensearch.ingest.AbstractProcessor;
import org.opensearch.ingest.IngestDocument;
import org.opensearch.neuralsearch.ml.MLCommonsClientAccessor;

import com.google.common.annotations.VisibleForTesting;

import lombok.extern.log4j.Log4j2;
import org.opensearch.neuralsearch.processor.optimization.TextImageEmbeddingInferenceFilter;
import org.opensearch.transport.client.OpenSearchClient;

/**
 * This processor is used for user input data text and image embedding processing, model_id can be used to indicate which model user use,
 * and field_map can be used to indicate which fields needs embedding and the corresponding keys for the embedding results.
 */
@Log4j2
public class TextImageEmbeddingProcessor extends AbstractProcessor {

    public static final String TYPE = "text_image_embedding";
    public static final String MODEL_ID_FIELD = "model_id";
    public static final String EMBEDDING_FIELD = "embedding";
    public static final boolean DEFAULT_SKIP_EXISTING = false;
    public static final String SKIP_EXISTING = "skip_existing";
    public static final String FIELD_MAP_FIELD = "field_map";
    public static final String TEXT_FIELD_NAME = "text";
    public static final String IMAGE_FIELD_NAME = "image";
    public static final String INPUT_TEXT = "inputText";
    public static final String INPUT_IMAGE = "inputImage";
    private static final String INDEX_FIELD = "_index";
    private static final String ID_FIELD = "_id";
    private static final Set<String> VALID_FIELD_NAMES = Set.of(TEXT_FIELD_NAME, IMAGE_FIELD_NAME);

    private final String modelId;
    private final String embedding;
    private final Map<String, String> fieldMap;
    private final boolean skipExisting;
    private final OpenSearchClient openSearchClient;
    private final MLCommonsClientAccessor mlCommonsClientAccessor;
    private final TextImageEmbeddingInferenceFilter inferenceFilter;
    private final Environment environment;
    private final ClusterService clusterService;

    public TextImageEmbeddingProcessor(
        final String tag,
        final String description,
        final String modelId,
        final String embedding,
        final Map<String, String> fieldMap,
        final boolean skipExisting,
        final TextImageEmbeddingInferenceFilter inferenceFilter,
        final OpenSearchClient openSearchClient,
        final MLCommonsClientAccessor clientAccessor,
        final Environment environment,
        final ClusterService clusterService
    ) {
        super(tag, description);
        if (StringUtils.isBlank(modelId)) throw new IllegalArgumentException("model_id is null or empty, can not process it");
        validateEmbeddingConfiguration(fieldMap);

        this.modelId = modelId;
        this.embedding = embedding;
        this.fieldMap = fieldMap;
        this.mlCommonsClientAccessor = clientAccessor;
        this.environment = environment;
        this.clusterService = clusterService;
        this.skipExisting = skipExisting;
        this.inferenceFilter = inferenceFilter;
        this.openSearchClient = openSearchClient;
    }

    private void validateEmbeddingConfiguration(final Map<String, String> fieldMap) {
        if (fieldMap == null
            || fieldMap.isEmpty()
            || fieldMap.entrySet().stream().anyMatch(x -> StringUtils.isBlank(x.getKey()) || Objects.isNull(x.getValue()))) {
            throw new IllegalArgumentException("Unable to create the TextImageEmbedding processor as field_map has invalid key or value");
        }

        if (fieldMap.entrySet().stream().anyMatch(entry -> !VALID_FIELD_NAMES.contains(entry.getKey()))) {
            throw new IllegalArgumentException(
                String.format(
                    Locale.ROOT,
                    "Unable to create the TextImageEmbedding processor with provided field name(s). Following names are supported [%s]",
                    String.join(",", VALID_FIELD_NAMES)
                )
            );
        }
    }

    @Override
    public IngestDocument execute(IngestDocument ingestDocument) {
        return ingestDocument;
    }

    /**
     * This method will be invoked by PipelineService to make async inference and then delegate the handler to
     * process the inference response or failure.
     * @param ingestDocument {@link IngestDocument} which is the document passed to processor.
     * @param handler {@link BiConsumer} which is the handler which can be used after the inference task is done.
     */
    @Override
    public void execute(final IngestDocument ingestDocument, final BiConsumer<IngestDocument, Exception> handler) {
        try {
            Map<String, String> knnMap = buildMapWithKnnKeyAndOriginalValue(ingestDocument);
            Map<String, String> inferenceMap = createInferences(knnMap);
            if (inferenceMap.isEmpty()) {
                handler.accept(ingestDocument, null);
                return;
            }
            if (skipExisting == false) {
                generateAndSetInference(ingestDocument, inferenceMap, handler);
                return;
            }
            // if skipExisting flag is turned on, eligible inference text and images will be compared and filtered after embeddings are
            // copied
            Object index = ingestDocument.getSourceAndMetadata().get(INDEX_FIELD);
            Object id = ingestDocument.getSourceAndMetadata().get(ID_FIELD);
            if (Objects.isNull(index) || Objects.isNull(id)) {
                generateAndSetInference(ingestDocument, inferenceMap, handler);
                return;
            }
            openSearchClient.execute(
                GetAction.INSTANCE,
                new GetRequest(index.toString(), id.toString()),
                ActionListener.wrap(
                    response -> reuseOrGenerateEmbedding(response, ingestDocument, knnMap, inferenceMap, handler),
                    e -> handler.accept(null, e)
                )
            );
        } catch (Exception e) {
            handler.accept(null, e);
        }

    }

    private void setVectorFieldsToDocument(final IngestDocument ingestDocument, final List<Number> vectors) {
        Objects.requireNonNull(vectors, "embedding failed, inference returns null result!");
        log.debug("Text embedding result fetched, starting build vector output!");
        Map<String, Object> textEmbeddingResult = buildTextEmbeddingResult(this.embedding, vectors);
        textEmbeddingResult.forEach(ingestDocument::setFieldValue);
    }

    @SuppressWarnings({ "unchecked" })
    private Map<String, String> createInferences(final Map<String, String> knnKeyMap) {
        Map<String, String> texts = new HashMap<>();
        if (fieldMap.containsKey(TEXT_FIELD_NAME) && knnKeyMap.containsKey(fieldMap.get(TEXT_FIELD_NAME))) {
            texts.put(INPUT_TEXT, knnKeyMap.get(fieldMap.get(TEXT_FIELD_NAME)));
        }
        if (fieldMap.containsKey(IMAGE_FIELD_NAME) && knnKeyMap.containsKey(fieldMap.get(IMAGE_FIELD_NAME))) {
            texts.put(INPUT_IMAGE, knnKeyMap.get(fieldMap.get(IMAGE_FIELD_NAME)));
        }
        return texts;
    }

    @VisibleForTesting
    Map<String, String> buildMapWithKnnKeyAndOriginalValue(final IngestDocument ingestDocument) {
        Map<String, Object> sourceAndMetadataMap = ingestDocument.getSourceAndMetadata();
        Map<String, String> mapWithKnnKeys = new LinkedHashMap<>();
        for (Map.Entry<String, String> fieldMapEntry : fieldMap.entrySet()) {
            String originalKey = fieldMapEntry.getValue(); // field from ingest document that we need to sent as model input, part of
                                                           // processor definition

            if (!sourceAndMetadataMap.containsKey(originalKey)) {
                continue;
            }

            Object metaValue = sourceAndMetadataMap.get(originalKey);
            if (Objects.isNull(metaValue)) {
                throw new IllegalArgumentException(
                    String.format(
                        Locale.getDefault(),
                        "Unsupported format of the field in the document, %s value must be a non-empty string. Currently it is null",
                        originalKey
                    )
                );
            } else if ((metaValue instanceof String) == false || Objects.isNull(metaValue) || StringUtils.isBlank((String) metaValue)) {
                throw new IllegalArgumentException(
                    String.format(
                        Locale.getDefault(),
                        "Unsupported format of the field in the document, %s value must be a non-empty string. Currently it is '%s'. Type is %s",
                        originalKey,
                        metaValue,
                        metaValue.getClass()
                    )
                );
            }
            mapWithKnnKeys.put(originalKey, (String) sourceAndMetadataMap.get(originalKey));
        }
        return mapWithKnnKeys;
    }

    @SuppressWarnings({ "unchecked" })
    @VisibleForTesting
    Map<String, Object> buildTextEmbeddingResult(final String knnKey, List<Number> modelTensorList) {
        Map<String, Object> result = new LinkedHashMap<>();
        result.put(knnKey, modelTensorList);
        return result;
    }

    @Override
    public String getType() {
        return TYPE;
    }

    /**
     * This method invokes inference call through mlCommonsClientAccessor and populates retrieved embeddings to ingestDocument
     *
     * @param ingestDocument ingestDocument to populate embeddings to
     * @param inferenceMap map indicating the path in ingestDocument to populate embeddings
     * @param handler SourceAndMetadataMap of ingestDocument Document
     *
     */
    private void generateAndSetInference(
        IngestDocument ingestDocument,
        Map<String, String> inferenceMap,
        BiConsumer<IngestDocument, Exception> handler
    ) {
        mlCommonsClientAccessor.inferenceSentencesMap(
            MapInferenceRequest.builder().modelId(this.modelId).inputObjects(inferenceMap).build(),
            ActionListener.wrap(vectors -> {
                setVectorFieldsToDocument(ingestDocument, vectors);
                handler.accept(ingestDocument, null);
            }, e -> { handler.accept(null, e); })
        );
    }

    // This method validates and filters given knnMap and inferenceMap after response is successfully retrieved from get operation.
    private void reuseOrGenerateEmbedding(
        GetResponse response,
        IngestDocument ingestDocument,
        Map<String, String> knnMap,
        Map<String, String> inferenceMap,
        BiConsumer<IngestDocument, Exception> handler
    ) {
        final Map<String, Object> existingDocument = response.getSourceAsMap();
        if (existingDocument == null || existingDocument.isEmpty()) {
            generateAndSetInference(ingestDocument, inferenceMap, handler);
            return;
        }
        // filter given knnMap by comparing existing document with ingestDocument
        Map<String, String> filteredKnnMap = inferenceFilter.filterAndCopyExistingEmbeddings(
            ingestDocument,
            existingDocument,
            knnMap,
            embedding
        );
        // create inference map based on filtered knnMap
        Map<String, String> filteredInferenceMap = createInferences(filteredKnnMap);
        if (filteredInferenceMap.isEmpty()) {
            handler.accept(ingestDocument, null);
        } else {
            generateAndSetInference(ingestDocument, filteredInferenceMap, handler);
        }
    }
}
