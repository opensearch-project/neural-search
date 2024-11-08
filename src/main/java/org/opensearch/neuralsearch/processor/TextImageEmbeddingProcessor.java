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
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.core.action.ActionListener;
import org.opensearch.env.Environment;
import org.opensearch.index.mapper.IndexFieldMapper;
import org.opensearch.ingest.AbstractProcessor;
import org.opensearch.ingest.IngestDocument;
import org.opensearch.neuralsearch.ml.MLCommonsClientAccessor;

import com.google.common.annotations.VisibleForTesting;

import lombok.extern.log4j.Log4j2;
import org.opensearch.neuralsearch.ml.MLCommonsClientAccessor.InferenceRequest;
import org.opensearch.neuralsearch.util.ProcessorDocumentUtils;

/**
 * This processor is used for user input data text and image embedding processing, model_id can be used to indicate which model user use,
 * and field_map can be used to indicate which fields needs embedding and the corresponding keys for the embedding results.
 */
@Log4j2
public class TextImageEmbeddingProcessor extends AbstractProcessor {

    public static final String TYPE = "text_image_embedding";
    public static final String MODEL_ID_FIELD = "model_id";
    public static final String EMBEDDING_FIELD = "embedding";
    public static final String FIELD_MAP_FIELD = "field_map";
    public static final String TEXT_FIELD_NAME = "text";
    public static final String IMAGE_FIELD_NAME = "image";
    public static final String INPUT_TEXT = "inputText";
    public static final String INPUT_IMAGE = "inputImage";
    private static final Set<String> VALID_FIELD_NAMES = Set.of(TEXT_FIELD_NAME, IMAGE_FIELD_NAME);

    private final String modelId;
    private final String embedding;
    private final Map<String, String> fieldMap;

    private final MLCommonsClientAccessor mlCommonsClientAccessor;

    private final Environment environment;
    private final ClusterService clusterService;

    public TextImageEmbeddingProcessor(
        final String tag,
        final String description,
        final String modelId,
        final String embedding,
        final Map<String, String> fieldMap,
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
            validateEmbeddingFieldsValue(ingestDocument);
            Map<String, String> knnMap = buildMapWithKnnKeyAndOriginalValue(ingestDocument);
            Map<String, String> inferenceMap = createInferences(knnMap);
            if (inferenceMap.isEmpty()) {
                handler.accept(ingestDocument, null);
            } else {
                mlCommonsClientAccessor.inferenceSentencesMap(
                    InferenceRequest.builder().modelId(this.modelId).inputObjects(inferenceMap).build(),
                    ActionListener.wrap(vectors -> {
                        setVectorFieldsToDocument(ingestDocument, vectors);
                        handler.accept(ingestDocument, null);
                    }, e -> { handler.accept(null, e); })
                );
            }
        } catch (Exception e) {
            handler.accept(null, e);
        }

    }

    private void setVectorFieldsToDocument(final IngestDocument ingestDocument, final List<Float> vectors) {
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
            if (!(sourceAndMetadataMap.get(originalKey) instanceof String)) {
                throw new IllegalArgumentException("Unsupported format of the field in the document, value must be a string");
            }
            mapWithKnnKeys.put(originalKey, (String) sourceAndMetadataMap.get(originalKey));
        }
        return mapWithKnnKeys;
    }

    @SuppressWarnings({ "unchecked" })
    @VisibleForTesting
    Map<String, Object> buildTextEmbeddingResult(final String knnKey, List<Float> modelTensorList) {
        Map<String, Object> result = new LinkedHashMap<>();
        result.put(knnKey, modelTensorList);
        return result;
    }

    private void validateEmbeddingFieldsValue(final IngestDocument ingestDocument) {
        Map<String, Object> sourceAndMetadataMap = ingestDocument.getSourceAndMetadata();
        String indexName = sourceAndMetadataMap.get(IndexFieldMapper.NAME).toString();
        ProcessorDocumentUtils.validateMapTypeValue(
            FIELD_MAP_FIELD,
            sourceAndMetadataMap,
            fieldMap,
            indexName,
            clusterService,
            environment,
            false
        );
    }

    @Override
    public String getType() {
        return TYPE;
    }
}
