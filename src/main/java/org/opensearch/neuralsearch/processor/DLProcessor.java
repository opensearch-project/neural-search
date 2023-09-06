/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.neuralsearch.processor;

import org.apache.commons.lang3.StringUtils;
import org.opensearch.core.action.ActionListener;
import org.opensearch.env.Environment;
import org.opensearch.ingest.AbstractProcessor;
import org.opensearch.ingest.IngestDocument;
import org.opensearch.neuralsearch.ml.MLCommonsClientAccessor;

import java.util.List;
import java.util.Map;
import java.util.function.BiConsumer;

public abstract class DLProcessor extends AbstractProcessor {
    public static final String TYPE = "text_embedding";
    public static final String MODEL_ID_FIELD = "model_id";
    public static final String FIELD_MAP_FIELD = "field_map";

    private static final String LIST_TYPE_NESTED_MAP_KEY = "knn";

    @VisibleForTesting
    private final String modelId;

    private final Map<String, Object> fieldMap;

    private final MLCommonsClientAccessor mlCommonsClientAccessor;

    private final Environment environment;

    public DLProcessor(
            String tag,
            String description,
            String modelId,
            Map<String, Object> fieldMap,
            MLCommonsClientAccessor clientAccessor,
            Environment environment
    ) {
        super(tag, description);
        if (StringUtils.isBlank(modelId)) throw new IllegalArgumentException("model_id is null or empty, can not process it");
        validateFieldMapConfiguration(fieldMap);

        this.modelId = modelId;
        this.fieldMap = fieldMap;
        this.mlCommonsClientAccessor = clientAccessor;
        this.environment = environment;
    }
    public abstract void validateFieldMapConfiguration(Map<String, Object> fieldMap);

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
    public void execute(IngestDocument ingestDocument, BiConsumer<IngestDocument, Exception> handler) {
        // When received a bulk indexing request, the pipeline will be executed in this method, (see
        // https://github.com/opensearch-project/OpenSearch/blob/main/server/src/main/java/org/opensearch/action/bulk/TransportBulkAction.java#L226).
        // Before the pipeline execution, the pipeline will be marked as resolved (means executed),
        // and then this overriding method will be invoked when executing the text embedding processor.
        // After the inference completes, the handler will invoke the doInternalExecute method again to run actual write operation.
        try {
            validateIngestFieldsValue(ingestDocument);
            Map<String, Object> knnMap = buildMapWithKnnKeyAndOriginalValue(ingestDocument);
            List<String> inferenceList = createInferenceList(knnMap);
            if (inferenceList.size() == 0) {
                handler.accept(ingestDocument, null);
            } else {
                mlCommonsClientAccessor.inferenceSentences(this.modelId, inferenceList, ActionListener.wrap(vectors -> {
                    setVectorFieldsToDocument(ingestDocument, knnMap, vectors);
                    handler.accept(ingestDocument, null);
                }, e -> { handler.accept(null, e); }));
            }
        } catch (Exception e) {
            handler.accept(null, e);
        }
    }

    public abstract void validateIngestFieldsValue(IngestDocument ingestDocument);



}
