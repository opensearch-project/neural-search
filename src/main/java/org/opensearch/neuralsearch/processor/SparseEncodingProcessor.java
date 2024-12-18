/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.processor;

import java.util.List;
import java.util.Map;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

import lombok.Getter;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.core.action.ActionListener;
import org.opensearch.env.Environment;
import org.opensearch.ingest.IngestDocument;
import org.opensearch.neuralsearch.ml.MLCommonsClientAccessor;
import org.opensearch.neuralsearch.util.prune.PruneType;
import org.opensearch.neuralsearch.util.TokenWeightUtil;

import lombok.extern.log4j.Log4j2;
import org.opensearch.neuralsearch.util.prune.PruneUtils;

/**
 * This processor is used for user input data text sparse encoding processing, model_id can be used to indicate which model user use,
 * and field_map can be used to indicate which fields needs text embedding and the corresponding keys for the sparse encoding results.
 */
@Log4j2
public final class SparseEncodingProcessor extends InferenceProcessor {

    public static final String TYPE = "sparse_encoding";
    public static final String LIST_TYPE_NESTED_MAP_KEY = "sparse_encoding";
    @Getter
    private final PruneType pruneType;
    @Getter
    private final float pruneRatio;

    public SparseEncodingProcessor(
        String tag,
        String description,
        int batchSize,
        String modelId,
        Map<String, Object> fieldMap,
        PruneType pruneType,
        float pruneRatio,
        MLCommonsClientAccessor clientAccessor,
        Environment environment,
        ClusterService clusterService
    ) {
        super(tag, description, batchSize, TYPE, LIST_TYPE_NESTED_MAP_KEY, modelId, fieldMap, clientAccessor, environment, clusterService);
        this.pruneType = pruneType;
        this.pruneRatio = pruneRatio;
    }

    @Override
    public void doExecute(
        IngestDocument ingestDocument,
        Map<String, Object> ProcessMap,
        List<String> inferenceList,
        BiConsumer<IngestDocument, Exception> handler
    ) {
        mlCommonsClientAccessor.inferenceSentencesWithMapResult(this.modelId, inferenceList, ActionListener.wrap(resultMaps -> {
            List<Map<String, Float>> sparseVectors = TokenWeightUtil.fetchListOfTokenWeightMap(resultMaps)
                .stream()
                .map(vector -> PruneUtils.pruneSparseVector(pruneType, pruneRatio, vector))
                .toList();
            setVectorFieldsToDocument(ingestDocument, ProcessMap, sparseVectors);
            handler.accept(ingestDocument, null);
        }, e -> { handler.accept(null, e); }));
    }

    @Override
    public void doBatchExecute(List<String> inferenceList, Consumer<List<?>> handler, Consumer<Exception> onException) {
        mlCommonsClientAccessor.inferenceSentencesWithMapResult(this.modelId, inferenceList, ActionListener.wrap(resultMaps -> {
            List<Map<String, Float>> sparseVectors = TokenWeightUtil.fetchListOfTokenWeightMap(resultMaps)
                .stream()
                .map(vector -> PruneUtils.pruneSparseVector(pruneType, pruneRatio, vector))
                .toList();
            handler.accept(sparseVectors);
        }, onException));
    }
}
