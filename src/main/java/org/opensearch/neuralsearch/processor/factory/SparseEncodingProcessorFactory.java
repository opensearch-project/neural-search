/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.processor.factory;

import static org.opensearch.ingest.ConfigurationUtils.readMap;
import static org.opensearch.ingest.ConfigurationUtils.readStringProperty;
import static org.opensearch.ingest.ConfigurationUtils.readOptionalStringProperty;
import static org.opensearch.ingest.ConfigurationUtils.readDoubleProperty;
import static org.opensearch.neuralsearch.processor.TextEmbeddingProcessor.MODEL_ID_FIELD;
import static org.opensearch.neuralsearch.processor.TextEmbeddingProcessor.FIELD_MAP_FIELD;
import static org.opensearch.neuralsearch.processor.SparseEncodingProcessor.TYPE;

import java.util.Locale;
import java.util.Map;

import org.opensearch.cluster.service.ClusterService;
import org.opensearch.env.Environment;
import org.opensearch.ingest.AbstractBatchingProcessor;
import org.opensearch.neuralsearch.ml.MLCommonsClientAccessor;
import org.opensearch.neuralsearch.processor.SparseEncodingProcessor;

import lombok.extern.log4j.Log4j2;
import org.opensearch.neuralsearch.util.prune.PruneUtils;
import org.opensearch.neuralsearch.util.prune.PruneType;

/**
 * Factory for sparse encoding ingest processor for ingestion pipeline. Instantiates processor based on user provided input.
 */
@Log4j2
public class SparseEncodingProcessorFactory extends AbstractBatchingProcessor.Factory {
    private final MLCommonsClientAccessor clientAccessor;
    private final Environment environment;
    private final ClusterService clusterService;

    public SparseEncodingProcessorFactory(MLCommonsClientAccessor clientAccessor, Environment environment, ClusterService clusterService) {
        super(TYPE);
        this.clientAccessor = clientAccessor;
        this.environment = environment;
        this.clusterService = clusterService;
    }

    @Override
    protected AbstractBatchingProcessor newProcessor(String tag, String description, int batchSize, Map<String, Object> config) {
        String modelId = readStringProperty(TYPE, tag, config, MODEL_ID_FIELD);
        Map<String, Object> fieldMap = readMap(TYPE, tag, config, FIELD_MAP_FIELD);
        // if the field is miss, will return PruneType.None
        PruneType pruneType = PruneType.fromString(readOptionalStringProperty(TYPE, tag, config, PruneUtils.PRUNE_TYPE_FIELD));
        float pruneRatio = 0;
        if (pruneType != PruneType.NONE) {
            // if we have prune type, then prune ratio field must have value
            // readDoubleProperty will throw exception if value is not present
            pruneRatio = readDoubleProperty(TYPE, tag, config, PruneUtils.PRUNE_RATIO_FIELD).floatValue();
            if (!PruneUtils.isValidPruneRatio(pruneType, pruneRatio)) {
                throw new IllegalArgumentException(
                    String.format(
                        Locale.ROOT,
                        "Illegal prune_ratio %f for prune_type: %s. %s",
                        pruneRatio,
                        pruneType.getValue(),
                        PruneUtils.getValidPruneRatioDescription(pruneType)
                    )
                );
            }
        } else if (config.containsKey(PruneUtils.PRUNE_RATIO_FIELD)) {
            // if we don't have prune type, then prune ratio field must not have value
            throw new IllegalArgumentException("prune_ratio field is not supported when prune_type is not provided");
        }

        return new SparseEncodingProcessor(
            tag,
            description,
            batchSize,
            modelId,
            fieldMap,
            pruneType,
            pruneRatio,
            clientAccessor,
            environment,
            clusterService
        );
    }
}
