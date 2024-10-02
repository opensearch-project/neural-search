/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.processor.factory;

import java.util.Map;

import org.opensearch.cluster.service.ClusterService;
import org.opensearch.env.Environment;
import org.opensearch.index.analysis.AnalysisRegistry;
import org.opensearch.ingest.Processor;
import org.opensearch.neuralsearch.processor.TextChunkingProcessor;
import static org.opensearch.neuralsearch.processor.TextChunkingProcessor.TYPE;
import static org.opensearch.neuralsearch.processor.TextChunkingProcessor.FIELD_MAP_FIELD;
import static org.opensearch.neuralsearch.processor.TextChunkingProcessor.ALGORITHM_FIELD;
import static org.opensearch.neuralsearch.processor.TextChunkingProcessor.IGNORE_MISSING;
import static org.opensearch.neuralsearch.processor.TextChunkingProcessor.DEFAULT_IGNORE_MISSING;
import static org.opensearch.ingest.ConfigurationUtils.readMap;
import static org.opensearch.ingest.ConfigurationUtils.readBooleanProperty;

/**
 * Factory for chunking ingest processor for ingestion pipeline.
 * Instantiates processor based on user provided input, which includes:
 * 1. field_map: the input and output fields specified by the user
 * 2. algorithm: chunking algorithm and its parameters
 */
public class TextChunkingProcessorFactory implements Processor.Factory {

    private final Environment environment;

    private final ClusterService clusterService;

    private final AnalysisRegistry analysisRegistry;

    public TextChunkingProcessorFactory(Environment environment, ClusterService clusterService, AnalysisRegistry analysisRegistry) {
        this.environment = environment;
        this.clusterService = clusterService;
        this.analysisRegistry = analysisRegistry;
    }

    @Override
    public TextChunkingProcessor create(
        Map<String, Processor.Factory> registry,
        String processorTag,
        String description,
        Map<String, Object> config
    ) throws Exception {
        Map<String, Object> fieldMap = readMap(TYPE, processorTag, config, FIELD_MAP_FIELD);
        Map<String, Object> algorithmMap = readMap(TYPE, processorTag, config, ALGORITHM_FIELD);
        boolean ignoreMissing = readBooleanProperty(TYPE, processorTag, config, IGNORE_MISSING, DEFAULT_IGNORE_MISSING);
        return new TextChunkingProcessor(
            processorTag,
            description,
            fieldMap,
            algorithmMap,
            ignoreMissing,
            environment,
            clusterService,
            analysisRegistry
        );
    }
}
