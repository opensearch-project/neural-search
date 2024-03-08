/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.processor.factory;

import java.util.Map;

import org.opensearch.cluster.service.ClusterService;
import org.opensearch.env.Environment;
import org.opensearch.index.analysis.AnalysisRegistry;
import org.opensearch.indices.IndicesService;
import org.opensearch.ingest.Processor;
import org.opensearch.neuralsearch.processor.DocumentChunkingProcessor;
import static org.opensearch.neuralsearch.processor.DocumentChunkingProcessor.TYPE;
import static org.opensearch.neuralsearch.processor.DocumentChunkingProcessor.FIELD_MAP_FIELD;
import static org.opensearch.neuralsearch.processor.DocumentChunkingProcessor.ALGORITHM_FIELD;
import static org.opensearch.ingest.ConfigurationUtils.readMap;

/**
 * Factory for chunking ingest processor for ingestion pipeline.
 * Instantiates processor based on user provided input.
 */
public class DocumentChunkingProcessorFactory implements Processor.Factory {

    private final Environment environment;

    private final ClusterService clusterService;

    private final IndicesService indicesService;

    private final AnalysisRegistry analysisRegistry;

    public DocumentChunkingProcessorFactory(
        Environment environment,
        ClusterService clusterService,
        IndicesService indicesService,
        AnalysisRegistry analysisRegistry
    ) {
        this.environment = environment;
        this.clusterService = clusterService;
        this.indicesService = indicesService;
        this.analysisRegistry = analysisRegistry;
    }

    @Override
    public DocumentChunkingProcessor create(
        Map<String, Processor.Factory> registry,
        String processorTag,
        String description,
        Map<String, Object> config
    ) throws Exception {
        Map<String, Object> fieldMap = readMap(TYPE, processorTag, config, FIELD_MAP_FIELD);
        Map<String, Object> algorithmMap = readMap(TYPE, processorTag, config, ALGORITHM_FIELD);
        return new DocumentChunkingProcessor(
            processorTag,
            description,
            fieldMap,
            algorithmMap,
            environment,
            clusterService,
            indicesService,
            analysisRegistry
        );
    }
}
