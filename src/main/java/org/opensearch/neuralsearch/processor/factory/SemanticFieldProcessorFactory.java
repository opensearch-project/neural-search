/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.processor.factory;

import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.settings.Settings;
import org.opensearch.env.Environment;
import org.opensearch.index.analysis.AnalysisRegistry;
import org.opensearch.ingest.AbstractBatchingSystemProcessor;
import org.opensearch.neuralsearch.processor.chunker.Chunker;
import org.opensearch.neuralsearch.processor.chunker.ChunkerFactory;
import org.opensearch.neuralsearch.processor.chunker.FixedTokenLengthChunker;
import org.opensearch.neuralsearch.util.SemanticMappingUtils;
import org.opensearch.neuralsearch.ml.MLCommonsClientAccessor;
import org.opensearch.neuralsearch.processor.semantic.SemanticFieldProcessor;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.opensearch.neuralsearch.settings.NeuralSearchSettings.SEMANTIC_INGEST_BATCH_SIZE;
import static org.opensearch.plugins.IngestPlugin.SystemIngestPipelineConfigKeys.INDEX_MAPPINGS;
import static org.opensearch.plugins.IngestPlugin.SystemIngestPipelineConfigKeys.INDEX_SETTINGS;
import static org.opensearch.plugins.IngestPlugin.SystemIngestPipelineConfigKeys.INDEX_TEMPLATE_MAPPINGS;
import static org.opensearch.plugins.IngestPlugin.SystemIngestPipelineConfigKeys.INDEX_TEMPLATE_SETTINGS;

/**
 *  Factory for semantic fields.
 *
 *  This factory is for internal usage and will be systematically invoked if we detect there are semantic fields
 *  defined. Users should not be able to define this type of the processor in a regular ingest pipeline.
 */
public final class SemanticFieldProcessorFactory extends AbstractBatchingSystemProcessor.Factory {
    public static final String PROCESSOR_FACTORY_TYPE = "system_ingest_processor_factory_semantic_field";
    /**
     * Ideally this should be configurable through the semantic fields. For P0 we will use a default value,
     * and in P1 we will make it configurable.
     * TODO: Make batch size configurable
     */
    private static final int DEFAULT_BATCH_SIZE = 10;

    private final MLCommonsClientAccessor mlClientAccessor;

    private final Environment environment;

    private final ClusterService clusterService;
    private final AnalysisRegistry analysisRegistry;

    public SemanticFieldProcessorFactory(
        final MLCommonsClientAccessor mlClientAccessor,
        final Environment environment,
        final ClusterService clusterService,
        final AnalysisRegistry analysisRegistry
    ) {
        super(PROCESSOR_FACTORY_TYPE);
        this.mlClientAccessor = mlClientAccessor;
        this.environment = environment;
        this.clusterService = clusterService;
        this.analysisRegistry = analysisRegistry;
    }

    @SuppressWarnings("unchecked")
    @Override
    protected AbstractBatchingSystemProcessor newProcessor(String tag, String description, Map<String, Object> config) {
        final List<Map<String, Object>> mappings = new ArrayList<>();
        final List<Settings> settingsList = new ArrayList<>();
        // Configurations from later templates override earlier ones. Index configurations override all template
        // configurations, so we read templates first, then index configurations.
        if (config.get(INDEX_TEMPLATE_MAPPINGS) instanceof List<?> mappingFromTemplates) {
            mappings.addAll((List<Map<String, Object>>) mappingFromTemplates);
        }
        if (config.get(INDEX_MAPPINGS) instanceof Map<?, ?> mappingFromIndex) {
            mappings.add((Map<String, Object>) mappingFromIndex);
        }
        if (config.get(INDEX_TEMPLATE_SETTINGS) instanceof List<?> settingsFromTemplates) {
            settingsList.addAll((List<Settings>) settingsFromTemplates);
        }
        if (config.get(INDEX_SETTINGS) instanceof Settings settingsFromIndex) {
            settingsList.add(settingsFromIndex);
        }

        // If no config we are not able to create a processor so simply return a null to show no processor created
        if (mappings.isEmpty()) {
            return null;
        }

        if (description == null) {
            description = "This is a system ingest processor for semantic fields. It will do text chunking and "
                + "embedding generation for semantic fields.";
        }

        final Map<String, Map<String, Object>> semanticFieldPathToConfigMap = new HashMap<>();
        for (final Map<String, Object> mapping : mappings) {
            final Map<String, Object> properties = SemanticMappingUtils.getProperties(mapping);
            // if there is no property in the mapping we simply skip it
            if (properties.isEmpty()) {
                continue;
            }

            SemanticMappingUtils.collectSemanticField(properties, semanticFieldPathToConfigMap);
        }

        // If no semantic field we don't need to create a processor so simply return null to show no processor created.
        if (semanticFieldPathToConfigMap.isEmpty()) {
            return null;
        }

        int batch_size = DEFAULT_BATCH_SIZE;
        for (Settings settings : settingsList) {
            if (settings.hasValue(SEMANTIC_INGEST_BATCH_SIZE.getKey())) {
                batch_size = SEMANTIC_INGEST_BATCH_SIZE.get(settings);
            }
        }

        // TODO: Allow users to define the chunkers for each field and build the semantic field path -> chunkers map
        // and pass to the SemanticFieldProcessor to use. - https://github.com/opensearch-project/neural-search/issues/1340

        return new SemanticFieldProcessor(
            tag,
            description,
            batch_size,
            semanticFieldPathToConfigMap,
            mlClientAccessor,
            environment,
            clusterService,
            createDefaultTextChunker()
        );
    }

    /**
     * Create a default text chunker.
     * @return A default fixed token length chunker
     */
    private Chunker createDefaultTextChunker() {
        final Map<String, Object> chunkerParameters = new HashMap<>();
        chunkerParameters.put(FixedTokenLengthChunker.ANALYSIS_REGISTRY_FIELD, analysisRegistry);
        return ChunkerFactory.create(FixedTokenLengthChunker.ALGORITHM_NAME, chunkerParameters);
    }
}
