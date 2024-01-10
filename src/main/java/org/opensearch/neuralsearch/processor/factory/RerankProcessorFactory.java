/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.processor.factory;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.StringJoiner;

import org.opensearch.ingest.ConfigurationUtils;
import org.opensearch.neuralsearch.ml.MLCommonsClientAccessor;
import org.opensearch.neuralsearch.processor.rerank.ContextSourceFetcher;
import org.opensearch.neuralsearch.processor.rerank.DocumentContextSourceFetcher;
import org.opensearch.neuralsearch.processor.rerank.MLOpenSearchRerankProcessor;
import org.opensearch.neuralsearch.processor.rerank.QueryContextSourceFetcher;
import org.opensearch.neuralsearch.processor.rerank.RerankType;
import org.opensearch.search.pipeline.Processor;
import org.opensearch.search.pipeline.SearchResponseProcessor;

import com.google.common.collect.Sets;

import lombok.AllArgsConstructor;

/**
 * Factory for rerank processors. Must:
 * - Instantiate the right kind of rerank processor
 * - Instantiate the appropriate context source fetchers
 */
@AllArgsConstructor
public class RerankProcessorFactory implements Processor.Factory<SearchResponseProcessor> {

    public static final String RERANK_PROCESSOR_TYPE = "rerank";
    public static final String CONTEXT_CONFIG_FIELD = "context";

    private final MLCommonsClientAccessor clientAccessor;

    @Override
    public SearchResponseProcessor create(
        final Map<String, Processor.Factory<SearchResponseProcessor>> processorFactories,
        final String tag,
        final String description,
        final boolean ignoreFailure,
        final Map<String, Object> config,
        final Processor.PipelineContext pipelineContext
    ) {
        RerankType type = findRerankType(config);
        boolean includeQueryContextFetcher = ContextFetcherFactory.shouldIncludeQueryContextFetcher(type);
        List<ContextSourceFetcher> contextFetchers = ContextFetcherFactory.createFetchers(config, includeQueryContextFetcher, tag);
        switch (type) {
            case ML_OPENSEARCH:
                Map<String, Object> rerankerConfig = ConfigurationUtils.readMap(RERANK_PROCESSOR_TYPE, tag, config, type.getLabel());
                String modelId = ConfigurationUtils.readStringProperty(
                    RERANK_PROCESSOR_TYPE,
                    tag,
                    rerankerConfig,
                    MLOpenSearchRerankProcessor.MODEL_ID_FIELD
                );
                return new MLOpenSearchRerankProcessor(description, tag, ignoreFailure, modelId, contextFetchers, clientAccessor);
            default:
                throw new IllegalArgumentException(String.format(Locale.ROOT, "Cannot build reranker type %s", type.getLabel()));
        }
    }

    private RerankType findRerankType(final Map<String, Object> config) throws IllegalArgumentException {
        // Set of rerank type labels in the config
        Set<String> rerankTypes = Sets.intersection(config.keySet(), RerankType.labelMap().keySet());
        // A rerank type must be provided
        if (rerankTypes.size() == 0) {
            StringJoiner msgBuilder = new StringJoiner(", ", "No rerank type found. Possible rerank types are: [", "]");
            for (RerankType t : RerankType.values()) {
                msgBuilder.add(t.getLabel());
            }
            throw new IllegalArgumentException(msgBuilder.toString());
        }
        // Only one rerank type may be provided
        if (rerankTypes.size() > 1) {
            StringJoiner msgBuilder = new StringJoiner(", ", "Multiple rerank types found: [", "]. Only one is permitted.");
            rerankTypes.forEach(rt -> msgBuilder.add(rt));
            throw new IllegalArgumentException(msgBuilder.toString());
        }
        return RerankType.from(rerankTypes.iterator().next());
    }

    /**
     * Factory class for context fetchers. Constructs a list of context fetchers
     * specified in the pipeline config (and maybe the query context fetcher)
     */
    private static class ContextFetcherFactory {

        /**
         * Map rerank types to whether they should include the query context source fetcher
         * @param type the constructing RerankType
         * @return does this RerankType depend on the QueryContextSourceFetcher?
         */
        public static boolean shouldIncludeQueryContextFetcher(RerankType type) {
            switch (type) {
                case ML_OPENSEARCH:
                    return true;
                default:
                    return false;
            }
        }

        /**
         * Create necessary queryContextFetchers for this processor
         * @param config processor config object. Look for "context" field to find fetchers
         * @param includeQueryContextFetcher should I include the queryContextFetcher?
         * @return list of contextFetchers for the processor to use
         */
        public static List<ContextSourceFetcher> createFetchers(
            Map<String, Object> config,
            boolean includeQueryContextFetcher,
            String tag
        ) {
            Map<String, Object> contextConfig = ConfigurationUtils.readMap(RERANK_PROCESSOR_TYPE, tag, config, CONTEXT_CONFIG_FIELD);
            List<ContextSourceFetcher> fetchers = new ArrayList<>();
            for (String key : contextConfig.keySet()) {
                Object cfg = contextConfig.get(key);
                switch (key) {
                    case DocumentContextSourceFetcher.NAME:
                        fetchers.add(DocumentContextSourceFetcher.create(cfg));
                        break;
                    default:
                        throw new IllegalArgumentException(String.format(Locale.ROOT, "unrecognized context field: %s", key));
                }
            }
            if (includeQueryContextFetcher) {
                fetchers.add(new QueryContextSourceFetcher());
            }
            return fetchers;
        }
    }
}
