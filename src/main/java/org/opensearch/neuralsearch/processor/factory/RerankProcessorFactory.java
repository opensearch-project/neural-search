/*
 * Copyright 2023 Aryn
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.opensearch.neuralsearch.processor.factory;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.stream.Collectors;

import lombok.AllArgsConstructor;

import org.opensearch.neuralsearch.ml.MLCommonsClientAccessor;
import org.opensearch.neuralsearch.processor.rerank.ContextSourceFetcher;
import org.opensearch.neuralsearch.processor.rerank.DocumentContextSourceFetcher;
import org.opensearch.neuralsearch.processor.rerank.QueryContextSourceFetcher;
import org.opensearch.neuralsearch.processor.rerank.RerankType;
import org.opensearch.neuralsearch.processor.rerank.TextSimilarityRerankProcessor;
import org.opensearch.search.pipeline.Processor;
import org.opensearch.search.pipeline.SearchResponseProcessor;

import com.google.common.annotations.VisibleForTesting;

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
        List<ContextSourceFetcher> contextFetchers = ContextFetcherFactory.createFetchers(config, includeQueryContextFetcher);
        switch (type) {
            case ML_OPENSEARCH:
                @SuppressWarnings("unchecked")
                Map<String, String> rerankerConfig = (Map<String, String>) config.remove(type.getLabel());
                String modelId = rerankerConfig.get(TextSimilarityRerankProcessor.MODEL_ID_FIELD);
                if (modelId == null) {
                    throw new IllegalArgumentException(
                        String.format(Locale.ROOT, "%s must be specified", TextSimilarityRerankProcessor.MODEL_ID_FIELD)
                    );
                }
                return new TextSimilarityRerankProcessor(description, tag, ignoreFailure, modelId, contextFetchers, clientAccessor);
            default:
                throw new IllegalArgumentException(
                    String.format(Locale.ROOT, "could not find constructor for reranker type %s", type.getLabel())
                );
        }
    }

    @VisibleForTesting
    RerankType findRerankType(final Map<String, Object> config) throws IllegalArgumentException {
        for (String key : config.keySet()) {
            try {
                RerankType attempt = RerankType.from(key);
                return attempt;
            } catch (IllegalArgumentException e) {
                // Assume it's just a different field in the config, so don't do anything.
                // If we get to the end and there were no valid RerankTypes, then we can panic.
            }
        }
        throw new IllegalArgumentException("no rerank type found");
    }

    /**
     * Factory class for context fetchers. Constructs a list of context fetchers
     * specified in the pipeline config (and maybe the query context fetcher)
     */
    protected static class ContextFetcherFactory {

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
        public static List<ContextSourceFetcher> createFetchers(Map<String, Object> config, boolean includeQueryContextFetcher) {
            List<ContextSourceFetcher> fetchers = new ArrayList<>();
            @SuppressWarnings("unchecked")
            Map<String, Object> contextConfig = (Map<String, Object>) config.remove(CONTEXT_CONFIG_FIELD);
            if (contextConfig == null) {
                throw new IllegalArgumentException(String.format(Locale.ROOT, "%s field must be provided", CONTEXT_CONFIG_FIELD));
            }
            for (String key : contextConfig.keySet()) {
                switch (key) {
                    case DocumentContextSourceFetcher.NAME:
                        Object cfg = contextConfig.get(key);
                        if (!(cfg instanceof List<?>)) {
                            throw new IllegalArgumentException(String.format(Locale.ROOT, "%s must be a list of strings", key));
                        }
                        List<?> fields = (List<?>) contextConfig.get(key);
                        if (fields.size() == 0) {
                            throw new IllegalArgumentException(String.format(Locale.ROOT, "%s must be nonempty", key));
                        }
                        List<String> strfields = fields.stream().map(field -> (String) field).collect(Collectors.toList());
                        fetchers.add(new DocumentContextSourceFetcher(strfields));
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
