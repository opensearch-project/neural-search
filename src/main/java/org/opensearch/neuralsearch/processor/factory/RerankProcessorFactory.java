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

import java.util.Map;

import lombok.AllArgsConstructor;

import org.opensearch.neuralsearch.ml.MLCommonsClientAccessor;
import org.opensearch.neuralsearch.processor.rerank.CrossEncoderRerankProcessor;
import org.opensearch.neuralsearch.processor.rerank.RerankType;
import org.opensearch.search.pipeline.Processor;
import org.opensearch.search.pipeline.SearchResponseProcessor;

@AllArgsConstructor
public class RerankProcessorFactory implements Processor.Factory<SearchResponseProcessor> {

    public static final String RERANK_PROCESSOR_TYPE = "rerank";

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
        switch (type) {
            case CROSS_ENCODER:
                @SuppressWarnings("unchecked")
                Map<String, String> rerankerConfig = (Map<String, String>) config.get(type.getLabel());
                String modelId = rerankerConfig.get(CrossEncoderRerankProcessor.MODEL_ID_FIELD);
                String rerankContext = rerankerConfig.get(CrossEncoderRerankProcessor.RERANK_CONTEXT_FIELD);
                return new CrossEncoderRerankProcessor(description, tag, ignoreFailure, modelId, rerankContext, clientAccessor);
            default:
                throw new IllegalArgumentException("could not find constructor for reranker type " + type.getLabel());
        }
    }

    private RerankType findRerankType(final Map<String, Object> config) throws IllegalArgumentException {
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
}
