/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.processor.factory;

import org.opensearch.neuralsearch.processor.ExplainResponseProcessor;
import org.opensearch.search.pipeline.Processor;
import org.opensearch.search.pipeline.SearchResponseProcessor;

import java.util.Map;

public class ProcessorExplainPublisherFactory implements Processor.Factory<SearchResponseProcessor> {

    @Override
    public SearchResponseProcessor create(
        Map<String, Processor.Factory<SearchResponseProcessor>> processorFactories,
        String tag,
        String description,
        boolean ignoreFailure,
        Map<String, Object> config,
        Processor.PipelineContext pipelineContext
    ) throws Exception {
        return new ExplainResponseProcessor(description, tag, ignoreFailure);
    }
}
