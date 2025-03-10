/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.util;

import com.google.common.annotations.VisibleForTesting;
import lombok.extern.log4j.Log4j2;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.ingest.IngestService;
import org.opensearch.search.pipeline.PipelineConfiguration;
import org.opensearch.search.pipeline.SearchPipelineService;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Class abstracts information related to ingest and search pipelines
 */
@Log4j2
public class PipelineServiceUtil {
    private ClusterService clusterService;

    /**
     * Constructor
     * @param clusterService
     */
    public PipelineServiceUtil(ClusterService clusterService) {
        this.clusterService = clusterService;
    }

    /**
     * Returns list of search pipeline configs
     * @return list of search pipeline configs
     */
    public List<Map<String, Object>> getSearchPipelineConfigs() {
        List<Map<String, Object>> pipelineConfigs = getSearchPipelines().stream()
            .map(PipelineConfiguration::getConfigAsMap)
            .collect(Collectors.toList());

        return pipelineConfigs;
    }

    /**
     * Returns list of ingest pipeline configs
     * @return list of ingest pipeline configs
     */
    public List<Map<String, Object>> getIngestPipelineConfigs() {
        List<Map<String, Object>> pipelineConfigs = getIngestPipelines().stream()
            .map(org.opensearch.ingest.PipelineConfiguration::getConfigAsMap)
            .collect(Collectors.toList());

        return pipelineConfigs;
    }

    @VisibleForTesting
    protected List<org.opensearch.ingest.PipelineConfiguration> getIngestPipelines() {
        return IngestService.getPipelines(clusterService.state());
    }

    @VisibleForTesting
    protected List<org.opensearch.search.pipeline.PipelineConfiguration> getSearchPipelines() {
        return SearchPipelineService.getPipelines(clusterService.state());
    }
}
