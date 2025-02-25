/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.util;

import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import lombok.extern.log4j.Log4j2;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.ingest.IngestService;
import org.opensearch.search.pipeline.PipelineConfiguration;
import org.opensearch.search.pipeline.SearchPipelineService;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Class abstracts information related to underlying OpenSearch cluster
 */
@NoArgsConstructor(access = AccessLevel.PRIVATE)
@Log4j2
public class PipelineInfoUtil {
    private ClusterService clusterService;
    private static PipelineInfoUtil instance;

    /**
     * Return instance of the cluster context, must be initialized first for proper usage
     * @return instance of cluster context
     */
    public static synchronized PipelineInfoUtil instance() {
        if (instance == null) {
            instance = new PipelineInfoUtil();
        }
        return instance;
    }

    /**
     * Initializes instance of info util by injecting dependencies
     * @param clusterService
     */
    public void initialize(ClusterService clusterService) {
        this.clusterService = clusterService;
    }

    public List<Map<String, Object>> getSearchPipelineConfigs() {
        List<Map<String, Object>> pipelineConfigs = SearchPipelineService.getPipelines(clusterService.state())
            .stream()
            .map(PipelineConfiguration::getConfigAsMap)
            .collect(Collectors.toList());

        return pipelineConfigs;
    }

    public List<Map<String, Object>> getIngestPipelineConfigs() {
        List<Map<String, Object>> pipelineConfigs = IngestService.getPipelines(clusterService.state())
            .stream()
            .map(org.opensearch.ingest.PipelineConfiguration::getConfigAsMap)
            .collect(Collectors.toList());

        return pipelineConfigs;
    }
}
