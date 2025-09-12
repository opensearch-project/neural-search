/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.util;

import lombok.AccessLevel;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.NonNull;
import lombok.extern.log4j.Log4j2;
import org.opensearch.Version;
import org.opensearch.action.IndicesRequest;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.metadata.IndexNameExpressionResolver;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.core.index.Index;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Class abstracts information related to underlying OpenSearch cluster
 */
@NoArgsConstructor(access = AccessLevel.PRIVATE)
@Log4j2
public class NeuralSearchClusterUtil {
    @Getter
    private ClusterService clusterService;
    private IndexNameExpressionResolver indexNameExpressionResolver;

    private static NeuralSearchClusterUtil instance;

    /**
     * Return instance of the cluster context, must be initialized first for proper usage
     * @return instance of cluster context
     */
    public static NeuralSearchClusterUtil instance() {
        if (instance == null) {
            synchronized (NeuralSearchClusterUtil.class) {
                if (instance == null) {
                    instance = new NeuralSearchClusterUtil();
                }
            }
        }
        return instance;
    }

    /**
     * Initializes instance of cluster context by injecting dependencies
     * @param clusterService
     */
    public void initialize(final ClusterService clusterService, final IndexNameExpressionResolver indexNameExpressionResolver) {
        this.clusterService = clusterService;
        this.indexNameExpressionResolver = indexNameExpressionResolver;
    }

    /**
     * Return minimal OpenSearch version based on all nodes currently discoverable in the cluster
     * @return minimal installed OpenSearch version, default to Version.CURRENT which is typically the latest version
     */
    public Version getClusterMinVersion() {
        return this.clusterService.state().getNodes().getMinNodeVersion();
    }

    public List<IndexMetadata> getIndexMetadataList(@NonNull final IndicesRequest searchRequest) {
        final Index[] concreteIndices = this.indexNameExpressionResolver.concreteIndices(clusterService.state(), searchRequest);
        return Arrays.stream(concreteIndices)
            .map(concreteIndex -> clusterService.state().metadata().index(concreteIndex))
            .collect(Collectors.toList());
    }

    public Map<String, String> getIndexMapping(String[] indices) {
        try {
            if (indices != null && indices.length > 0) {
                return Arrays.stream(indices).filter(indexName -> {
                    IndexMetadata indexMetadata = clusterService.state().metadata().index(indexName);
                    return indexMetadata != null && indexMetadata.mapping() != null;
                }).collect(Collectors.toMap(indexName -> indexName, indexName -> {
                    IndexMetadata indexMetadata = clusterService.state().metadata().index(indexName);
                    return indexMetadata.mapping().source().toString();
                }));
            }
        } catch (Exception e) {
            log.warn("Failed to extract index mapping", e);
            throw new IllegalStateException("Failed to extract index mapping", e);
        }
        throw new IllegalStateException("No valid index found to extract mapping");
    }
}
