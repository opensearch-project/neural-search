/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.rest;

import org.opensearch.OpenSearchStatusException;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.core.index.Index;

import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.Optional;
import java.util.stream.Collectors;

import static org.opensearch.neuralsearch.sparse.SparseSettings.SPARSE_INDEX;

/**
 * A Utils class for REST API operations
 */
public class RestUtils {
    /**
     * @param indices An array of indices related to the request
     * @param clusterService ClusterService of OpenSearch Cluster
     * @param apiOperation Determine whether the request is to warm up or clear cache
     */
    public static void validateSparseIndices(Index[] indices, ClusterService clusterService, String apiOperation) {
        List<String> invalidIndexNames = Arrays.stream(indices).filter(index -> {
            String sparseIndexSetting = Optional.ofNullable(clusterService)
                .map(ClusterService::state)
                .map(ClusterState::metadata)
                .map(metadata -> metadata.getIndexSafe(index))
                .map(IndexMetadata::getSettings)
                .map(settings -> settings.get(SPARSE_INDEX))
                .orElse(null);

            return !"true".equals(sparseIndexSetting);
        }).map(Index::getName).collect(Collectors.toList());

        if (!invalidIndexNames.isEmpty()) {
            throw new OpenSearchStatusException(
                String.format(
                    Locale.ROOT,
                    "Request rejected. Indices [%s] don't support %s operation.",
                    String.join(", ", invalidIndexNames),
                    apiOperation
                ),
                RestStatus.BAD_REQUEST
            );
        }
    }
}
