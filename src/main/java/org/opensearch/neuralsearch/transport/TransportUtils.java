/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.transport;

import org.opensearch.OpenSearchStatusException;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.neuralsearch.sparse.SparseSettings;

import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.Optional;
import java.util.stream.Collectors;

/**
 * A utility class for transport layer operations
 */
public class TransportUtils {
    /**
     * Validates that all provided indices are sparse indices
     * @param state ClusterState
     * @param concreteIndices Array of concrete index names
     * @param operationName Name of the operation for error messages
     * @throws OpenSearchStatusException if any index is not a sparse index
     */
    public static void validateSparseIndices(ClusterState state, String[] concreteIndices, String operationName) {
        List<String> invalidIndexNames = Arrays.stream(concreteIndices).filter(indexName -> {
            Boolean isSparseIndex = Optional.ofNullable(state)
                .map(ClusterState::metadata)
                .map(metadata -> metadata.index(indexName))
                .map(IndexMetadata::getSettings)
                .map(SparseSettings.IS_SPARSE_INDEX_SETTING::get)
                .orElse(false);

            return !isSparseIndex;
        }).collect(Collectors.toList());

        if (!invalidIndexNames.isEmpty()) {
            throw new OpenSearchStatusException(
                String.format(
                    Locale.ROOT,
                    "Request rejected. Indices [%s] don't support %s operation.",
                    String.join(", ", invalidIndexNames),
                    operationName
                ),
                RestStatus.BAD_REQUEST
            );
        }
    }
}
