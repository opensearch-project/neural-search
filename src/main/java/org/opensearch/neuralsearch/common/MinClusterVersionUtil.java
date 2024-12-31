/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.common;

import com.google.common.collect.ImmutableMap;
import org.opensearch.Version;
import org.opensearch.knn.index.util.IndexUtil;
import org.opensearch.neuralsearch.util.NeuralSearchClusterUtil;

import java.util.Map;

import static org.opensearch.knn.index.query.KNNQueryBuilder.MAX_DISTANCE_FIELD;
import static org.opensearch.knn.index.query.KNNQueryBuilder.MIN_SCORE_FIELD;
import static org.opensearch.neuralsearch.query.NeuralQueryBuilder.MODEL_ID_FIELD;

/**
 * A util class which holds the logic to determine the min version supported by the request parameters
 */
public final class MinClusterVersionUtil {

    private static final Version MINIMAL_SUPPORTED_VERSION_DEFAULT_MODEL_ID = Version.V_2_11_0;
    private static final Version MINIMAL_SUPPORTED_VERSION_RADIAL_SEARCH = Version.V_2_14_0;
    private static final Version MINIMAL_SUPPORTED_VERSION_PAGINATION_IN_HYBRID_QUERY = Version.V_2_19_0;

    // Note this minimal version will act as a override
    private static final Map<String, Version> MINIMAL_VERSION_NEURAL = ImmutableMap.<String, Version>builder()
        .put(MODEL_ID_FIELD.getPreferredName(), MINIMAL_SUPPORTED_VERSION_DEFAULT_MODEL_ID)
        .put(MAX_DISTANCE_FIELD.getPreferredName(), MINIMAL_SUPPORTED_VERSION_RADIAL_SEARCH)
        .put(MIN_SCORE_FIELD.getPreferredName(), MINIMAL_SUPPORTED_VERSION_RADIAL_SEARCH)
        .build();

    public static boolean isClusterOnOrAfterMinReqVersionForDefaultModelIdSupport() {
        return NeuralSearchClusterUtil.instance().getClusterMinVersion().onOrAfter(MINIMAL_SUPPORTED_VERSION_DEFAULT_MODEL_ID);
    }

    public static boolean isClusterOnOrAfterMinReqVersionForRadialSearch() {
        return NeuralSearchClusterUtil.instance().getClusterMinVersion().onOrAfter(MINIMAL_SUPPORTED_VERSION_RADIAL_SEARCH);
    }

    public static boolean isClusterOnOrAfterMinReqVersionForPaginationInHybridQuery() {
        return NeuralSearchClusterUtil.instance().getClusterMinVersion().onOrAfter(MINIMAL_SUPPORTED_VERSION_PAGINATION_IN_HYBRID_QUERY);
    }

    public static boolean isClusterOnOrAfterMinReqVersion(String key) {
        Version version;
        if (MINIMAL_VERSION_NEURAL.containsKey(key)) {
            version = MINIMAL_VERSION_NEURAL.get(key);
        } else {
            version = IndexUtil.minimalRequiredVersionMap.get(key);
        }
        return NeuralSearchClusterUtil.instance().getClusterMinVersion().onOrAfter(version);
    }
}
