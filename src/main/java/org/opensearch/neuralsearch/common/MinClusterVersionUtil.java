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
import static org.opensearch.neuralsearch.query.NeuralQueryBuilder.QUERY_IMAGE_FIELD;

/**
 * A util class which holds the logic to determine the min version supported by the request parameters
 */
public final class MinClusterVersionUtil {

    private static final Version MINIMAL_SUPPORTED_VERSION_DEFAULT_DENSE_MODEL_ID = Version.V_2_11_0;
    private static final Version MINIMAL_SUPPORTED_VERSION_RADIAL_SEARCH = Version.V_2_14_0;
    private static final Version MINIMAL_SUPPORTED_VERSION_QUERY_IMAGE_FIX = Version.V_2_19_0;
    private static final Version MINIMAL_SUPPORTED_VERSION_PAGINATION_IN_HYBRID_QUERY = Version.V_2_19_0;
    private static final Version MINIMAL_SUPPORTED_VERSION_NEURAL_ORIGINAL_QUERY_TEXT = Version.V_3_0_0;
    public static final Version MINIMAL_SUPPORTED_VERSION_SEMANTIC_FIELD = Version.V_3_1_0;
    public static final Version MINIMAL_SUPPORTED_VERSION_STATS_CATEGORY_FILTERING = Version.V_3_1_0;
    private static final Version MINIMAL_SUPPORTED_VERSION_NEURAL_KNN_QUERY_BUILDER = Version.V_3_0_0;

    // Constant for neural_knn_query version check
    public static final String NEURAL_KNN_QUERY = "neural_knn_query";

    // Note this minimal version will act as an override
    private static final Map<String, Version> MINIMAL_VERSION_NEURAL = ImmutableMap.<String, Version>builder()
        .put(MODEL_ID_FIELD.getPreferredName(), MINIMAL_SUPPORTED_VERSION_DEFAULT_DENSE_MODEL_ID)
        .put(MAX_DISTANCE_FIELD.getPreferredName(), MINIMAL_SUPPORTED_VERSION_RADIAL_SEARCH)
        .put(MIN_SCORE_FIELD.getPreferredName(), MINIMAL_SUPPORTED_VERSION_RADIAL_SEARCH)
        .put(QUERY_IMAGE_FIELD.getPreferredName(), MINIMAL_SUPPORTED_VERSION_QUERY_IMAGE_FIX)
        .put(NEURAL_KNN_QUERY, MINIMAL_SUPPORTED_VERSION_NEURAL_KNN_QUERY_BUILDER)
        .build();

    public static boolean isClusterOnOrAfterMinReqVersionForDefaultDenseModelIdSupport() {
        return NeuralSearchClusterUtil.instance().getClusterMinVersion().onOrAfter(MINIMAL_SUPPORTED_VERSION_DEFAULT_DENSE_MODEL_ID);
    }

    public static boolean isClusterOnOrAfterMinReqVersionForRadialSearch() {
        return NeuralSearchClusterUtil.instance().getClusterMinVersion().onOrAfter(MINIMAL_SUPPORTED_VERSION_RADIAL_SEARCH);
    }

    public static boolean isClusterOnOrAfterMinReqVersionForPaginationInHybridQuery() {
        return NeuralSearchClusterUtil.instance().getClusterMinVersion().onOrAfter(MINIMAL_SUPPORTED_VERSION_PAGINATION_IN_HYBRID_QUERY);
    }

    public static boolean isClusterOnOrAfterMinReqVersionForStatCategoryFiltering() {
        return NeuralSearchClusterUtil.instance().getClusterMinVersion().onOrAfter(MINIMAL_SUPPORTED_VERSION_STATS_CATEGORY_FILTERING);
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

    /**
     * Checks if the version from StreamInput/StreamOutput is on or after the minimum required version for NeuralKNNQueryText
     *
     * @param version The version to check
     * @return true if the version is on or after the minimum required version
     */
    public static boolean isVersionOnOrAfterMinReqVersionForNeuralKNNQueryText(Version version) {
        return version.onOrAfter(MINIMAL_SUPPORTED_VERSION_NEURAL_ORIGINAL_QUERY_TEXT);
    }

    /**
     * Checks if the cluster min version is on or after the minimum required version for semantic field type
     *
     * @return true if the version is on or after the minimum required version
     */
    public static boolean isClusterOnOrAfterMinReqVersionForSemanticFieldType() {
        return NeuralSearchClusterUtil.instance().getClusterMinVersion().onOrAfter(MINIMAL_SUPPORTED_VERSION_SEMANTIC_FIELD);
    }

    /**
     * Checks if the cluster min version supports NeuralKNNQueryBuilder
     *
     * @return true if the cluster version supports NeuralKNNQueryBuilder
     */
    public static boolean isClusterOnOrAfterMinReqVersionForNeuralKNNQueryBuilder() {
        return NeuralSearchClusterUtil.instance().getClusterMinVersion().onOrAfter(MINIMAL_SUPPORTED_VERSION_NEURAL_KNN_QUERY_BUILDER);
    }
}
