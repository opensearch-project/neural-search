/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.common;

import com.google.common.collect.ImmutableMap;
import org.opensearch.Version;
import org.opensearch.knn.index.util.IndexUtil;
import org.opensearch.neuralsearch.stats.events.EventStatName;
import org.opensearch.neuralsearch.stats.info.InfoStatName;
import org.opensearch.neuralsearch.util.NeuralSearchClusterUtil;

import java.util.EnumSet;
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

    // Note this minimal version will act as an override
    private static final Map<String, Version> MINIMAL_VERSION_NEURAL = ImmutableMap.<String, Version>builder()
        .put(MODEL_ID_FIELD.getPreferredName(), MINIMAL_SUPPORTED_VERSION_DEFAULT_DENSE_MODEL_ID)
        .put(MAX_DISTANCE_FIELD.getPreferredName(), MINIMAL_SUPPORTED_VERSION_RADIAL_SEARCH)
        .put(MIN_SCORE_FIELD.getPreferredName(), MINIMAL_SUPPORTED_VERSION_RADIAL_SEARCH)
        .put(QUERY_IMAGE_FIELD.getPreferredName(), MINIMAL_SUPPORTED_VERSION_QUERY_IMAGE_FIX)
        .build();

    /**
     * Info stats organized by version added
     */
    public static final Map<Version, EnumSet<InfoStatName>> infoStatsByVersion = ImmutableMap.<Version, EnumSet<InfoStatName>>builder()
        .put(Version.V_3_0_0, EnumSet.of(InfoStatName.CLUSTER_VERSION, InfoStatName.TEXT_EMBEDDING_PROCESSORS))
        .put(
            Version.V_3_1_0,
            EnumSet.of(
                InfoStatName.TEXT_EMBEDDING_SKIP_EXISTING_PROCESSORS,
                InfoStatName.TEXT_CHUNKING_PROCESSORS,
                InfoStatName.TEXT_CHUNKING_DELIMITER_PROCESSORS,
                InfoStatName.TEXT_CHUNKING_FIXED_LENGTH_PROCESSORS,
                InfoStatName.NORMALIZATION_PROCESSORS,
                InfoStatName.NORM_TECHNIQUE_L2_PROCESSORS,
                InfoStatName.NORM_TECHNIQUE_MINMAX_PROCESSORS,
                InfoStatName.NORM_TECHNIQUE_ZSCORE_PROCESSORS,
                InfoStatName.COMB_TECHNIQUE_ARITHMETIC_PROCESSORS,
                InfoStatName.COMB_TECHNIQUE_GEOMETRIC_PROCESSORS,
                InfoStatName.COMB_TECHNIQUE_HARMONIC_PROCESSORS,
                InfoStatName.RRF_PROCESSORS,
                InfoStatName.COMB_TECHNIQUE_RRF_PROCESSORS
            )
        )
        .build();

    /**
     * Event stats organized by version added
     */
    public static final Map<Version, EnumSet<EventStatName>> eventStatsByVersion = ImmutableMap.<Version, EnumSet<EventStatName>>builder()
        .put(Version.V_3_0_0, EnumSet.of(EventStatName.TEXT_EMBEDDING_PROCESSOR_EXECUTIONS))
        .put(
            Version.V_3_1_0,
            EnumSet.of(
                EventStatName.TEXT_EMBEDDING_PROCESSOR_SKIP_EXISTING_EXECUTIONS,
                EventStatName.TEXT_CHUNKING_PROCESSOR_EXECUTIONS,
                EventStatName.TEXT_CHUNKING_FIXED_LENGTH_EXECUTIONS,
                EventStatName.TEXT_CHUNKING_DELIMITER_EXECUTIONS,
                EventStatName.SEMANTIC_HIGHLIGHTING_REQUEST_COUNT,
                EventStatName.NORMALIZATION_PROCESSOR_EXECUTIONS,
                EventStatName.NORM_TECHNIQUE_L2_EXECUTIONS,
                EventStatName.NORM_TECHNIQUE_MINMAX_EXECUTIONS,
                EventStatName.NORM_TECHNIQUE_NORM_ZSCORE_EXECUTIONS,
                EventStatName.COMB_TECHNIQUE_ARITHMETIC_EXECUTIONS,
                EventStatName.COMB_TECHNIQUE_GEOMETRIC_EXECUTIONS,
                EventStatName.COMB_TECHNIQUE_HARMONIC_EXECUTIONS,
                EventStatName.RRF_PROCESSOR_EXECUTIONS,
                EventStatName.COMB_TECHNIQUE_RRF_EXECUTIONS,
                EventStatName.HYBRID_QUERY_REQUESTS,
                EventStatName.HYBRID_QUERY_INNER_HITS_REQUESTS,
                EventStatName.HYBRID_QUERY_FILTER_REQUESTS,
                EventStatName.HYBRID_QUERY_PAGINATION_REQUESTS
            )
        )
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
     * Returns all cumulatively available info stats for a given version
     * @param version
     * @return an EnumSet of all available stats at that version
     */
    public static EnumSet<InfoStatName> getInfoStatsAvailableInVersion(Version version) {
        if (version == Version.CURRENT) {
            return EnumSet.allOf(InfoStatName.class);
        }

        EnumSet<InfoStatName> infoStatNames = EnumSet.noneOf(InfoStatName.class);
        for (Map.Entry<Version, EnumSet<InfoStatName>> entry : infoStatsByVersion.entrySet()) {
            if (entry.getKey().onOrBefore(version)) {
                infoStatNames.addAll(entry.getValue());
            }
        }
        return infoStatNames;
    }

    /**
     * Returns all cumulatively available event stats for a given version
     * @param version
     * @return an EnumSet of all available stats at that version
     */
    public static EnumSet<EventStatName> getEventStatsAvailableInVersion(Version version) {
        if (version == Version.CURRENT) {
            return EnumSet.allOf(EventStatName.class);
        }

        EnumSet<EventStatName> eventStatNames = EnumSet.noneOf(EventStatName.class);
        for (Map.Entry<Version, EnumSet<EventStatName>> entry : eventStatsByVersion.entrySet()) {
            if (entry.getKey().onOrBefore(version)) {
                eventStatNames.addAll(entry.getValue());
            }
        }
        return eventStatNames;
    }
}
