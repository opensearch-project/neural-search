/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.stats.events;

import lombok.Getter;
import org.apache.commons.lang.StringUtils;
import org.opensearch.Version;
import org.opensearch.neuralsearch.stats.common.StatName;

import java.util.Arrays;
import java.util.Locale;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Enum that contains all event stat names, paths, and types
 * WE SHOULD AVOID CHANGING THE ORDER OF THESE STAT ENUMS! The ordinal is used in StreamInput/Output.
 * Changing the order will break the mixed cluster version upgrade version case.
 */
@Getter
public enum EventStatName implements StatName {
    TEXT_EMBEDDING_PROCESSOR_EXECUTIONS(
        "text_embedding_executions",
        "processors.ingest",
        EventStatType.TIMESTAMPED_EVENT_COUNTER,
        Version.V_3_0_0
    ),
    TEXT_EMBEDDING_PROCESSOR_SKIP_EXISTING_EXECUTIONS(
        "text_embedding_skip_existing_executions",
        "processors.ingest",
        EventStatType.TIMESTAMPED_EVENT_COUNTER,
        Version.V_3_1_0
    ),
    TEXT_CHUNKING_PROCESSOR_EXECUTIONS(
        "text_chunking_executions",
        "processors.ingest",
        EventStatType.TIMESTAMPED_EVENT_COUNTER,
        Version.V_3_1_0
    ),
    TEXT_CHUNKING_FIXED_LENGTH_EXECUTIONS(
        "text_chunking_fixed_length_executions",
        "processors.ingest",
        EventStatType.TIMESTAMPED_EVENT_COUNTER,
        Version.V_3_1_0
    ),
    TEXT_CHUNKING_DELIMITER_EXECUTIONS(
        "text_chunking_delimiter_executions",
        "processors.ingest",
        EventStatType.TIMESTAMPED_EVENT_COUNTER,
        Version.V_3_1_0
    ),
    SEMANTIC_FIELD_PROCESSOR_EXECUTIONS(
        "semantic_field_executions",
        "processors.ingest",
        EventStatType.TIMESTAMPED_EVENT_COUNTER,
        Version.V_3_1_0
    ),
    SEMANTIC_FIELD_PROCESSOR_CHUNKING_EXECUTIONS(
        "semantic_field_chunking_executions",
        "processors.ingest",
        EventStatType.TIMESTAMPED_EVENT_COUNTER,
        Version.V_3_1_0
    ),
    SEMANTIC_HIGHLIGHTING_REQUEST_COUNT(
        "semantic_highlighting_request_count",
        "semantic_highlighting",
        EventStatType.TIMESTAMPED_EVENT_COUNTER,
        Version.V_3_1_0
    ),
    NORMALIZATION_PROCESSOR_EXECUTIONS(
        "normalization_processor_executions",
        "processors.search.hybrid",
        EventStatType.TIMESTAMPED_EVENT_COUNTER,
        Version.V_3_1_0
    ),
    NORM_TECHNIQUE_L2_EXECUTIONS(
        "norm_l2_executions",
        "processors.search.hybrid",
        EventStatType.TIMESTAMPED_EVENT_COUNTER,
        Version.V_3_1_0
    ),
    NORM_TECHNIQUE_MINMAX_EXECUTIONS(
        "norm_minmax_executions",
        "processors.search.hybrid",
        EventStatType.TIMESTAMPED_EVENT_COUNTER,
        Version.V_3_1_0
    ),
    NORM_TECHNIQUE_NORM_ZSCORE_EXECUTIONS(
        "norm_zscore_executions",
        "processors.search.hybrid",
        EventStatType.TIMESTAMPED_EVENT_COUNTER,
        Version.V_3_1_0
    ),
    COMB_TECHNIQUE_ARITHMETIC_EXECUTIONS(
        "comb_arithmetic_executions",
        "processors.search.hybrid",
        EventStatType.TIMESTAMPED_EVENT_COUNTER,
        Version.V_3_1_0
    ),
    COMB_TECHNIQUE_GEOMETRIC_EXECUTIONS(
        "comb_geometric_executions",
        "processors.search.hybrid",
        EventStatType.TIMESTAMPED_EVENT_COUNTER,
        Version.V_3_1_0
    ),
    COMB_TECHNIQUE_HARMONIC_EXECUTIONS(
        "comb_harmonic_executions",
        "processors.search.hybrid",
        EventStatType.TIMESTAMPED_EVENT_COUNTER,
        Version.V_3_1_0
    ),
    RRF_PROCESSOR_EXECUTIONS(
        "rank_based_normalization_processor_executions",
        "processors.search.hybrid",
        EventStatType.TIMESTAMPED_EVENT_COUNTER,
        Version.V_3_1_0
    ),
    COMB_TECHNIQUE_RRF_EXECUTIONS(
        "comb_rrf_executions",
        "processors.search.hybrid",
        EventStatType.TIMESTAMPED_EVENT_COUNTER,
        Version.V_3_1_0
    ),
    HYBRID_QUERY_REQUESTS("hybrid_query_requests", "query.hybrid", EventStatType.TIMESTAMPED_EVENT_COUNTER, Version.V_3_1_0),
    HYBRID_QUERY_INNER_HITS_REQUESTS(
        "hybrid_query_with_inner_hits_requests",
        "query.hybrid",
        EventStatType.TIMESTAMPED_EVENT_COUNTER,
        Version.V_3_1_0
    ),
    HYBRID_QUERY_FILTER_REQUESTS(
        "hybrid_query_with_filter_requests",
        "query.hybrid",
        EventStatType.TIMESTAMPED_EVENT_COUNTER,
        Version.V_3_1_0
    ),
    HYBRID_QUERY_PAGINATION_REQUESTS(
        "hybrid_query_with_pagination_requests",
        "query.hybrid",
        EventStatType.TIMESTAMPED_EVENT_COUNTER,
        Version.V_3_1_0
    ),
    NEURAL_QUERY_REQUESTS("neural_query_requests", "query.neural", EventStatType.TIMESTAMPED_EVENT_COUNTER, Version.V_3_1_0),
    NEURAL_QUERY_AGAINST_KNN_REQUESTS(
        "neural_query_against_knn_requests",
        "query.neural",
        EventStatType.TIMESTAMPED_EVENT_COUNTER,
        Version.V_3_1_0
    ),
    NEURAL_QUERY_AGAINST_SEMANTIC_DENSE_REQUESTS(
        "neural_query_against_semantic_dense_requests",
        "query.neural",
        EventStatType.TIMESTAMPED_EVENT_COUNTER,
        Version.V_3_1_0
    ),
    NEURAL_QUERY_AGAINST_SEMANTIC_SPARSE_REQUESTS(
        "neural_query_against_semantic_sparse_requests",
        "query.neural",
        EventStatType.TIMESTAMPED_EVENT_COUNTER,
        Version.V_3_1_0
    ),
    NEURAL_SPARSE_QUERY_REQUESTS(
        "neural_sparse_query_requests",
        "query.neural_sparse",
        EventStatType.TIMESTAMPED_EVENT_COUNTER,
        Version.V_3_1_0
    );

    private final String nameString;
    private final String path;
    private final EventStatType statType;
    private EventStat eventStat;
    private Version version;

    /**
     * Enum lookup table by nameString
     */
    private static final Map<String, EventStatName> BY_NAME = Arrays.stream(values())
        .collect(Collectors.toMap(stat -> stat.nameString, stat -> stat));

    /**
     * Constructor
     * @param nameString the unique name of the stat.
     * @param path the unique path of the stat
     * @param statType the category of stat
     * @param version the version the stat is added
     */
    EventStatName(String nameString, String path, EventStatType statType, Version version) {
        this.nameString = nameString;
        this.path = path;
        this.statType = statType;
        this.version = version;

        switch (statType) {
            case EventStatType.TIMESTAMPED_EVENT_COUNTER:
                eventStat = new TimestampedEventStat(this);
                break;
        }

        // Validates all event stats are instantiated correctly. This is covered by unit tests as well.
        if (eventStat == null) {
            throw new IllegalArgumentException(
                String.format(Locale.ROOT, "Unable to initialize event stat [%s]. Unrecognized event stat type: [%s]", nameString, statType)
            );
        }
    }

    /**
     * Gets the StatName associated with a unique string name
     * @throws IllegalArgumentException if stat name does not exist
     * @param name the string name of the stat
     * @return the StatName enum associated with that String name
     */
    public static EventStatName from(String name) {
        if (isValidName(name) == false) {
            throw new IllegalArgumentException(String.format(Locale.ROOT, "Event stat not found: %s", name));
        }
        return BY_NAME.get(name);
    }

    /**
     * Determines whether a given string is a valid stat name
     * @param name
     * @return whether the name is valid
     */
    public static boolean isValidName(String name) {
        return BY_NAME.containsKey(name);
    }

    /**
     * Gets the full dot notation path of the stat, defining its location in the response body
     * @return the destination dot notation path of the stat value
     */
    public String getFullPath() {
        if (StringUtils.isBlank(path)) {
            return nameString;
        }
        return String.join(".", path, nameString);
    }

    /**
     * Gets the version the stat was added
     * @return the version the stat was added
     */
    public Version version() {
        return this.version;
    }

    @Override
    public String toString() {
        return getNameString();
    }
}
