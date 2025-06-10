/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.stats.info;

import lombok.Getter;
import org.apache.commons.lang.StringUtils;
import org.opensearch.Version;
import org.opensearch.neuralsearch.stats.common.StatName;

import java.util.Arrays;
import java.util.Locale;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Enum that contains all info stat names, paths, and types
 * WE SHOULD AVOID CHANGING THE ORDER OF THESE STAT ENUMS! The ordinal is used in StreamInput/Output.
 * Changing the order will break the mixed cluster version upgrade version case.
 */
@Getter
public enum InfoStatName implements StatName {
    /** Provides information about the cluster version */
    CLUSTER_VERSION("cluster_version", "", InfoStatType.INFO_STRING, Version.V_3_0_0),
    /** Counts text embedding processors in pipelines */
    TEXT_EMBEDDING_PROCESSORS("text_embedding_processors_in_pipelines", "processors.ingest", InfoStatType.INFO_COUNTER, Version.V_3_0_0),
    /** Counts text embedding processors with skip existing embeddings enabled */
    TEXT_EMBEDDING_SKIP_EXISTING_PROCESSORS(
        "text_embedding_skip_existing_processors",
        "processors.ingest",
        InfoStatType.INFO_COUNTER,
        Version.V_3_1_0
    ),
    /** Counts text chunking processors */
    TEXT_CHUNKING_PROCESSORS("text_chunking_processors", "processors.ingest", InfoStatType.INFO_COUNTER, Version.V_3_1_0),
    /** Counts delimiter-based text chunking processors */
    TEXT_CHUNKING_DELIMITER_PROCESSORS(
        "text_chunking_delimiter_processors",
        "processors.ingest",
        InfoStatType.INFO_COUNTER,
        Version.V_3_1_0
    ),
    /** Counts fixed token length text chunking processors */
    TEXT_CHUNKING_FIXED_TOKEN_LENGTH_PROCESSORS(
        "text_chunking_fixed_token_length_processors",
        "processors.ingest",
        InfoStatType.INFO_COUNTER,
        Version.V_3_1_0
    ),
    /** Counts fixed character length text chunking processors */
    TEXT_CHUNKING_FIXED_CHAR_LENGTH_PROCESSORS(
        "text_chunking_fixed_char_length_processors",
        "processors.ingest",
        InfoStatType.INFO_COUNTER,
        Version.V_3_1_0
    ),
    /** Counts normalization processors */
    NORMALIZATION_PROCESSORS("normalization_processors", "processors.search.hybrid", InfoStatType.INFO_COUNTER, Version.V_3_1_0),
    /** Counts L2 normalization technique processors */
    NORM_TECHNIQUE_L2_PROCESSORS("norm_l2_processors", "processors.search.hybrid", InfoStatType.INFO_COUNTER, Version.V_3_1_0),
    /** Counts min-max normalization technique processors */
    NORM_TECHNIQUE_MINMAX_PROCESSORS("norm_minmax_processors", "processors.search.hybrid", InfoStatType.INFO_COUNTER, Version.V_3_1_0),
    /** Counts z-score normalization technique processors */
    NORM_TECHNIQUE_ZSCORE_PROCESSORS("norm_zscore_processors", "processors.search.hybrid", InfoStatType.INFO_COUNTER, Version.V_3_1_0),
    /** Counts arithmetic combination technique processors */
    COMB_TECHNIQUE_ARITHMETIC_PROCESSORS(
        "comb_arithmetic_processors",
        "processors.search.hybrid",
        InfoStatType.INFO_COUNTER,
        Version.V_3_1_0
    ),
    /** Counts geometric combination technique processors */
    COMB_TECHNIQUE_GEOMETRIC_PROCESSORS(
        "comb_geometric_processors",
        "processors.search.hybrid",
        InfoStatType.INFO_COUNTER,
        Version.V_3_1_0
    ),
    /** Counts harmonic combination technique processors */
    COMB_TECHNIQUE_HARMONIC_PROCESSORS("comb_harmonic_processors", "processors.search.hybrid", InfoStatType.INFO_COUNTER, Version.V_3_1_0),
    /** Counts rank-based normalization processors */
    RRF_PROCESSORS("rank_based_normalization_processors", "processors.search.hybrid", InfoStatType.INFO_COUNTER, Version.V_3_1_0),
    /** Counts reciprocal rank fusion combination technique processors */
    COMB_TECHNIQUE_RRF_PROCESSORS("comb_rrf_processors", "processors.search.hybrid", InfoStatType.INFO_COUNTER, Version.V_3_1_0),
    /** Counts text-image embedding processors */
    TEXT_IMAGE_EMBEDDING_PROCESSORS("text_image_embedding_processors", "processors.ingest", InfoStatType.INFO_COUNTER, Version.V_3_1_0),
    /** Counts sparse encoding processors */
    SPARSE_ENCODING_PROCESSORS("sparse_encoding_processors", "processors.ingest", InfoStatType.INFO_COUNTER, Version.V_3_1_0),
    /** Counts neural query enricher processors */
    NEURAL_QUERY_ENRICHER_PROCESSORS("neural_query_enricher_processors", "processors.search", InfoStatType.INFO_COUNTER, Version.V_3_1_0),
    /** Counts neural sparse two-phase processors */
    NEURAL_SPARSE_TWO_PHASE_PROCESSORS(
        "neural_sparse_two_phase_processors",
        "processors.search",
        InfoStatType.INFO_COUNTER,
        Version.V_3_1_0
    ),
    /** Counts rerank by field processors */
    RERANK_BY_FIELD_PROCESSORS("rerank_by_field_processors", "processors.search", InfoStatType.INFO_COUNTER, Version.V_3_1_0),
    /** Counts ML reranking processors */
    RERANK_ML_PROCESSORS("rerank_ml_processors", "processors.search", InfoStatType.INFO_COUNTER, Version.V_3_1_0),;

    private final String nameString;
    private final String path;
    private final InfoStatType statType;
    private final Version version;

    private static final Map<String, InfoStatName> BY_NAME = Arrays.stream(values())
        .collect(Collectors.toMap(stat -> stat.nameString, stat -> stat));

    /**
     * Constructor
     * @param nameString the unique name of the stat.
     * @param path the unique path of the stat
     * @param statType the category of stat
     * @param version the version the stat is added
     */
    InfoStatName(String nameString, String path, InfoStatType statType, Version version) {
        this.nameString = nameString;
        this.path = path;
        this.statType = statType;
        this.version = version;
    }

    /**
     * Gets the StatName associated with a unique string name
     * @throws IllegalArgumentException if stat name does not exist
     * @param name the string name of the stat
     * @return the StatName enum associated with that String name
     */
    public static InfoStatName from(String name) {
        if (isValidName(name) == false) {
            throw new IllegalArgumentException(String.format(Locale.ROOT, "Info stat not found: %s", name));
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
