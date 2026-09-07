/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.sparse.common;

/**
 * Constants used throughout the sparse vector search implementation.
 * Contains field names, configuration parameters, and default values for SEISMIC algorithm.
 */
public final class SparseConstants {
    public static final String NAME_FIELD = "name";
    public static final String PARAMETERS_FIELD = "parameters";
    public static final String N_POSTINGS_FIELD = "n_postings";
    public static final String ENGINE_FIELD = "engine";
    public static final String FORWARD_INDEX_FIELD = "forward_index";
    public static final String CLUSTERING_BATCH_SIZE_FIELD = "clustering_batch_size";
    public static final String SUMMARY_PRUNE_RATIO_FIELD = "summary_prune_ratio";
    public static final String QUANTIZATION_CEILING_INGEST_FIELD = "quantization_ceiling_ingest";
    public static final String QUANTIZATION_CEILING_SEARCH_FIELD = "quantization_ceiling_search";
    public static final String SEISMIC = "seismic";
    public static final String CLUSTER_RATIO_FIELD = "cluster_ratio";
    public static final String APPROXIMATE_THRESHOLD_FIELD = "approximate_threshold";
    public static final String THREAD_POOL_NAME = "seismic_thread_pool";
    // Tokens are stored in a signed short[] to keep the memory footprint low, so they are folded
    // into the non-negative signed-short range [0, Short.MAX_VALUE] via this modulus. Using 32768
    // (not 65536) keeps every folded value <= Short.MAX_VALUE, so it round-trips without being
    // sign-extended to a negative int on read.
    public static final int MODULUS_FOR_SHORT = 32768;
    public static final String COMPOUND_EXTENSION = "c";

    /**
     * SEISMIC algorithm configuration constants and default values.
     */
    public static final class Seismic {
        public static final int DEFAULT_N_POSTINGS = -1;
        public static final float DEFAULT_SUMMARY_PRUNE_RATIO = 0.4f;
        public static final float DEFAULT_CLUSTER_RATIO = 0.1f;
        public static final int DEFAULT_APPROXIMATE_THRESHOLD = 1000000;
        public static final float DEFAULT_POSTING_PRUNE_RATIO = 0.0005f;
        public static final int DEFAULT_POSTING_MINIMUM_LENGTH = 160;
        public static final int DEFAULT_CLUSTERING_BATCH_SIZE = 1;
        public static final int MIN_CLUSTERING_BATCH_SIZE = 1;
        public static final int MAX_CLUSTERING_BATCH_SIZE = 10000;
        public static final float DEFAULT_QUANTIZATION_CEILING_INGEST = 3.0f;
        public static final float DEFAULT_QUANTIZATION_CEILING_SEARCH = 16.0f;
        // Mirrors nsparse's kDefaultBlockBudget: how many blocks a per_block forward index reads
        // per query. Sent explicitly only so the query's quantization range reaches the index --
        // there is no query parameter for it yet.
        public static final int DEFAULT_BLOCK_BUDGET = 50;
    }
}
