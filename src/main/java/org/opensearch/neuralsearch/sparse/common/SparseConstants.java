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
    public static final String SUMMARY_PRUNE_RATIO_FIELD = "summary_prune_ratio";
    public static final String SEISMIC = "seismic";
    public static final String CLUSTER_RATIO_FIELD = "cluster_ratio";
    public static final String APPROXIMATE_THRESHOLD_FIELD = "approximate_threshold";
    public static final String INDEX_THREAD_QTY = "indexThreadQty";
    public static final int MODULUS_FOR_SHORT = 65536;

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
    }
}
