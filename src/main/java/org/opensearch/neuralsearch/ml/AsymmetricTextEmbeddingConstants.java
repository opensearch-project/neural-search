/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.ml;

/**
 * Constants for asymmetric E5 text embedding functionality (both local and remote)
 */
public final class AsymmetricTextEmbeddingConstants {

    // Remote model parameter keys
    public static final String TEXTS_KEY = "texts";
    public static final String CONTENT_TYPE_KEY = "content_type";

    // Content type values
    public static final String QUERY_CONTENT_TYPE = "query";
    public static final String PASSAGE_CONTENT_TYPE = "passage";

    // Default prefixes for local models
    public static final String DEFAULT_QUERY_PREFIX = "query: ";
    public static final String DEFAULT_PASSAGE_PREFIX = "passage: ";

    // Model configuration keys
    public static final String QUERY_PREFIX_CONFIG_KEY = "query_prefix";
    public static final String PASSAGE_PREFIX_CONFIG_KEY = "passage_prefix";

    private AsymmetricTextEmbeddingConstants() {}
}
