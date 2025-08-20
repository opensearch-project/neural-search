/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.constants;

/**
 * Constants for semantic field
 */
public class SemanticFieldConstants {
    /**
     * Name of the model id parameter. We use this key to define the id of the ML model that we will use for the
     * semantic field.
     */
    public static final String MODEL_ID = "model_id";

    /**
     * Name of the search model id parameter. We use this key to define the id of the ML model that we will use to
     * inference the query text during the search. If this parameter is not defined we will use the model_id instead.
     */
    public static final String SEARCH_MODEL_ID = "search_model_id";

    /**
     * Name of the raw field type parameter. We use this key to define the field type for the raw data. It will control
     * how to store and query the raw data.
     */
    public static final String RAW_FIELD_TYPE = "raw_field_type";

    /**
     * Name of the raw field type parameter. We use this key to define a custom field name for the semantic info.
     */
    public static final String SEMANTIC_INFO_FIELD_NAME = "semantic_info_field_name";

    /**
     * Default suffix for semantic info field name. It will be used to construct the field name of the semantic info.
     */
    public static final String DEFAULT_SEMANTIC_INFO_FIELD_NAME_SUFFIX = "_semantic_info";

    /**
     * Name of the field to control if we should do chunking for the semantic field. By default, the chunking is
     * disabled to not downgrade the search performance.
     */
    public static final String CHUNKING = "chunking";

    /**
     * Name of the field for search analyzer parameter. With this field set up, user has not to specify
     * it during query time.
     */
    public static final String SEMANTIC_FIELD_SEARCH_ANALYZER = "semantic_field_search_analyzer";

    /**
     * Name of the field for dense embedding config. The config will be used to override config of the underlying
     * knn_vector field.
     */
    public static final String DENSE_EMBEDDING_CONFIG = "dense_embedding_config";

    /**
     * Name of the field for sparse encoding config. The config will be used to control how to do sparse encoding.
     * {@link org.opensearch.neuralsearch.mapper.dto.SparseEncodingConfig}
     */
    public static final String SPARSE_ENCODING_CONFIG = "sparse_encoding_config";

    /**
     * Name of the field to configure if we should skip the embedding generation for the semantic field with existing
     * reusable embedding. We think the embedding is reusable if it exists and the semantic field value and the
     * model is not changed.
     */
    public static final String SKIP_EXISTING_EMBEDDING = "skip_existing_embedding";
}
