/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.highlight;

/**
 * Constants for semantic highlighting functionality
 */
public final class SemanticHighlightingConstants {
    // System-generated factory and processor types
    public static final String SYSTEM_FACTORY_TYPE = "semantic_highlighting_system_factory";
    public static final String PROCESSOR_TYPE = "semantic_highlighting";

    // Default processor tags and descriptions
    public static final String DEFAULT_PROCESSOR_TAG = "semantic-highlighter";
    public static final String DEFAULT_PROCESSOR_DESCRIPTION = "System-generated semantic highlighting processor";

    // configuration keys
    public static final String HIGHLIGHTER_TYPE = "semantic";
    public static final String MODEL_ID = "model_id";
    public static final String BATCH_INFERENCE = "batch_inference";
    public static final String MAX_INFERENCE_BATCH_SIZE = "max_inference_batch_size";
    public static final String PRE_TAG = "pre_tag";
    public static final String POST_TAG = "post_tag";

    // Default values
    public static final String DEFAULT_PRE_TAG = "<em>";
    public static final String DEFAULT_POST_TAG = "</em>";
    public static final int DEFAULT_MAX_INFERENCE_BATCH_SIZE = 100;

    // ML inference keys
    public static final String HIGHLIGHTS_KEY = "highlights";
    public static final String START_KEY = "start";
    public static final String END_KEY = "end";
    public static final String QUESTION_KEY = "question";
    public static final String CONTEXT_KEY = "context";
    public static final String INPUTS_KEY = "inputs";
}
