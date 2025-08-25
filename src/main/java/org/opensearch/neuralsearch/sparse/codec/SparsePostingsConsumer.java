/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.sparse.codec;

/**
 * This class is responsible for writing sparse postings to the index
 */
public class SparsePostingsConsumer {

    static final String CODEC_NAME = "SparsePostingsProducer";

    // Initial format
    public static final int VERSION_START = 1;
    public static final int VERSION_CURRENT = VERSION_START;

    /** Extension of terms file */
    static final String TERMS_EXTENSION = "sit";
    static final String POSTING_EXTENSION = "sip";
}
