/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.sparse.codec;

import java.io.IOException;
import org.apache.lucene.index.Fields;
import org.apache.lucene.codecs.NormsProducer;
import lombok.extern.log4j.Log4j2;
import org.apache.lucene.codecs.FieldsConsumer;

/**
 * This class is responsible for writing sparse postings to the index
 */
@Log4j2
public class SparsePostingsConsumer extends FieldsConsumer {

    static final String CODEC_NAME = "SparsePostingsProducer";

    // Initial format
    public static final int VERSION_START = 1;
    public static final int VERSION_CURRENT = VERSION_START;

    /** Extension of terms file */
    static final String TERMS_EXTENSION = "sit";
    static final String POSTING_EXTENSION = "sip";

    @Override
    public void write(Fields fields, NormsProducer norms) throws IOException {
        // TODO: implement the write method
    }

    @Override
    public void close() throws IOException {
        // TODO: implement the close method
    }
}
