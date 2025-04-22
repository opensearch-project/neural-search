/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.sparse.codec;

import org.apache.lucene.codecs.FieldsConsumer;
import org.apache.lucene.codecs.FieldsProducer;
import org.apache.lucene.codecs.PostingsFormat;
import org.apache.lucene.index.SegmentReadState;
import org.apache.lucene.index.SegmentWriteState;

import java.io.IOException;

/**
 * Format for sparse vector postings.
 */
public class SparsePostingsFormat extends PostingsFormat {
    private PostingsFormat delegate;

    public SparsePostingsFormat(PostingsFormat delegate) {
        super(delegate.getName());
        this.delegate = delegate;
    }

    @Override
    public FieldsConsumer fieldsConsumer(SegmentWriteState state) throws IOException {
        return new SparsePostingsConsumer(this.delegate.fieldsConsumer(state), state);
    }

    @Override
    public FieldsProducer fieldsProducer(SegmentReadState state) throws IOException {
        return new SparsePostingsProducer(this.delegate.fieldsProducer(state), state);
    }
}
