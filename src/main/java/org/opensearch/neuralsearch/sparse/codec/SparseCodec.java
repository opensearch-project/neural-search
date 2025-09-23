/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.sparse.codec;

import org.apache.lucene.codecs.Codec;
import org.apache.lucene.codecs.DocValuesFormat;
import org.apache.lucene.codecs.FilterCodec;
import org.apache.lucene.codecs.PostingsFormat;
import org.apache.lucene.backward_codecs.lucene101.Lucene101Codec;

/**
 * SparseCodec is used to encode and decode sparse vector related data structures.
 */
public class SparseCodec extends FilterCodec {
    private static final String NAME = "Sparse10010Codec";
    public static final Codec DEFAULT_DELEGATE = new Lucene101Codec();

    public SparseCodec() {
        this(DEFAULT_DELEGATE);
    }

    /**
     * Sole constructor. When subclassing this codec, create a no-arg ctor and pass the delegate codec
     * and a unique name to this ctor.
     *
     * @param delegate the delegate codec
     */
    public SparseCodec(Codec delegate) {
        super(NAME, delegate);
    }

    @Override
    public DocValuesFormat docValuesFormat() {
        return new SparseDocValuesFormat(delegate.docValuesFormat());
    }

    @Override
    public PostingsFormat postingsFormat() {
        return new SparsePostingsFormat(this.delegate.postingsFormat());
    }
}
