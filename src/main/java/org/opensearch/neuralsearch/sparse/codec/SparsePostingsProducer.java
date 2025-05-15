/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.sparse.codec;

import lombok.Getter;
import org.apache.lucene.codecs.FieldsProducer;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.SegmentReadState;
import org.apache.lucene.index.Terms;
import org.opensearch.neuralsearch.sparse.SparseTokensField;
import org.opensearch.neuralsearch.sparse.common.InMemoryKey;

import java.io.IOException;
import java.util.Iterator;

/**
 * SparsePostingsProducer vends SparseTerms for each sparse field.
 * It is used to read sparse postings from the index.
 */
@Getter
public class SparsePostingsProducer extends FieldsProducer {

    private final FieldsProducer delegate;
    private final SegmentReadState state;
    private final SparseTermsLuceneReader reader;

    public SparsePostingsProducer(FieldsProducer delegate, SegmentReadState state) throws IOException {
        super();
        this.delegate = delegate;
        this.state = state;
        this.reader = new SparseTermsLuceneReader(state);
    }

    @Override
    public void close() throws IOException {
        this.delegate.close();
        this.reader.close();
    }

    @Override
    public void checkIntegrity() throws IOException {
        this.delegate.checkIntegrity();
    }

    @Override
    public Iterator<String> iterator() {
        return this.delegate.iterator();
    }

    @Override
    public Terms terms(String field) throws IOException {
        FieldInfo fieldInfo = this.state.fieldInfos.fieldInfo(field);
        if (!SparseTokensField.isSparseField(fieldInfo)) {
            return delegate.terms(field);
        }
        InMemoryKey.IndexKey key = new InMemoryKey.IndexKey(this.state.segmentInfo, fieldInfo);
        return new SparseTerms(key, reader, field);
    }

    @Override
    public int size() {
        return 0;
    }
}
