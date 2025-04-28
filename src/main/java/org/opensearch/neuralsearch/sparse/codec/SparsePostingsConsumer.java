/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.sparse.codec;

import org.apache.lucene.codecs.FieldsConsumer;
import org.apache.lucene.codecs.NormsProducer;
import org.apache.lucene.index.Fields;
import org.apache.lucene.index.FilterLeafReader;
import org.apache.lucene.index.MergeState;
import org.apache.lucene.index.SegmentWriteState;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.util.BytesRef;
import org.opensearch.neuralsearch.sparse.SparseTokensField;
import org.opensearch.neuralsearch.sparse.common.MergeHelper;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * This class is responsible for writing sparse postings to the index
 */
public class SparsePostingsConsumer extends FieldsConsumer {
    private final FieldsConsumer delegate;
    private final SegmentWriteState state;

    public SparsePostingsConsumer(FieldsConsumer delegate, SegmentWriteState state) {
        super();
        this.delegate = delegate;
        this.state = state;
    }

    @Override
    public void write(Fields fields, NormsProducer norms) throws IOException {
        List<String> nonSparseFields = new ArrayList<>();
        List<String> sparseFields = new ArrayList<>();
        for (String field : fields) {
            if (!SparseTokensField.isSparseField(this.state.fieldInfos.fieldInfo(field))) {
                nonSparseFields.add(field);
            } else {
                sparseFields.add(field);
            }
        }
        if (!nonSparseFields.isEmpty()) {
            Fields maskedFields = new FilterLeafReader.FilterFields(fields) {
                @Override
                public Iterator<String> iterator() {
                    return nonSparseFields.iterator();
                }
            };
            this.delegate.write(maskedFields, norms);
        }

        String lastField = null;
        for (String field : sparseFields) {
            assert lastField == null || lastField.compareTo(field) < 0;
            lastField = field;

            Terms terms = fields.terms(field);
            if (terms == null) {
                continue;
            }

            ClusteredPostingTermsWriter clusteredPostingTermsWriter = new ClusteredPostingTermsWriter(
                this.state,
                this.state.fieldInfos.fieldInfo(field)
            );

            TermsEnum termsEnum = terms.iterator();
            while (true) {
                BytesRef term = termsEnum.next();
                if (term == null) {
                    break;
                }
                clusteredPostingTermsWriter.write(term, termsEnum, norms);
            }

            // termsWriter.finish();
        }
    }

    @Override
    public void merge(MergeState mergeState, NormsProducer norms) throws IOException {
        // merge non-sparse fields
        super.merge(mergeState, norms);
        // merge sparse fields
        SparsePostingsReader sparsePostingsReader = new SparsePostingsReader(mergeState);
        sparsePostingsReader.merge();
        MergeHelper.clearInMemoryData(mergeState, null, InMemoryClusteredPosting::clearIndex);
    }

    @Override
    public void close() throws IOException {
        this.delegate.close();
    }
}
