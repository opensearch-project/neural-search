/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.sparse.codec;

import org.apache.lucene.codecs.FieldsConsumer;
import org.apache.lucene.codecs.NormsProducer;
import org.apache.lucene.index.Fields;
import org.apache.lucene.index.FilterLeafReader;
import org.apache.lucene.index.SegmentWriteState;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.util.BytesRef;
import org.opensearch.neuralsearch.sparse.SparseTokensField;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

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
        Fields maskedFields = new FilterLeafReader.FilterFields(fields) {
            @Override
            public Iterator<String> iterator() {
                return nonSparseFields.iterator();
            }
        };
        this.delegate.write(maskedFields, norms);

        String lastField = null;
        for (String field : sparseFields) {
            assert lastField == null || lastField.compareTo(field) < 0;
            lastField = field;

            Terms terms = fields.terms(field);
            if (terms == null) {
                continue;
            }

            TermsEnum termsEnum = terms.iterator();
            // Lucene90BlockTreeTermsWriter.TermsWriter termsWriter = new
            // Lucene90BlockTreeTermsWriter.TermsWriter(fieldInfos.fieldInfo(field));
            while (true) {
                BytesRef term = termsEnum.next();
                // if (DEBUG) System.out.println("BTTW: next term " + term);

                if (term == null) {
                    break;
                }

                // if (DEBUG) System.out.println("write field=" + fieldInfo.name + " term=" +
                // ToStringUtils.bytesRefToString(term));
                // termsWriter.write(term, termsEnum, norms);
            }

            // termsWriter.finish();
        }
    }

    @Override
    public void close() throws IOException {
        this.delegate.close();
    }
}
