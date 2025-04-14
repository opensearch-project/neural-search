/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.sparse.codec;

import org.apache.lucene.codecs.FieldsConsumer;
import org.apache.lucene.codecs.NormsProducer;
import org.apache.lucene.index.Fields;

import java.io.IOException;

public class SparsePostingsWriter extends FieldsConsumer {
    @Override
    public void write(Fields fields, NormsProducer norms) throws IOException {

    }

    @Override
    public void close() throws IOException {

    }
}
