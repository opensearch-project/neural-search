/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.sparse.codec;

import lombok.RequiredArgsConstructor;
import org.apache.lucene.codecs.BlockTermState;
import org.apache.lucene.index.SegmentWriteState;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.util.BytesRef;
import org.opensearch.common.util.io.IOUtils;

import java.io.IOException;

/**
 * Writer for sparse terms in Lucene index format.
 * Handles writing field metadata, terms, and block term states to index output.
 */
@RequiredArgsConstructor
public class SparseTermsLuceneWriter {
    private IndexOutput termsOut;
    private final String codecName;
    private final int version;
    private final CodecUtilWrapper codecUtilWrapper;

    /**
     * Initializes the writer with output stream and writes index header.
     *
     * @param termsOut the index output stream
     * @param state the segment write state
     * @throws IOException if an I/O error occurs
     */
    public void init(IndexOutput termsOut, SegmentWriteState state) throws IOException {
        this.termsOut = termsOut;
        codecUtilWrapper.writeIndexHeader(termsOut, codecName, version, state.segmentInfo.getId(), state.segmentSuffix);
    }

    /**
     * Closes the writer and writes footer with start file pointer.
     *
     * @param startFp the start file pointer
     * @throws IOException if an I/O error occurs
     */
    public void close(long startFp) throws IOException {
        this.termsOut.writeLong(startFp);
        codecUtilWrapper.writeFooter(this.termsOut);
    }

    /**
     * Writes the number of fields.
     *
     * @param fieldCount the field count
     * @throws IOException if an I/O error occurs
     */
    public void writeFieldCount(int fieldCount) throws IOException {
        termsOut.writeVInt(fieldCount);
    }

    /**
     * Writes the field number.
     *
     * @param fieldNumber the field number
     * @throws IOException if an I/O error occurs
     */
    public void writeFieldNumber(int fieldNumber) throws IOException {
        termsOut.writeVInt(fieldNumber);
    }

    /**
     * Writes the total size of terms.
     *
     * @param termsSize the terms size
     * @throws IOException if an I/O error occurs
     */
    public void writeTermsSize(long termsSize) throws IOException {
        termsOut.writeVLong(termsSize);
    }

    /**
     * Writes a term with its block term state.
     *
     * @param term the term bytes
     * @param state the block term state
     * @throws IOException if an I/O error occurs
     */
    public void writeTerm(BytesRef term, BlockTermState state) throws IOException {
        this.termsOut.writeVInt(term.length);
        this.termsOut.writeBytes(term.bytes, term.offset, term.length);
        this.termsOut.writeVLong(state.blockFilePointer);
    }

    /**
     * Closes the writer while handling any exceptions.
     */
    public void closeWithException() {
        IOUtils.closeWhileHandlingException(this.termsOut);
    }
}
