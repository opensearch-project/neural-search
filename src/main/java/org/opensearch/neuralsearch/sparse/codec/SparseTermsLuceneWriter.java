/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.sparse.codec;

import org.apache.lucene.codecs.BlockTermState;
import org.apache.lucene.codecs.CodecUtil;
import org.apache.lucene.index.SegmentWriteState;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.IOUtils;

import java.io.IOException;

public class SparseTermsLuceneWriter {
    private IndexOutput termsOut;
    private final int version;
    private final String codec_name;

    public SparseTermsLuceneWriter(String codec_name, int version) {
        this.codec_name = codec_name;
        this.version = version;
    }

    public void init(IndexOutput termsOut, SegmentWriteState state) throws IOException {
        this.termsOut = termsOut;
        CodecUtil.writeIndexHeader(termsOut, codec_name, version, state.segmentInfo.getId(), state.segmentSuffix);
    }

    public void close(long startFp) throws IOException {
        this.termsOut.writeLong(startFp);
        CodecUtil.writeFooter(this.termsOut);
    }

    public void writeFieldCount(int fieldCount) throws IOException {
        termsOut.writeVInt(fieldCount);
    }

    public void writeFieldNumber(int fieldNumber) throws IOException {
        termsOut.writeVInt(fieldNumber);
    }

    public void writeTermsSize(long termsSize) throws IOException {
        termsOut.writeVLong(termsSize);
    }

    public void writeTerm(BytesRef term, BlockTermState state) throws IOException {
        this.termsOut.writeVInt(term.length);
        this.termsOut.writeBytes(term.bytes, term.offset, term.length);
        this.termsOut.writeVLong(state.blockFilePointer);
    }

    public void closeWithException() {
        IOUtils.closeWhileHandlingException(this.termsOut);
    }
}
