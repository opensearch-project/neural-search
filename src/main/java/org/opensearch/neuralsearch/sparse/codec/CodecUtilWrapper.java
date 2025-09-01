/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.sparse.codec;

import org.apache.lucene.codecs.CodecUtil;
import org.apache.lucene.store.DataInput;
import org.apache.lucene.store.DataOutput;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;

import java.io.IOException;

/**
 * A wrapper on CodecUtil class to enable better testability.
 */
public class CodecUtilWrapper {
    public long retrieveChecksum(IndexInput in) throws IOException {
        return CodecUtil.retrieveChecksum(in);
    }

    public int footerLength() {
        return CodecUtil.footerLength();
    }

    public long checksumEntireFile(IndexInput input) throws IOException {
        return CodecUtil.checksumEntireFile(input);
    }

    public int checkIndexHeader(DataInput in, String codec, int minVersion, int maxVersion, byte[] expectedID, String expectedSuffix)
        throws IOException {
        return CodecUtil.checkIndexHeader(in, codec, minVersion, maxVersion, expectedID, expectedSuffix);
    }

    public void writeIndexHeader(DataOutput out, String codec, int version, byte[] id, String suffix) throws IOException {
        CodecUtil.writeIndexHeader(out, codec, version, id, suffix);
    }

    public void writeFooter(IndexOutput out) throws IOException {
        CodecUtil.writeFooter(out);
    }
}
