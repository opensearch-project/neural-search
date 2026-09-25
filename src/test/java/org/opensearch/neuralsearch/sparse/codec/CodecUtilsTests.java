/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.sparse.codec;

import org.apache.lucene.codecs.Codec;
import org.apache.lucene.index.SegmentInfo;
import org.apache.lucene.store.ByteBuffersDirectory;
import org.apache.lucene.util.StringHelper;
import org.apache.lucene.util.Version;
import org.opensearch.neuralsearch.sparse.AbstractSparseTestBase;
import org.opensearch.neuralsearch.sparse.algorithm.SparseEngine;

import java.util.Collections;
import java.util.List;
import java.util.Set;

/**
 * The engine file name is the only contract between the writer that creates the file and the
 * reader that later has to find it again, so these assert the exact shape and the lookup rules --
 * including the compound-file suffix, which is what {@link SparseCompoundFormat} leaves behind.
 */
public class CodecUtilsTests extends AbstractSparseTestBase {

    private static final String EXTENSION = SparseEngine.NATIVE.extension();

    public void testBuildIndexFileNameFormat() {
        assertEquals("_seg1_101_my_field.nsparse", CodecUtils.buildIndexFileName("_seg1", "101", "my_field", EXTENSION));
    }

    public void testBuildIndexFileNameDistinguishesVersions() {
        String v101 = CodecUtils.buildIndexFileName("_seg1", "101", "f", EXTENSION);
        String v102 = CodecUtils.buildIndexFileName("_seg1", "102", "f", EXTENSION);

        // The version is what keeps a file written by an earlier payload layout from being read back
        assertNotEquals(v101, v102);
    }

    public void testGetEngineFilesMatchesFieldSuffix() {
        SegmentInfo segmentInfo = segmentInfoWithFiles(false, "_0_101_field.nsparse", "_0_101_other.nsparse");

        assertEquals(List.of("_0_101_field.nsparse"), CodecUtils.getEngineFiles(EXTENSION, "field", segmentInfo));
    }

    public void testGetEngineFilesIgnoresUnrelatedFiles() {
        SegmentInfo segmentInfo = segmentInfoWithFiles(false, "_0_101_field.nsparse", "_0_Lucene99.doc");

        assertEquals(List.of("_0_101_field.nsparse"), CodecUtils.getEngineFiles(EXTENSION, "field", segmentInfo));
    }

    public void testGetEngineFilesUsesCompoundSuffixForCompoundSegment() {
        // A compound segment keeps the engine file beside the .cfs under a trailing "c", so the
        // plain name must no longer match and the "c" name must.
        SegmentInfo segmentInfo = segmentInfoWithFiles(true, "_0_101_field.nsparsec");

        assertEquals(List.of("_0_101_field.nsparsec"), CodecUtils.getEngineFiles(EXTENSION, "field", segmentInfo));
        assertTrue(CodecUtils.getEngineFiles(EXTENSION, "field", segmentInfoWithFiles(true, "_0_101_field.nsparse")).isEmpty());
    }

    public void testGetEngineFilesWithEmptyFieldNameMatchesAnyField() {
        SegmentInfo segmentInfo = segmentInfoWithFiles(false, "_0_101_a.nsparse", "_0_101_bb.nsparse");

        assertEquals(2, CodecUtils.getEngineFiles(EXTENSION, "", segmentInfo).size());
    }

    public void testGetEngineFilesSortedByLength() {
        SegmentInfo segmentInfo = segmentInfoWithFiles(false, "_0_101_longer.nsparse", "_0_101_ab.nsparse");

        assertEquals(List.of("_0_101_ab.nsparse", "_0_101_longer.nsparse"), CodecUtils.getEngineFiles(EXTENSION, "", segmentInfo));
    }

    public void testGetEngineFilesReturnsEmptyWhenFieldHasNoFile() {
        SegmentInfo segmentInfo = segmentInfoWithFiles(false, "_0_101_field.nsparse");

        assertTrue(CodecUtils.getEngineFiles(EXTENSION, "absent", segmentInfo).isEmpty());
    }

    /**
     * Built here rather than via {@code TestsPrepareUtils} because the compound flag is only
     * settable at construction: {@code SegmentInfo#setUseCompoundFile} is package private.
     */
    private SegmentInfo segmentInfoWithFiles(boolean useCompoundFile, String... files) {
        SegmentInfo segmentInfo = new SegmentInfo(
            new ByteBuffersDirectory(),
            Version.LATEST,
            Version.LATEST,
            "_0",
            10,
            useCompoundFile,
            false,
            Codec.getDefault(),
            Collections.emptyMap(),
            new byte[StringHelper.ID_LENGTH],
            Collections.emptyMap(),
            null
        );
        segmentInfo.setFiles(Set.of(files));
        return segmentInfo;
    }
}
