/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.sparse.codec;

import lombok.SneakyThrows;
import org.apache.lucene.codecs.Codec;
import org.apache.lucene.codecs.CompoundDirectory;
import org.apache.lucene.codecs.CompoundFormat;
import org.apache.lucene.index.SegmentInfo;
import org.apache.lucene.store.ByteBuffersDirectory;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.util.StringHelper;
import org.apache.lucene.util.Version;
import org.opensearch.neuralsearch.sparse.AbstractSparseTestBase;
import org.opensearch.neuralsearch.sparse.TestsPrepareUtils;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * nsparse loads an index by filesystem path, so the engine file must stay a standalone file rather
 * than becoming a slice inside the .cfs. These assert it is copied out and dropped from the segment
 * before the real format runs, and that a segment with no engine file is left untouched.
 */
public class SparseCompoundFormatTests extends AbstractSparseTestBase {

    private static final String ENGINE_FILE = "_0_101_field.nsparse";
    private static final String OTHER_FILE = "_0_Lucene99.doc";

    private CompoundFormat delegate;
    private SparseCompoundFormat sparseCompoundFormat;
    private Directory dir;

    @SneakyThrows
    @Override
    public void setUp() {
        super.setUp();
        delegate = mock(CompoundFormat.class);
        sparseCompoundFormat = new SparseCompoundFormat(delegate);
        dir = new ByteBuffersDirectory();
    }

    @SneakyThrows
    public void testWriteCopiesEngineFileOutOfTheCompoundFile() {
        createFiles(ENGINE_FILE, OTHER_FILE);
        SegmentInfo segmentInfo = segmentInfoWithFiles(ENGINE_FILE, OTHER_FILE);

        sparseCompoundFormat.write(dir, segmentInfo, IOContext.DEFAULT);

        // The standalone copy the native side will mmap
        assertTrue(Arrays.asList(dir.listAll()).contains(ENGINE_FILE + "c"));
        // Removed from the segment, so the delegate never folds it into the .cfs
        assertFalse(segmentInfo.files().contains(ENGINE_FILE));
        assertTrue(segmentInfo.files().contains(OTHER_FILE));
        verify(delegate).write(dir, segmentInfo, IOContext.DEFAULT);
    }

    @SneakyThrows
    public void testWriteCopiesEveryEngineFileInTheSegment() {
        String secondEngineFile = "_0_101_other.nsparse";
        createFiles(ENGINE_FILE, secondEngineFile, OTHER_FILE);
        SegmentInfo segmentInfo = segmentInfoWithFiles(ENGINE_FILE, secondEngineFile, OTHER_FILE);

        sparseCompoundFormat.write(dir, segmentInfo, IOContext.DEFAULT);

        List<String> written = Arrays.asList(dir.listAll());
        assertTrue(written.contains(ENGINE_FILE + "c"));
        assertTrue(written.contains(secondEngineFile + "c"));
        assertEquals(Set.of(OTHER_FILE), segmentInfo.files());
    }

    @SneakyThrows
    public void testWriteLeavesSegmentWithoutEngineFileUntouched() {
        createFiles(OTHER_FILE);
        SegmentInfo segmentInfo = segmentInfoWithFiles(OTHER_FILE);

        sparseCompoundFormat.write(dir, segmentInfo, IOContext.DEFAULT);

        assertEquals(Set.of(OTHER_FILE), segmentInfo.files());
        assertEquals(List.of(OTHER_FILE), Arrays.asList(dir.listAll()));
        verify(delegate).write(dir, segmentInfo, IOContext.DEFAULT);
    }

    @SneakyThrows
    public void testGetCompoundReaderWrapsDelegateReader() {
        SegmentInfo segmentInfo = TestsPrepareUtils.prepareSegmentInfo();
        when(delegate.getCompoundReader(dir, segmentInfo)).thenReturn(mock(CompoundDirectory.class));

        assertTrue(sparseCompoundFormat.getCompoundReader(dir, segmentInfo) instanceof SparseCompoundDirectory);
    }

    @SneakyThrows
    private void createFiles(String... names) {
        for (String name : names) {
            try (IndexOutput output = dir.createOutput(name, IOContext.DEFAULT)) {
                output.writeInt(1);
            }
        }
    }

    /**
     * The segment name has to be a single token: {@code SegmentInfo#setFiles} rewrites every name
     * through {@code namedForThisSegment}, which mangles a name containing an underscore.
     */
    private SegmentInfo segmentInfoWithFiles(String... files) {
        SegmentInfo segmentInfo = new SegmentInfo(
            dir,
            Version.LATEST,
            Version.LATEST,
            "_0",
            10,
            false,
            false,
            Codec.getDefault(),
            Collections.emptyMap(),
            new byte[StringHelper.ID_LENGTH],
            Collections.emptyMap(),
            null
        );
        segmentInfo.setFiles(new HashSet<>(Arrays.asList(files)));
        return segmentInfo;
    }
}
