/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.sparse.codec.nativeindex;

import lombok.SneakyThrows;
import org.apache.lucene.codecs.Codec;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.SegmentInfo;
import org.apache.lucene.store.ByteBuffersDirectory;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.store.FilterDirectory;
import org.apache.lucene.util.StringHelper;
import org.apache.lucene.util.Version;
import org.opensearch.neuralsearch.sparse.AbstractSparseTestBase;

import java.io.IOException;
import java.util.Collections;
import java.util.Set;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Everything here stops short of {@code address()}, which would load the shared object: the parts
 * worth pinning without it are the path resolution and the decision of who owns the handle. A
 * reader with a core cache must share one index per (core, field) so a per-query load cannot
 * happen, and a reader without one must get its own, since there is no core close to free it.
 */
public class SegmentNativeIndexTests extends AbstractSparseTestBase {

    private static final String FIELD = "sparse_field";
    private static final String ENGINE_FILE = "_0_101_" + FIELD + ".nsparse";

    @SneakyThrows
    public void testOpenReusesTheIndexAlreadyLoadedForTheCore() {
        SegmentInfo segmentInfo = fsSegmentInfo(ENGINE_FILE);
        LeafReader reader = readerWithCore();

        SegmentNativeIndex first = SegmentNativeIndex.open(reader, segmentInfo, FIELD);
        SegmentNativeIndex second = SegmentNativeIndex.open(reader, segmentInfo, FIELD);

        assertSame(first, second);
    }

    @SneakyThrows
    public void testOpenKeepsFieldsOfTheSameCoreApart() {
        String otherField = "other_field";
        SegmentInfo segmentInfo = fsSegmentInfo(ENGINE_FILE, "_0_101_" + otherField + ".nsparse");
        LeafReader reader = readerWithCore();

        assertNotSame(SegmentNativeIndex.open(reader, segmentInfo, FIELD), SegmentNativeIndex.open(reader, segmentInfo, otherField));
    }

    @SneakyThrows
    public void testOpenIsolatesDifferentCores() {
        SegmentInfo segmentInfo = fsSegmentInfo(ENGINE_FILE);

        assertNotSame(
            SegmentNativeIndex.open(readerWithCore(), segmentInfo, FIELD),
            SegmentNativeIndex.open(readerWithCore(), segmentInfo, FIELD)
        );
    }

    @SneakyThrows
    public void testOpenWithoutCoreCacheReturnsAQueryScopedIndex() {
        SegmentInfo segmentInfo = fsSegmentInfo(ENGINE_FILE);
        LeafReader reader = mock(LeafReader.class);
        when(reader.getCoreCacheHelper()).thenReturn(null);

        // Nothing owns the handle, so it must not be cached and shared
        assertNotSame(SegmentNativeIndex.open(reader, segmentInfo, FIELD), SegmentNativeIndex.open(reader, segmentInfo, FIELD));
    }

    @SneakyThrows
    public void testCloseOfAQueryScopedIndexIsIdempotent() {
        SegmentInfo segmentInfo = fsSegmentInfo(ENGINE_FILE);
        LeafReader reader = mock(LeafReader.class);
        when(reader.getCoreCacheHelper()).thenReturn(null);
        SegmentNativeIndex index = SegmentNativeIndex.open(reader, segmentInfo, FIELD);

        // Never loaded, so freeing is a no-op -- but it must still refuse to hand out an address
        index.close();
        index.close();

        expectThrows(org.apache.lucene.store.AlreadyClosedException.class, index::address);
    }

    public void testOpenFailsWhenTheFieldHasNoEngineFile() {
        SegmentInfo segmentInfo = fsSegmentInfo("_0_101_another_field.nsparse");
        LeafReader reader = readerWithCore();

        IOException e = expectThrows(IOException.class, () -> SegmentNativeIndex.open(reader, segmentInfo, FIELD));
        assertTrue(e.getMessage().contains(FIELD));
    }

    public void testOpenFailsOnADirectoryWithNoFilesystemPath() {
        SegmentInfo segmentInfo = segmentInfo(new ByteBuffersDirectory(), ENGINE_FILE);
        LeafReader reader = readerWithCore();

        // nsparse maps the file itself, so a directory that cannot produce a path is unusable
        IOException e = expectThrows(IOException.class, () -> SegmentNativeIndex.open(reader, segmentInfo, FIELD));
        assertTrue(e.getMessage().contains("Cannot resolve a filesystem path"));
    }

    @SneakyThrows
    public void testResolvesThroughADirectoryWrapper() {
        Directory wrapped = new FilterDirectory(FSDirectory.open(createTempDir())) {
        };

        // A wrapped FSDirectory still has a path underneath, so this must not throw
        assertNotNull(SegmentNativeIndex.open(readerWithCore(), segmentInfo(wrapped, ENGINE_FILE), FIELD));
    }

    /** A reader whose core lifecycle can own the handle. Each call is a distinct core. */
    private LeafReader readerWithCore() {
        LeafReader reader = mock(LeafReader.class);
        IndexReader.CacheHelper cacheHelper = mock(IndexReader.CacheHelper.class);
        when(cacheHelper.getKey()).thenReturn(mock(IndexReader.CacheKey.class));
        when(reader.getCoreCacheHelper()).thenReturn(cacheHelper);
        return reader;
    }

    @SneakyThrows
    private SegmentInfo fsSegmentInfo(String... files) {
        return segmentInfo(FSDirectory.open(createTempDir()), files);
    }

    private SegmentInfo segmentInfo(Directory directory, String... files) {
        SegmentInfo segmentInfo = new SegmentInfo(
            directory,
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
        segmentInfo.setFiles(Set.of(files));
        return segmentInfo;
    }
}
