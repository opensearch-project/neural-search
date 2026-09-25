/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.sparse.codec;

import lombok.SneakyThrows;
import org.apache.lucene.codecs.CompoundDirectory;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.opensearch.neuralsearch.sparse.AbstractSparseTestBase;

import java.util.Set;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * The engine file was never written into the .cfs, so the delegate cannot serve it. These assert the
 * routing that makes it readable anyway, and that nothing else is diverted.
 */
public class SparseCompoundDirectoryTests extends AbstractSparseTestBase {

    private CompoundDirectory delegate;
    private Directory dir;
    private SparseCompoundDirectory sparseCompoundDirectory;

    @SneakyThrows
    @Override
    public void setUp() {
        super.setUp();
        delegate = mock(CompoundDirectory.class);
        dir = mock(Directory.class);
        sparseCompoundDirectory = new SparseCompoundDirectory(delegate, dir);
    }

    @SneakyThrows
    public void testOpenInputReadsEngineFileFromSegmentDirectory() {
        IndexInput expected = mock(IndexInput.class);
        when(dir.openInput("_seg_101_field.nsparsec", IOContext.DEFAULT)).thenReturn(expected);

        assertSame(expected, sparseCompoundDirectory.openInput("_seg_101_field.nsparsec", IOContext.DEFAULT));
        verify(delegate, never()).openInput("_seg_101_field.nsparsec", IOContext.DEFAULT);
    }

    @SneakyThrows
    public void testOpenInputDelegatesEverythingElse() {
        IndexInput expected = mock(IndexInput.class);
        when(delegate.openInput("_seg_Lucene99.doc", IOContext.DEFAULT)).thenReturn(expected);

        assertSame(expected, sparseCompoundDirectory.openInput("_seg_Lucene99.doc", IOContext.DEFAULT));
        verify(dir, never()).openInput("_seg_Lucene99.doc", IOContext.DEFAULT);
    }

    @SneakyThrows
    public void testOpenInputDelegatesEngineFileWithoutCompoundSuffix() {
        // A bare .nsparse name inside a compound segment is not what the format writes, so it must
        // not be diverted -- only the "c" sibling lives outside the .cfs.
        IndexInput expected = mock(IndexInput.class);
        when(delegate.openInput("_seg_101_field.nsparse", IOContext.DEFAULT)).thenReturn(expected);

        assertSame(expected, sparseCompoundDirectory.openInput("_seg_101_field.nsparse", IOContext.DEFAULT));
        verify(dir, never()).openInput("_seg_101_field.nsparse", IOContext.DEFAULT);
    }

    @SneakyThrows
    public void testMetadataOperationsDelegate() {
        when(delegate.listAll()).thenReturn(new String[] { "a", "b" });
        when(delegate.fileLength("a")).thenReturn(42L);
        when(delegate.getPendingDeletions()).thenReturn(Set.of("c"));

        assertArrayEquals(new String[] { "a", "b" }, sparseCompoundDirectory.listAll());
        assertEquals(42L, sparseCompoundDirectory.fileLength("a"));
        assertEquals(Set.of("c"), sparseCompoundDirectory.getPendingDeletions());

        sparseCompoundDirectory.checkIntegrity();
        verify(delegate).checkIntegrity();
    }

    @SneakyThrows
    public void testCloseClosesDelegateOnly() {
        sparseCompoundDirectory.close();

        verify(delegate).close();
        verify(dir, never()).close();
    }
}
