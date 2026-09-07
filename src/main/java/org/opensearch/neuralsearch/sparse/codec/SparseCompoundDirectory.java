/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.sparse.codec;

import org.apache.lucene.codecs.CompoundDirectory;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.opensearch.neuralsearch.sparse.algorithm.SparseEngine;
import org.opensearch.neuralsearch.sparse.common.SparseConstants;

import java.io.IOException;
import java.util.Set;

/**
 * The read side of {@link SparseCompoundFormat}: a compound reader that serves the native engine
 * file from the segment directory instead of from the compound file.
 *
 * The engine file was never written into the {@code .cfs}, so the delegate does not know its name.
 * {@link #openInput} sends {@code .nsparsec} names to the underlying directory and everything else
 * to the delegate.
 */
public class SparseCompoundDirectory extends CompoundDirectory {
    private final CompoundDirectory delegate;
    private final Directory dir;

    /**
     * @param delegate the codec's own compound reader
     * @param dir      the segment directory holding the standalone engine file
     */
    public SparseCompoundDirectory(CompoundDirectory delegate, Directory dir) {
        this.delegate = delegate;
        this.dir = dir;
    }

    @Override
    public void checkIntegrity() throws IOException {
        delegate.checkIntegrity();
    }

    @Override
    public String[] listAll() throws IOException {
        return delegate.listAll();
    }

    @Override
    public long fileLength(String name) throws IOException {
        return delegate.fileLength(name);
    }

    @Override
    public IndexInput openInput(String name, IOContext context) throws IOException {
        if (name.endsWith(SparseEngine.NATIVE.extension() + SparseConstants.COMPOUND_EXTENSION)) {
            return dir.openInput(name, context);
        }
        return delegate.openInput(name, context);
    }

    @Override
    public void close() throws IOException {
        delegate.close();
    }

    @Override
    public Set<String> getPendingDeletions() throws IOException {
        return delegate.getPendingDeletions();
    }
}
