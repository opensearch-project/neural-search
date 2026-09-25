/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.sparse.codec;

import lombok.AllArgsConstructor;
import org.apache.lucene.codecs.CompoundDirectory;
import org.apache.lucene.codecs.CompoundFormat;
import org.apache.lucene.index.SegmentInfo;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.opensearch.neuralsearch.sparse.algorithm.SparseEngine;
import org.opensearch.neuralsearch.sparse.common.SparseConstants;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Keeps the native engine file out of the segment's compound file.
 *
 * nsparse loads an index by filesystem path
 * ({@link org.opensearch.neuralsearch.sparse.codec.nativeindex.SegmentNativeIndex}), so it cannot
 * read one that has been folded into a {@code .cfs} as a slice. Before delegating, each
 * {@code .nsparse} file is copied to a {@code .nsparsec} sibling and removed from the segment's
 * file set, leaving a standalone file for the native side; {@link SparseCompoundDirectory} routes
 * reads of those names back to the real directory. Everything else is the delegate's.
 *
 * The copy source is not deleted here, and does not leak: both callers snapshot the segment's file
 * set before invoking this and delete it afterwards -- {@code DocumentsWriterPerThread
 * .sealFlushedSegment} on flush, {@code IndexWriter.mergeMiddle} on merge. Rebinding the set via
 * {@code setFiles} below leaves those snapshots holding the {@code .nsparse} name, so Lucene removes
 * it once the compound file is written.
 */
@AllArgsConstructor
public class SparseCompoundFormat extends CompoundFormat {
    private final CompoundFormat delegate;

    @Override
    public CompoundDirectory getCompoundReader(Directory dir, SegmentInfo si) throws IOException {
        return new SparseCompoundDirectory(delegate.getCompoundReader(dir, si), dir);
    }

    @Override
    public void write(Directory dir, SegmentInfo si, IOContext context) throws IOException {
        String engineExtension = SparseEngine.NATIVE.extension();
        Set<String> engineFiles = si.files().stream().filter(file -> file.endsWith(engineExtension)).collect(Collectors.toSet());

        Set<String> segmentFiles = new HashSet<>(si.files());

        if (!engineFiles.isEmpty()) {
            for (String engineFile : engineFiles) {
                String compoundFile = engineFile + SparseConstants.COMPOUND_EXTENSION;
                dir.copyFrom(dir, engineFile, compoundFile, context);
            }
            segmentFiles.removeAll(engineFiles);
            si.setFiles(segmentFiles);
        }

        delegate.write(dir, si, context);
    }
}
