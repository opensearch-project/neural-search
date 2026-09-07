/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.sparse.codec.nativeindex;

import lombok.extern.log4j.Log4j2;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.SegmentInfo;
import org.apache.lucene.store.AlreadyClosedException;
import org.opensearch.neuralsearch.jni.NativeLibrary;
import org.opensearch.neuralsearch.sparse.algorithm.SparseEngine;
import org.opensearch.neuralsearch.sparse.codec.CodecUtils;

import java.io.Closeable;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * The native seismic index for one field of one segment, loaded on first query and freed when
 * the segment core closes.
 *
 * {@link NativeLibrary#loadIndex} asks nsparse for mmap residency, so the posting lists and the
 * forward index are borrowed from the segment file and accounted as reclaimable page cache. Only
 * the mapping handle and the per-term metadata sit on the native heap, so there is nothing worth
 * evicting and no byte limit to enforce — how much of the index stays resident is the OS's call.
 * The handle does still have to be freed, and that is all this class tracks.
 *
 * Lifetime is the segment core's, not a query's: a core outlives every search running against it,
 * so an address handed to a query cannot be freed underneath it and no reference counting is
 * needed. Tying it to the core rather than to the reader also means an NRT refresh reuses the
 * mapping instead of building a new one.
 */
@Log4j2
public final class SegmentNativeIndex implements Closeable {

    private static final ConcurrentMap<Key, SegmentNativeIndex> OPEN_INDICES = new ConcurrentHashMap<>();

    /** A segment core can carry several sparse fields, each with its own engine file. */
    private record Key(IndexReader.CacheKey core, String fieldName) {
    }

    private final String path;
    private final boolean coreScoped;
    private long address;
    private boolean freed;

    private SegmentNativeIndex(String path, boolean coreScoped) {
        this.path = path;
        this.coreScoped = coreScoped;
    }

    /**
     * Returns the segment's native index, reusing the one already loaded for this segment core
     * when there is one. The caller must {@link #close()} the result, which frees the handle only
     * in the fallback case where no core was available to own it.
     *
     * @param reader the leaf reader being searched, whose core owns the handle
     * @param segmentInfo the segment holding the engine file
     * @param fieldName the sparse field to load
     */
    public static SegmentNativeIndex open(LeafReader reader, SegmentInfo segmentInfo, String fieldName) throws IOException {
        final IndexReader.CacheHelper coreCache = reader.getCoreCacheHelper();
        if (coreCache == null) {
            // A reader wrapper that exposes no core lifecycle leaves nothing to hang the handle
            // on, so it must not outlive this query.
            return new SegmentNativeIndex(resolveIndexPath(segmentInfo, fieldName), false);
        }
        final Key key = new Key(coreCache.getKey(), fieldName);
        SegmentNativeIndex index = OPEN_INDICES.get(key);
        if (index != null) {
            return index;
        }
        // Resolved only on a miss: scanning segmentInfo.files() per query is pure overhead once
        // the handle is loaded, and the handle is loaded for all but the first query on a core.
        final String path = resolveIndexPath(segmentInfo, fieldName);
        return OPEN_INDICES.computeIfAbsent(key, k -> {
            SegmentNativeIndex created = new SegmentNativeIndex(path, true);
            coreCache.addClosedListener(ignored -> {
                OPEN_INDICES.remove(k);
                created.free();
            });
            return created;
        });
    }

    /**
     * The native index address, loading the index on first call. Valid for as long as the caller's
     * reader is open.
     */
    public synchronized long address() {
        if (freed) {
            throw new AlreadyClosedException("Native index has already been freed: " + path);
        }
        if (address == 0) {
            address = NativeLibrary.loadIndex(path);
            log.debug("Loaded native index with mmap residency: {}", path);
        }
        return address;
    }

    /** Frees the handle unless the segment core owns it, in which case closing the core does. */
    @Override
    public void close() {
        if (coreScoped == false) {
            free();
        }
    }

    private synchronized void free() {
        freed = true;
        if (address != 0) {
            NativeLibrary.freeIndex(address);
            log.debug("Freed native index: {}", path);
            address = 0;
        }
    }

    /**
     * nsparse maps the file itself, so it needs a filesystem path rather than a Lucene
     * {@link org.apache.lucene.store.IndexInput}.
     */
    private static String resolveIndexPath(SegmentInfo segmentInfo, String fieldName) throws IOException {
        List<String> engineFiles = CodecUtils.getEngineFiles(SparseEngine.NATIVE.extension(), fieldName, segmentInfo);
        if (engineFiles.isEmpty()) {
            throw new IOException("No native engine file for field [" + fieldName + "] in segment [" + segmentInfo.name + "]");
        }
        return CodecUtils.resolveFilePath(segmentInfo.dir, engineFiles.get(0));
    }
}
