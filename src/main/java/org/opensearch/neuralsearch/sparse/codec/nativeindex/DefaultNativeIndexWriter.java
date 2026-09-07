/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.sparse.codec.nativeindex;

import com.google.common.primitives.Ints;
import lombok.AllArgsConstructor;
import lombok.extern.log4j.Log4j2;
import org.apache.lucene.codecs.CodecUtil;
import org.apache.lucene.index.BinaryDocValues;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.SegmentWriteState;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.store.IndexOutput;
import org.opensearch.monitor.jvm.JvmInfo;
import org.opensearch.neuralsearch.jni.NativeLibrary;
import org.opensearch.neuralsearch.sparse.SparseSettings;
import org.opensearch.neuralsearch.sparse.algorithm.SparseEngine;
import org.opensearch.neuralsearch.sparse.codec.CodecUtils;
import org.opensearch.neuralsearch.sparse.common.BinaryVectorUtils;
import org.opensearch.neuralsearch.sparse.io.IndexOutputWrapper;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Builds the native engine file for one sparse field of one segment.
 *
 * The doc values are streamed into off-heap CSR memory, handed to nsparse in a single
 * {@code insertToIndex} call, and the built index is serialized into the segment's
 * {@link SparseEngine#NATIVE} file. Which nsparse index type gets built is derived from the field
 * mapping by {@link NativeIndexParameters}.
 *
 * One instance writes one field; both flush and merge go through {@link #writeIndex}. See
 * {@link CsrFileNativeIndexWriter} for the variant that stages the vectors in a file instead of in
 * off-heap memory.
 */
@Log4j2
@AllArgsConstructor
public class DefaultNativeIndexWriter {

    /**
     * How much this writer batches on-heap before transferring a chunk off-heap.
     *
     * 1% of the JVM heap, which is what the removed
     * {@code plugins.neural_search.sparse.vector_streaming_memory.limit} setting defaulted to -- so
     * behaviour is unchanged, it is simply no longer tunable. The batch size only sets how often the
     * transfer happens, not how much off-heap memory the segment ends up holding, so there was little
     * for an operator to gain by moving it.
     */
    private static final long STREAMING_MEMORY_LIMIT_BYTES = JvmInfo.jvmInfo().getMem().getHeapMax().getBytes() / 100;

    private final SegmentWriteState state;
    private final FieldInfo fieldInfo;

    /**
     * Builds the index over every document the iterator yields and writes it to the segment.
     *
     * A segment in which no document has the field still gets a footer-only file, so the name is
     * always openable. The native index is freed on every path out.
     *
     * @param binaryDocValues the field's sparse vectors, from a flush or a merge
     * @throws IOException if the engine file cannot be written
     */
    public void writeIndex(BinaryDocValues binaryDocValues) throws IOException {
        int threadCount = SparseSettings.state().getSettingValue(SparseSettings.SPARSE_ALGO_PARAM_INDEX_THREAD_QTY);
        final String engineFileName = CodecUtils.buildIndexFileName(
            state.segmentInfo.name,
            SparseEngine.NATIVE.version(),
            fieldInfo.name,
            SparseEngine.NATIVE.extension()
        );
        int totalDoc = state.segmentInfo.maxDoc();
        long bytesLimit = STREAMING_MEMORY_LIMIT_BYTES;
        // The buffer is a resource because streaming transfers vectors off-heap as it goes: if the
        // doc values throw partway, closing it is the only thing that can still free them.
        try (
            IndexOutput output = state.directory.createOutput(engineFileName, state.context);
            OffHeapSparseVectorsBuffer buffer = new OffHeapSparseVectorsBuffer(bytesLimit)
        ) {
            StreamedVectorsMetadata result = new StreamedVectorsMetadata();
            writeToBuffer(binaryDocValues, result, buffer);
            if (result.getDocIds().isEmpty()) {
                // No document in this segment has the field, so there is no index to
                // build. Still write the footer so the file stays a valid Lucene file.
                CodecUtil.writeFooter(output);
                return;
            }
            int dimension = result.getDimension();
            long indexAddress = NativeLibrary.initIndex(totalDoc, dimension, NativeIndexParameters.build(state, fieldInfo));
            // writeIndex takes ownership of indexAddress and frees it. Until it is
            // reached, nothing else will: an exception from insertToIndex would leak
            // the whole segment's native index, and merges retry on every attempt.
            boolean ownershipTransferred = false;
            try {
                // Hands the vectors over too, so the close() above this frees only what a failure
                // before this point left behind.
                buffer.insertInto(indexAddress, Ints.toArray(result.getDocIds()), threadCount);
                IndexOutputWrapper indexOutputWrapper = new IndexOutputWrapper(output);
                ownershipTransferred = true;
                NativeLibrary.writeIndex(indexAddress, indexOutputWrapper);
            } finally {
                if (!ownershipTransferred) {
                    NativeLibrary.freeIndex(indexAddress);
                }
            }
            CodecUtil.writeFooter(output);
        } catch (Exception e) {
            log.error("Fails to write native index", e);
            throw e;
        }
    }

    private void writeToBuffer(BinaryDocValues binaryDocValues, StreamedVectorsMetadata result, OffHeapSparseVectorsBuffer buffer)
        throws IOException {
        int docId = binaryDocValues.nextDoc();
        while (docId != DocIdSetIterator.NO_MORE_DOCS) {
            List<Integer> tokens = new ArrayList<>();
            List<Float> weights = new ArrayList<>();
            BinaryVectorUtils.readToList(binaryDocValues.binaryValue(), tokens, weights);
            result.updateMaxTokenId(tokens);
            result.getDocIds().add(docId);
            buffer.addVector(tokens, weights);
            docId = binaryDocValues.nextDoc();
        }
        buffer.flush();
    }
}
