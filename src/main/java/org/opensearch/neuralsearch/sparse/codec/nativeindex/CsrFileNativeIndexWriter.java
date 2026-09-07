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
import org.opensearch.neuralsearch.jni.NativeLibrary;
import org.opensearch.neuralsearch.sparse.SparseSettings;
import org.opensearch.neuralsearch.sparse.algorithm.SparseEngine;
import org.opensearch.neuralsearch.sparse.codec.CodecUtils;
import org.opensearch.neuralsearch.sparse.common.BinaryVectorUtils;
import org.opensearch.neuralsearch.sparse.io.IndexOutputWrapper;
import org.opensearch.neuralsearch.sparse.quantization.ByteQuantizationUtil;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Builds the native engine file for one sparse field of one segment, staging the vectors in files
 * that nsparse maps instead of streaming them off-heap over JNI.
 *
 * Same contract and same output as {@link DefaultNativeIndexWriter}: one instance per field, both
 * flush and merge go through {@link #writeIndex}, and the built index is serialized into the
 * segment's {@link SparseEngine#NATIVE} file. What differs is where the vectors live between the doc
 * values and the built index. The default writer transfers them off-heap, so the whole segment's CSR
 * arrays are resident anonymous memory at build time; here they go to a {@link CsrSparseVectorsFile}
 * and are borrowed from the mapping, so they are reclaimable page cache instead.
 *
 * Both index families are staged, at the value width each one borrows at: 8-bit codes for the
 * quantized seismic layouts a field over the threshold gets, float32 for the unquantized
 * {@code inverted} index a sub-threshold one gets. {@link #supports} then rules out only a directory
 * with no filesystem behind it, since nsparse maps the files itself.
 */
@Log4j2
@AllArgsConstructor
public class CsrFileNativeIndexWriter {
    private final SegmentWriteState state;
    private final FieldInfo fieldInfo;

    /**
     * Whether this writer can build into the segment's directory, as opposed to
     * {@link DefaultNativeIndexWriter}.
     *
     * Takes no field: both index families are staged, each at its own value width, so what remains is
     * a property of the directory alone.
     *
     * @param state the segment being written
     * @return true if nsparse can map what this writer would stage
     */
    public static boolean supports(SegmentWriteState state) {
        try {
            CodecUtils.resolveDirectoryPath(state.directory);
            return true;
        } catch (IOException e) {
            // Not filesystem-backed, so nsparse cannot map anything the writer stages. Not an error:
            // the caller falls back to streaming the vectors over JNI.
            log.debug("CSR staging unavailable for segment [{}]: {}", state.segmentInfo.name, e.getMessage());
            return false;
        }
    }

    /**
     * Builds the index over every document the iterator yields and writes it to the segment.
     *
     * A segment in which no document has the field still gets a footer-only file, so the name is
     * always openable. The staged files and the native index are both freed on every path out.
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
        try (
            IndexOutput output = state.directory.createOutput(engineFileName, state.context);
            // Closed after the index has been serialized, not after it has been built: under mmap
            // residency the index borrows the values out of the CSR file rather than copying them.
            CsrSparseVectorsFile csrFile = newCsrFile()
        ) {
            StreamedVectorsMetadata result = new StreamedVectorsMetadata();
            writeToCsrFile(binaryDocValues, result, csrFile);
            if (result.getDocIds().isEmpty()) {
                // No document in this segment has the field, so there is no index to
                // build. Still write the footer so the file stays a valid Lucene file.
                CodecUtil.writeFooter(output);
                return;
            }
            int dimension = result.getDimension();
            csrFile.finish(dimension, Ints.toArray(result.getDocIds()));
            long indexAddress = NativeLibrary.initIndex(totalDoc, dimension, NativeIndexParameters.build(state, fieldInfo));
            // writeIndex takes ownership of indexAddress and frees it. Until it is
            // reached, nothing else will: an exception from readCsrAndIdsToIndex would leak
            // the whole segment's native index, and merges retry on every attempt.
            boolean ownershipTransferred = false;
            try {
                NativeLibrary.readCsrAndIdsToIndex(indexAddress, csrFile.resolveCsrPath(), csrFile.resolveIdPath(), threadCount);
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
            log.error("Fails to write native index from a CSR file", e);
            throw e;
        }
    }

    /**
     * A staging file at the width the index {@link NativeIndexParameters} is about to build borrows at.
     *
     * The quantized case stages with the ingest quantizer, whose ceiling those parameters pass as
     * {@code vmax}: nothing re-encodes what lands in the file, so the two have to be the same
     * quantizer or the scores decode to a different scale.
     */
    private CsrSparseVectorsFile newCsrFile() throws IOException {
        if (NativeIndexParameters.isQuantized(state, fieldInfo)) {
            return CsrSparseVectorsFile.forQuantizedIndex(
                state.directory,
                state.context,
                state.segmentInfo.name,
                ByteQuantizationUtil.getByteQuantizerIngest(fieldInfo)
            );
        }
        return CsrSparseVectorsFile.forUnquantizedIndex(state.directory, state.context, state.segmentInfo.name);
    }

    private void writeToCsrFile(BinaryDocValues binaryDocValues, StreamedVectorsMetadata result, CsrSparseVectorsFile csrFile)
        throws IOException {
        int docId = binaryDocValues.nextDoc();
        while (docId != DocIdSetIterator.NO_MORE_DOCS) {
            List<Integer> tokens = new ArrayList<>();
            List<Float> weights = new ArrayList<>();
            BinaryVectorUtils.readToList(binaryDocValues.binaryValue(), tokens, weights);
            result.updateMaxTokenId(tokens);
            result.getDocIds().add(docId);
            csrFile.addVector(tokens, weights);
            docId = binaryDocValues.nextDoc();
        }
    }
}
