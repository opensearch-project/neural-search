/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.sparse.codec.nativeindex;

import lombok.extern.log4j.Log4j2;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.SegmentWriteState;
import org.opensearch.neuralsearch.sparse.algorithm.SparseForwardIndex;
import org.opensearch.neuralsearch.sparse.codec.CodecUtils;
import org.opensearch.neuralsearch.sparse.common.PredicateUtils;
import org.opensearch.neuralsearch.sparse.common.SparseFieldUtils;
import org.opensearch.neuralsearch.sparse.quantization.ByteQuantizationUtil;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * Translates the field mapping into the nsparse {@code index_factory} parameter map, for whichever
 * writer is building the segment.
 */
@Log4j2
final class NativeIndexParameters {

    private NativeIndexParameters() {}

    /**
     * A field that has not reached the seismic threshold gets a plain inverted index, so a small
     * segment is not clustered; past it, the layout follows the field's {@code forward_index}.
     */
    static Map<String, Object> build(SegmentWriteState state, FieldInfo fieldInfo) {
        Map<String, Object> parameters = new HashMap<>();
        parameters.put("idmap", true);
        if (PredicateUtils.shouldRunSeisPredicate.test(state.segmentInfo, fieldInfo)) {
            float clusterRatio = SparseFieldUtils.getClusterRatio(fieldInfo);
            int maxDoc = state.segmentInfo.maxDoc();
            int nPostings = SparseFieldUtils.getNPostings(fieldInfo, maxDoc);
            float summaryPruneRatio = SparseFieldUtils.getSummaryPruneRatio(fieldInfo);
            // Both layouts quantize to 8-bit codes over the same range, so a given weight is
            // clamped and rounded identically whichever one the field selects -- and identically
            // to the JVM path. 8-bit codes cut the engine file from 13.9 to 8.0 GiB on base_full,
            // and both are mmap-able, so a large segment's posting lists land in reclaimable page
            // cache rather than on the heap.
            //
            // They differ in where the forward vectors live: seismic_sq keeps one contiguous
            // forward index for the whole field, disk_seismic_sq stores each block's vectors
            // inline next to the block so a query reads only the blocks it selects.
            if (SparseFieldUtils.getSparseForwardIndex(fieldInfo) == SparseForwardIndex.PER_BLOCK) {
                parameters.put("index", "disk_seismic_sq");
            } else {
                parameters.put("index", "seismic_sq");
            }
            parameters.put("quantizer", "8bit");
            parameters.put("vmin", 0.0f);
            parameters.put("vmax", ByteQuantizationUtil.getCeilingValueIngest(fieldInfo));
            parameters.put("lambda", nPostings);
            parameters.put("beta", clusterRatio * nPostings);
            parameters.put("alpha", summaryPruneRatio);
            // handle batch index building
            int batchSize = SparseFieldUtils.getClusteringBatchSize(fieldInfo);
            // for batch_size=1, we simply don't set parameters
            if (batchSize > 1) {
                // Both knobs or neither: nsparse resolves a window count with nowhere to spill to a
                // single window, since windowing alone leaves the bulkier intermediate -- the
                // clustered lists -- whole-corpus while costing a corpus pass per window.
                String spillDirectory = resolveSpillDirectory(state);
                if (spillDirectory != null) {
                    parameters.put("inverted_list_batch_size", batchSize);
                    parameters.put("batch_file_output_path", spillDirectory);
                }
            }
        } else {
            parameters.put("index", "inverted");
        }
        return parameters;
    }

    /**
     * Where a batched build may spill each window of clustered lists, or null if it has nowhere to.
     *
     * The segment's own directory: the spill is scratch that the build unlinks as soon as it has
     * mapped it, and its name ({@code nsparse-clustered-lists-*.tmp}) is not one Lucene's file
     * deleter recognizes, so it needs no separate configured path. A directory with no filesystem
     * behind it -- a test's in-memory one -- has no path to hand nsparse, which is not an error: the
     * segment is simply built in one window, as it was before the parameter existed.
     */
    private static String resolveSpillDirectory(SegmentWriteState state) {
        String directory;
        try {
            directory = CodecUtils.resolveDirectoryPath(state.directory).toString();
        } catch (IOException e) {
            log.debug("Building segment [{}] in one window: {}", state.segmentInfo.name, e.getMessage());
            return null;
        }
        // index_factory splits its description on these, so a path holding one would not arrive
        // whole -- and would take the parameters after it with it, leaving the index built to
        // defaults rather than to the mapping. Give up the batching instead.
        if (directory.chars().anyMatch(c -> c == ',' || c == '|' || c == '=')) {
            log.warn("Building segment [{}] in one window: cannot pass path [{}] to the native engine", state.segmentInfo.name, directory);
            return null;
        }
        return directory;
    }

    /**
     * Whether the index {@link #build} selects reads 8-bit codes rather than floats.
     *
     * The inverted index a sub-threshold segment gets is unquantized, which is what lets it
     * reproduce the Lucene rank_features ranking exactly. Only the quantized layouts can be fed
     * pre-quantized vectors, so this also decides whether {@link CsrFileNativeIndexWriter} is
     * usable at all.
     */
    static boolean isQuantized(SegmentWriteState state, FieldInfo fieldInfo) {
        return PredicateUtils.shouldRunSeisPredicate.test(state.segmentInfo, fieldInfo);
    }
}
