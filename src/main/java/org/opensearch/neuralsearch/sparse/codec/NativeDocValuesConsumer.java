/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.sparse.codec;

import lombok.AllArgsConstructor;
import lombok.extern.log4j.Log4j2;
import org.apache.lucene.codecs.DocValuesProducer;
import org.apache.lucene.index.BinaryDocValues;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.SegmentWriteState;
import org.opensearch.neuralsearch.sparse.SparseSettings;
import org.opensearch.neuralsearch.sparse.algorithm.SparseEngine;
import org.opensearch.neuralsearch.sparse.codec.nativeindex.CsrFileNativeIndexWriter;
import org.opensearch.neuralsearch.sparse.codec.nativeindex.DefaultNativeIndexWriter;
import org.opensearch.neuralsearch.sparse.common.MergeStateFacade;
import org.opensearch.neuralsearch.sparse.common.SparseFieldUtils;
import org.opensearch.neuralsearch.sparse.mapper.SparseVectorField;

import java.io.IOException;
import java.util.List;

/**
 * Writes the native engine file for every {@link SparseEngine#NATIVE} field in a segment, on both
 * flush and merge, by delegating each field to a {@link CsrFileNativeIndexWriter}, or to a
 * {@link DefaultNativeIndexWriter} where that is not usable. The two produce the same engine file and
 * differ only in where the vectors sit in between, so which one ran is not visible to a reader.
 *
 * Fields on another engine, non-sparse fields, and — because building requires loading the JNI
 * library — every field while the native engine is disabled are skipped. The raw vectors are
 * written by the sibling consumer regardless, so a skipped segment can be rebuilt by force-merging
 * once the engine is back on.
 */
@Log4j2
@AllArgsConstructor
public class NativeDocValuesConsumer extends SparseVectorBinaryConsumer {
    private final SegmentWriteState state;
    private final MergeHelper mergeHelper;

    @Override
    public void addBinaryField(FieldInfo field, DocValuesProducer valuesProducer) throws IOException {
        if (shouldSkip(field)) {
            return;
        }
        writeIndex(field, valuesProducer.getBinary(field));
    }

    @Override
    public void merge(List<FieldInfo> fieldInfos, MergeStateFacade mergeStateFacade) throws IOException {
        for (FieldInfo fieldInfo : fieldInfos) {
            if (shouldSkip(fieldInfo)) {
                continue;
            }
            SparseDocValuesReader sparseDocValuesReader = mergeHelper.newSparseDocValuesReader(mergeStateFacade);
            writeIndex(fieldInfo, sparseDocValuesReader.getBinary(fieldInfo));
        }
    }

    /**
     * Builds one field's index, staging the vectors in a CSR file wherever nsparse can map one.
     *
     * The fallback is not a preference but a capability: a directory with no filesystem behind it
     * gives nsparse nothing to map, and only {@link DefaultNativeIndexWriter} can build there. Both
     * produce the same engine file, so the choice is invisible downstream -- which is also why it is
     * logged, since otherwise nothing on a running node says which path a segment took.
     */
    private void writeIndex(FieldInfo fieldInfo, BinaryDocValues binaryDocValues) throws IOException {
        if (CsrFileNativeIndexWriter.supports(state)) {
            log.debug("Staging field [{}] of segment [{}] as a CSR file", fieldInfo.getName(), state.segmentInfo.name);
            new CsrFileNativeIndexWriter(state, fieldInfo).writeIndex(binaryDocValues);
            return;
        }
        log.debug("Streaming field [{}] of segment [{}] off-heap", fieldInfo.getName(), state.segmentInfo.name);
        new DefaultNativeIndexWriter(state, fieldInfo).writeIndex(binaryDocValues);
    }

    private boolean shouldSkip(FieldInfo field) {
        if (!SparseVectorField.isSparseField(field) || SparseFieldUtils.getSparseEngine(field) != SparseEngine.NATIVE) {
            return true;
        }
        // A merge of pre-existing native segments is the one write that outlives the ingest gate, and
        // it would load the JNI library. Skip it: the raw vectors still reach disk through the
        // delegate consumer, so re-enabling and force-merging rebuilds the native index.
        if (SparseSettings.state().isNativeEngineEnabled() == false) {
            log.warn(
                "Skipping native sparse index for field [{}] in segment [{}]: {}",
                field.getName(),
                state.segmentInfo.name,
                SparseSettings.NATIVE_ENGINE_DISABLED_REASON
            );
            return true;
        }
        return false;
    }
}
