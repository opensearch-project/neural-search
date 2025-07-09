/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.sparse;

import org.apache.lucene.codecs.Codec;
import org.apache.lucene.codecs.DocValuesProducer;
import org.apache.lucene.codecs.FieldsProducer;
import org.apache.lucene.index.BinaryDocValues;
import org.apache.lucene.index.MergeState;
import org.apache.lucene.index.SegmentInfo;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.FieldInfos;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.index.DocValuesType;
import org.apache.lucene.index.DocValuesSkipIndexType;
import org.apache.lucene.index.VectorEncoding;
import org.apache.lucene.index.VectorSimilarityFunction;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.index.SortedDocValues;
import org.apache.lucene.index.DocValuesSkipper;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.SortedSetDocValues;
import org.apache.lucene.index.SortedNumericDocValues;
import org.apache.lucene.index.IndexableFieldType;
import org.apache.lucene.store.ByteBuffersDirectory;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.InfoStream;
import org.apache.lucene.util.Version;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.Executors;

public class testsPrepareUtils {

    private final String fieldName = "test_field";

    public FieldInfo prepareKeyFieldInfo() {

        // Create a FieldInfo object
        FieldInfo keyFieldInfo = new FieldInfo(
            fieldName,                     // name
            0,                             // number
            false,                         // storeTermVector
            false,                         // omitNorms
            false,                         // storePayloads
            IndexOptions.DOCS,             // indexOptions
            DocValuesType.BINARY,          // docValuesType
            DocValuesSkipIndexType.NONE,   // docValuesSkipIndex
            -1,                            // dvGen
            new HashMap<>(),               // attributes
            0,                             // pointDimensionCount
            0,                             // pointIndexDimensionCount
            0,                             // pointNumBytes
            0,                             // vectorDimension
            VectorEncoding.FLOAT32,        // vectorEncoding
            VectorSimilarityFunction.EUCLIDEAN, // vectorSimilarityFunction
            false,                         // softDeletesField
            false                          // isParentField
        );
        return keyFieldInfo;
    }

    public static SegmentInfo prepareSegmentInfo() {
        MergeState.DocMap[] docMaps = new MergeState.DocMap[1];
        docMaps[0] = docID -> docID;
        Directory dir = new ByteBuffersDirectory();
        byte[] id = new byte[16];
        for (int i = 0; i < id.length; i++) {
            id[i] = (byte) i;
        }

        SegmentInfo segmentInfo = new SegmentInfo(
            dir,                           // directory
            Version.LATEST,                // version
            Version.LATEST,                // minVersion
            "_test_segment",               // name
            10,                            // maxDoc
            false,                         // isCompoundFile
            false,                         // hasBlocks
            Codec.getDefault(),            // codec
            Collections.emptyMap(),        // diagnostics
            id,                            // id
            Collections.emptyMap(),        // attributes
            null                           // indexSort
        );
        return segmentInfo;
    }

    public static BinaryDocValues prepareBinaryDocValues() {
        final BytesRef value = new BytesRef(new byte[] { 1, 2, 3, 4 });
        BinaryDocValues binaryDocValues = new BinaryDocValues() {
            private int docID = -1;

            @Override
            public int docID() {
                return docID;
            }

            @Override
            public int nextDoc() {
                if (docID < 9) {
                    docID++;
                    return docID;
                }
                return NO_MORE_DOCS;
            }

            @Override
            public int advance(int target) {
                if (docID < target && target <= 9) {
                    docID = target;
                    return docID;
                }
                return NO_MORE_DOCS;
            }

            @Override
            public long cost() {
                return 10;
            }

            @Override
            public BytesRef binaryValue() {
                return value;
            }

            @Override
            public boolean advanceExact(int target) throws IOException {
                if (target <= 9) {
                    docID = target;
                    return true;
                }
                return false;
            }
        };
        return binaryDocValues;
    }

    public DocValuesProducer prepareDocValuesProducer(BinaryDocValues binaryDocValues) {
        DocValuesProducer docValuesProducer = new DocValuesProducer() {
            @Override
            public NumericDocValues getNumeric(FieldInfo field) {
                return null;
            }

            @Override
            public BinaryDocValues getBinary(FieldInfo field) {
                if (field.name.equals(fieldName)) {
                    return binaryDocValues;
                }
                return null;
            }

            @Override
            public SortedDocValues getSorted(FieldInfo field) {
                return null;
            }

            @Override
            public SortedNumericDocValues getSortedNumeric(FieldInfo field) {
                return null;
            }

            @Override
            public SortedSetDocValues getSortedSet(FieldInfo field) {
                return null;
            }

            @Override
            public void checkIntegrity() {}

            @Override
            public void close() {}

            @Override
            public DocValuesSkipper getSkipper(FieldInfo field) throws IOException {
                return null;
            }
        };
        return docValuesProducer;
    }

    public FieldsProducer prepareFieldsProducer() {
        FieldsProducer fieldsProducer = new FieldsProducer() {
            @Override
            public Iterator<String> iterator() {
                return Collections.singleton(fieldName).iterator();
            }

            @Override
            public Terms terms(String field) {
                return null;
            }

            @Override
            public int size() {
                return 1;
            }

            @Override
            public void checkIntegrity() {}

            @Override
            public void close() {}
        };
        return fieldsProducer;
    }

    public MergeState prepareMergeState(boolean isEmptyMaxDocs) {
        MergeState.DocMap[] docMaps = new MergeState.DocMap[1];
        docMaps[0] = docID -> docID;
        SegmentInfo segmentInfo = prepareSegmentInfo();

        int[] maxDocs = new int[] { 10 };
        if (isEmptyMaxDocs) {
            maxDocs = new int[] { 0 };
        }

        // Create a FieldInfo object
        FieldInfo keyFieldInfo = prepareKeyFieldInfo();

        // Create a real BinaryDocValues object
        BinaryDocValues binaryDocValues = prepareBinaryDocValues();

        // Create a DocValuesProducer
        DocValuesProducer docValuesProducer = prepareDocValuesProducer(binaryDocValues);

        DocValuesProducer[] docValuesProducers = new DocValuesProducer[1];
        docValuesProducers[0] = docValuesProducer;

        // Create FieldInfos, like an array of FieldInfo
        FieldInfos fieldInfos = new FieldInfos(new FieldInfo[] { keyFieldInfo });
        FieldInfos[] fieldInfosArray = new FieldInfos[1];
        fieldInfosArray[0] = fieldInfos;

        // Create FieldsProducer
        FieldsProducer fieldsProducer = prepareFieldsProducer();

        FieldsProducer[] fieldsProducers = new FieldsProducer[1];
        fieldsProducers[0] = fieldsProducer;

        // Create MergeState
        MergeState mergeState = new MergeState(
            docMaps,
            segmentInfo,
            fieldInfos,                // mergeFieldInfos
            null,                      // storedFieldsReaders
            null,                      // termVectorsReaders
            null,                      // normsProducers
            docValuesProducers,        // docValuesProducers
            fieldInfosArray,           // fieldInfos
            null,                      // liveDocs
            fieldsProducers,           // fieldsProducers
            null,                      // pointsReaders
            null,                      // knnVectorsReaders
            maxDocs,
            InfoStream.getDefault(),
            Executors.newSingleThreadExecutor(),
            false                      // needsIndexSort
        );
        return mergeState;
    }

    public static IndexableFieldType prepareMockIndexableFieldType() {
        return new IndexableFieldType() {
            @Override
            public boolean stored() {
                return false;
            }

            @Override
            public boolean tokenized() {
                return false;
            }

            @Override
            public boolean storeTermVectors() {
                return false;
            }

            @Override
            public boolean storeTermVectorOffsets() {
                return false;
            }

            @Override
            public boolean storeTermVectorPositions() {
                return false;
            }

            @Override
            public boolean storeTermVectorPayloads() {
                return false;
            }

            @Override
            public boolean omitNorms() {
                return false;
            }

            @Override
            public IndexOptions indexOptions() {
                return IndexOptions.DOCS_AND_FREQS;
            }

            @Override
            public DocValuesType docValuesType() {
                return DocValuesType.NUMERIC;
            }

            @Override
            public DocValuesSkipIndexType docValuesSkipIndexType() {
                return DocValuesSkipIndexType.NONE;
            }

            @Override
            public Map<String, String> getAttributes() {
                return new HashMap<>();
            }

            @Override
            public int pointDimensionCount() {
                return 0;
            }

            @Override
            public int pointIndexDimensionCount() {
                return 0;
            }

            @Override
            public int pointNumBytes() {
                return 0;
            }

            @Override
            public int vectorDimension() {
                return 0;
            }

            @Override
            public VectorEncoding vectorEncoding() {
                return VectorEncoding.FLOAT32;
            }

            @Override
            public VectorSimilarityFunction vectorSimilarityFunction() {
                return VectorSimilarityFunction.EUCLIDEAN;
            }
        };
    }
}
