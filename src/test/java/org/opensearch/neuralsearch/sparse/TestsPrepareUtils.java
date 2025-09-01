/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.sparse;

import org.apache.lucene.codecs.Codec;
import org.apache.lucene.codecs.DocValuesProducer;
import org.apache.lucene.codecs.FieldsProducer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.BinaryDocValues;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.DocValuesSkipIndexType;
import org.apache.lucene.index.DocValuesSkipper;
import org.apache.lucene.index.DocValuesType;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.FieldInfos;
import org.apache.lucene.index.FilterDirectoryReader;
import org.apache.lucene.index.FilterLeafReader;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.IndexableFieldType;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.MergeState;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.index.SegmentCommitInfo;
import org.apache.lucene.index.SegmentInfo;
import org.apache.lucene.index.SegmentWriteState;
import org.apache.lucene.index.SortedDocValues;
import org.apache.lucene.index.SortedNumericDocValues;
import org.apache.lucene.index.SortedSetDocValues;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.VectorEncoding;
import org.apache.lucene.index.VectorSimilarityFunction;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.store.ByteBuffersDirectory;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.tests.analysis.MockAnalyzer;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.InfoStream;
import org.apache.lucene.util.StringHelper;
import org.apache.lucene.util.Version;
import org.opensearch.common.settings.Settings;
import org.opensearch.index.mapper.ContentPath;
import org.opensearch.neuralsearch.sparse.codec.SparseBinaryDocValuesPassThrough;
import org.opensearch.neuralsearch.sparse.common.SparseConstants;
import org.opensearch.neuralsearch.sparse.mapper.SparseTokensField;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.Executors;

import static org.apache.lucene.tests.util.LuceneTestCase.random;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class TestsPrepareUtils {

    private final static String fieldName = "test_field";

    public static FieldInfo prepareKeyFieldInfo() {

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

    public static SegmentInfo prepareSegmentInfo(int maxDoc) {
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
            maxDoc,                        // maxDoc
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

    public static BinaryDocValues prepareBinaryDocValues() throws IOException {
        BinaryDocValues mockBinaryDocValues = mock(BinaryDocValues.class);

        when(mockBinaryDocValues.docID()).thenReturn(-1);

        when(mockBinaryDocValues.nextDoc()).thenReturn(0)
            .thenReturn(1)
            .thenReturn(2)
            .thenReturn(3)
            .thenReturn(4)
            .thenReturn(5)
            .thenReturn(6)
            .thenReturn(7)
            .thenReturn(8)
            .thenReturn(9)
            .thenReturn(DocIdSetIterator.NO_MORE_DOCS);
        when(mockBinaryDocValues.cost()).thenReturn(10L);

        when(mockBinaryDocValues.binaryValue()).thenReturn(new BytesRef(new byte[] { 1, 2, 3, 4 }));
        return mockBinaryDocValues;
    }

    public static DocValuesProducer prepareDocValuesProducer(BinaryDocValues binaryDocValues) {
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

    public static FieldsProducer prepareFieldsProducer() {
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

    public static MergeState prepareMergeState(boolean isEmptyMaxDocs) throws IOException {
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

    public static IndexableFieldType prepareIndexableFieldType() {
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

    public static Settings prepareIndexSettings() {
        return Settings.builder().put("index.number_of_shards", 1).put("index.number_of_replicas", 0).build();
    }

    public static ContentPath prepareContentPath() {
        return new ContentPath();
    }

    public static SegmentCommitInfo prepareSegmentCommitInfo() {
        SegmentInfo segmentInfo = prepareSegmentInfo();
        byte[] id = StringHelper.randomId();
        return new SegmentCommitInfo(
            segmentInfo,
            0,      // delCount
            0,      // softDelCount
            -1,     // delGen
            -1,     // fieldInfosGen
            -1,     // docValuesGen
            id
        );
    }

    public static IndexReader prepareTestIndexReader() throws IOException {
        ByteBuffersDirectory directory = new ByteBuffersDirectory();
        IndexWriterConfig config = new IndexWriterConfig(new MockAnalyzer(random()));
        IndexWriter writer = new IndexWriter(directory, config);

        // Create document with test field
        Document doc = new Document();
        doc.add(new StringField(fieldName, "test_value", Field.Store.NO));

        writer.addDocument(doc);
        writer.close();
        return DirectoryReader.open(directory);
    }

    public static SegmentWriteState prepareSegmentWriteState() {
        Directory directory = new ByteBuffersDirectory();
        SegmentInfo segmentInfo = prepareSegmentInfo();
        FieldInfos fieldInfos = new FieldInfos(new FieldInfo[] { prepareKeyFieldInfo() });
        IOContext ioContext = IOContext.DEFAULT;

        return new SegmentWriteState(InfoStream.getDefault(), directory, segmentInfo, fieldInfos, null, ioContext);
    }

    public static SegmentWriteState prepareSegmentWriteState(SegmentInfo segmentInfo) {
        Directory directory = new ByteBuffersDirectory();
        FieldInfos fieldInfos = new FieldInfos(new FieldInfo[] { prepareKeyFieldInfo() });
        IOContext ioContext = IOContext.DEFAULT;

        return new SegmentWriteState(InfoStream.getDefault(), directory, segmentInfo, fieldInfos, null, ioContext);
    }

    public static SegmentWriteState prepareSegmentWriteState(Directory directory, FieldInfos fieldInfos) {
        SegmentInfo segmentInfo = prepareSegmentInfo();
        IOContext ioContext = IOContext.DEFAULT;

        return new SegmentWriteState(InfoStream.getDefault(), directory, segmentInfo, fieldInfos, null, ioContext);
    }

    public static BytesRef prepareValidSparseVectorBytes() {
        // Create a valid sparse vector BytesRef with token "1" -> 0.5f
        try {
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            DataOutputStream dos = new DataOutputStream(baos);

            // Write one token-value pair: "1" -> 0.5f
            dos.writeInt(1);
            dos.writeFloat(0.5f);
            dos.close();
            return new BytesRef(baos.toByteArray());
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public static DirectoryReader prepareIndexReaderWithSparseField(int docNumbers) throws IOException {
        Directory directory = new ByteBuffersDirectory();
        IndexWriterConfig config = new IndexWriterConfig(new MockAnalyzer(random()));
        IndexWriter writer = new IndexWriter(directory, config);

        // Create custom field type for sparse field
        FieldType sparseFieldType = new FieldType();
        sparseFieldType.setStored(false);
        sparseFieldType.setTokenized(false);
        sparseFieldType.setIndexOptions(IndexOptions.DOCS);
        sparseFieldType.setDocValuesType(DocValuesType.BINARY);

        // Add required attributes for sparse field
        sparseFieldType.putAttribute(SparseTokensField.SPARSE_FIELD, "true");
        sparseFieldType.putAttribute(SparseConstants.APPROXIMATE_THRESHOLD_FIELD, "10");
        sparseFieldType.freeze();

        // Create documents with sparse field
        for (int i = 0; i < docNumbers; i++) {
            Document doc = new Document();
            BytesRef sparseValue = TestsPrepareUtils.prepareValidSparseVectorBytes();
            Field sparseField = new Field("sparse_field", sparseValue, sparseFieldType);
            doc.add(sparseField);
            writer.addDocument(doc);
        }

        writer.close();
        DirectoryReader baseReader = DirectoryReader.open(directory);
        return new TestDirectoryReaderWrapper(baseReader);
    }

    /**
     * Wrapper to ensure sparse field BinaryDocValues are wrapped with SparseBinaryDocValuesPassThrough.
     * This mimics the behavior of the neural-search codec in production.
     */
    private static class TestDirectoryReaderWrapper extends FilterDirectoryReader {

        public TestDirectoryReaderWrapper(DirectoryReader in) throws IOException {
            super(in, new SubReaderWrapper() {
                @Override
                public LeafReader wrap(LeafReader reader) {
                    return new FilterLeafReader(reader) {
                        @Override
                        public BinaryDocValues getBinaryDocValues(String field) throws IOException {
                            BinaryDocValues original = super.getBinaryDocValues(field);
                            if (original != null && field.equals("sparse_field")) {
                                // Wrap with SparseBinaryDocValuesPassThrough for sparse fields
                                SegmentInfo segmentInfo = prepareSegmentInfo();
                                return new SparseBinaryDocValuesPassThrough(original, segmentInfo);
                            }
                            return original;
                        }

                        @Override
                        public CacheHelper getCoreCacheHelper() {
                            return in.getCoreCacheHelper();
                        }

                        @Override
                        public CacheHelper getReaderCacheHelper() {
                            return in.getReaderCacheHelper();
                        }
                    };
                }
            });
        }

        @Override
        protected DirectoryReader doWrapDirectoryReader(DirectoryReader in) throws IOException {
            return new TestDirectoryReaderWrapper(in);
        }

        @Override
        public CacheHelper getReaderCacheHelper() {
            return in.getReaderCacheHelper();
        }
    }
}
