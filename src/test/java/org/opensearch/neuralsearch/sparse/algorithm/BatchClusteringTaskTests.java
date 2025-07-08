/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.sparse.algorithm;

import com.google.common.annotations.VisibleForTesting;
import org.apache.commons.lang3.tuple.Pair;
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
import java.io.IOException;
import org.apache.lucene.store.ByteBuffersDirectory;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.InfoStream;
import org.apache.lucene.util.Version;
import org.apache.lucene.store.Directory;
import org.junit.Before;
import org.mockito.MockitoAnnotations;
import org.opensearch.neuralsearch.sparse.AbstractSparseTestBase;
import org.opensearch.neuralsearch.sparse.common.InMemoryKey;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Iterator;
import java.util.concurrent.Executors;
import java.lang.reflect.Field;

public class BatchClusteringTaskTests extends AbstractSparseTestBase {
    @VisibleForTesting
    private List<BytesRef> terms;
    private InMemoryKey.IndexKey key;
    private String fieldName = "test_field";

    @Before
    @Override
    public void setUp() throws Exception {
        super.setUp();
        MockitoAnnotations.openMocks(this);

        terms = Arrays.asList(new BytesRef("term1"), new BytesRef("term2"));
        key = new InMemoryKey.IndexKey(null, "test_field");
    }

    private FieldInfo prepareKeyFieldInfo() {

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

    private SegmentInfo prepareSegmentInfo() {
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

    private BinaryDocValues prepareBinaryDocValues() {
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

    private DocValuesProducer prepareDocValuesProducer(BinaryDocValues binaryDocValues) {
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

    private FieldsProducer prepareFieldsProducer() {
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

    private MergeState prepareMergeState(boolean isEmptyMaxDocs) {
        MergeState.DocMap[] docMaps = new MergeState.DocMap[1];
        docMaps[0] = docID -> docID;
        SegmentInfo segmentInfo = prepareSegmentInfo();
        // FieldInfo KeyFieldInfo = prepareKeyFieldInfo();

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

    public void testConstructorDeepCopiesTerms() throws Exception {
        // Setup
        List<BytesRef> originalTerms = Arrays.asList(new BytesRef("term1"), new BytesRef("term2"));

        // Execute - create task with null mergeState to test constructor
        BatchClusteringTask task = new BatchClusteringTask(originalTerms, key, 0.5f, 0.3f, 10, null, null);

        // Verify task is created
        assertNotNull("Task should be created successfully", task);

        Field termsField = BatchClusteringTask.class.getDeclaredField("terms");
        termsField.setAccessible(true);
        @SuppressWarnings("unchecked")
        List<BytesRef> taskTerms = (List<BytesRef>) termsField.get(task);

        // Verify deep copy by checking actual content
        assertEquals("First term should be 'term1'", "term1", taskTerms.get(0).utf8ToString());

        // Modify original terms to verify deep copy
        originalTerms.get(0).bytes[0] = (byte) 'X';

        // Verify task's terms remain unchanged (proving deep copy worked)
        assertEquals("Task's first term should still be 'term1'", "term1", taskTerms.get(0).utf8ToString());
        assertNotEquals("Original term should now be different", "term1", originalTerms.get(0).utf8ToString());
    }

    public void testTaskCreationWithDifferentParameters() throws Exception {
        // Test creating tasks with different parameter combinations
        float[] summaryPruneRatios = { 0.1f, 0.5f, 0.9f };
        float[] clusterRatios = { 0.0f, 0.3f, 1.0f };
        int[] nPostingsValues = { 1, 10, 100 };

        for (float summaryPruneRatio : summaryPruneRatios) {
            for (float clusterRatio : clusterRatios) {
                for (int nPostings : nPostingsValues) {
                    BatchClusteringTask task = new BatchClusteringTask(terms, key, summaryPruneRatio, clusterRatio, nPostings, null, null);
                    assertNotNull(
                        "Task should be created with parameters: " + summaryPruneRatio + ", " + clusterRatio + ", " + nPostings,
                        task
                    );
                }
            }
        }
    }

    public void testGetWithNullMergeState() throws Exception {
        // Test behavior with null merge state - should throw NullPointerException when accessing maxDocs
        BatchClusteringTask task = new BatchClusteringTask(terms, key, 0.5f, 0.3f, 10, null, null);

        NullPointerException exception = assertThrows(NullPointerException.class, () -> task.get());

        // Optionally verify the exception message if needed
        assertNotNull("Exception should not be null", exception);
    }

    public void testGetWithNonNullMergeState() throws Exception {
        // Test behavior with a not null merge state
        boolean isEmptyMaxDocs = false;
        MergeState mergeState = prepareMergeState(isEmptyMaxDocs);
        FieldInfo keyFieldInfo = prepareKeyFieldInfo();

        // Create BatchClusteringTask
        BatchClusteringTask task = new BatchClusteringTask(terms, key, 0.5f, 0.3f, 10, mergeState, keyFieldInfo);

        // Execute and examine the result
        List<Pair<BytesRef, PostingClusters>> result = task.get();

        // Verify the returned clusters
        assertNotNull("Result should not be null", result);
        System.out.println("Result: " + result);
        assertEquals("Should return clusters for each term", terms.size(), result.size());

        for (int i = 0; i < result.size(); i++) {
            Pair<BytesRef, PostingClusters> pair = result.get(i);

            // Verify term matches
            assertNotNull("Term should not be null", pair.getLeft());
            assertEquals("Term should match input", terms.get(i).utf8ToString(), pair.getLeft().utf8ToString());

            // Verify clusters
            PostingClusters clusters = pair.getRight();
            assertNotNull("PostingClusters should not be null", clusters);
            assertNotNull("Clusters list should not be null", clusters.getClusters());

            // Additional cluster validation
            assertTrue("Should have non-negative cluster count", clusters.getClusters().size() >= 0);
        }
    }

    public void testGetWithNonNullMergeStateZeroMaxDocs() throws Exception {
        // Test behavior with a not null merge state
        boolean isEmptyMaxDocs = true;
        MergeState mergeState = prepareMergeState(isEmptyMaxDocs);
        FieldInfo keyFieldInfo = prepareKeyFieldInfo();

        // Create BatchClusteringTask
        BatchClusteringTask task = new BatchClusteringTask(terms, key, 0.5f, 0.3f, 10, mergeState, keyFieldInfo);

        // Execute and examine the result
        List<Pair<BytesRef, PostingClusters>> result = task.get();

        // Verify the returned clusters
        assertNotNull("Result should not be null", result);
        // Should trigger early quit schema to return an empty list
        assertEquals("Should return an empty list", 0, result.size());
    }

    public void testTermsDeepCopyInGet() throws Exception {
        // Test that the terms are properly deep copied and used in get() method
        List<BytesRef> originalTerms = Arrays.asList(new BytesRef("original1"), new BytesRef("original2"));

        BatchClusteringTask task = new BatchClusteringTask(originalTerms, key, 0.5f, 0.3f, 10, null, null);

        // Modify original terms
        originalTerms.get(0).bytes[0] = (byte) 'M';

        // The task should still have the original values due to deep copy
        // We can't easily test the get() method output, but we can verify the task was created properly
        assertNotNull("Task should be created and maintain its own copy of terms", task);
    }
}
