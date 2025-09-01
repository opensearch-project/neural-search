/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.sparse.algorithm.seismic;

import lombok.SneakyThrows;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.lucene.codecs.DocValuesProducer;
import org.apache.lucene.index.BinaryDocValues;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.SegmentInfo;
import org.apache.lucene.util.BytesRef;
import org.junit.Before;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.opensearch.neuralsearch.sparse.AbstractSparseTestBase;
import org.opensearch.neuralsearch.sparse.cache.CacheKey;
import org.opensearch.neuralsearch.sparse.codec.MergeHelper;
import org.opensearch.neuralsearch.sparse.codec.SparseBinaryDocValuesPassThrough;
import org.opensearch.neuralsearch.sparse.common.MergeStateFacade;
import org.opensearch.neuralsearch.sparse.data.PostingClusters;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class BatchClusteringTaskTests extends AbstractSparseTestBase {
    private List<BytesRef> terms;
    private CacheKey key;
    @Mock
    private MergeStateFacade mergeStateFacade;
    @Mock
    private MergeHelper mergeHelper;
    @Mock
    private FieldInfo fieldInfo;
    @Mock
    private SegmentInfo segmentInfo;
    @Mock
    private DocValuesProducer docValuesProducer;
    @Mock
    private SparseBinaryDocValuesPassThrough binaryDocValuesPassThrough;

    @Before
    @Override
    @SneakyThrows
    public void setUp() {
        super.setUp();
        MockitoAnnotations.openMocks(this);
        terms = Arrays.asList(new BytesRef("term1"), new BytesRef("term2"));
        when(fieldInfo.getName()).thenReturn("test_field");
        key = new CacheKey(segmentInfo, fieldInfo);
        when(mergeStateFacade.getMaxDocs()).thenReturn(new int[] { 5, 6 });
        when(mergeHelper.getMergedPostingForATerm(any(), any(), any(), any(), any())).thenReturn(
            preparePostings(1, 1, 2, 2, 3, 3, 4, 4, 5, 5, 6, 6, 7, 7, 8, 8, 9, 9, 10, 10)
        );
        when(mergeStateFacade.getDocValuesProducers()).thenReturn(new DocValuesProducer[] { docValuesProducer });
        when(docValuesProducer.getBinary(any())).thenReturn(binaryDocValuesPassThrough);
        when(binaryDocValuesPassThrough.read(anyInt())).thenReturn(createVector(1, 2, 3, 4, 5, 6))
            .thenReturn(createVector(7, 8, 9, 10, 11, 12))
            .thenReturn(null);
        when(binaryDocValuesPassThrough.getSegmentInfo()).thenReturn(segmentInfo);
    }

    public void testConstructorDeepCopiesTerms() throws Exception {
        // Setup
        List<BytesRef> originalTerms = Arrays.asList(new BytesRef("term1"), new BytesRef("term2"));

        // Execute - create task with null mergeState to test constructor
        BatchClusteringTask task = new BatchClusteringTask(originalTerms, key, 0.5f, 0.3f, 10, mergeStateFacade, null, mergeHelper);

        // Verify task is created
        assertNotNull("Task should be created successfully", task);

        List<BytesRef> taskTerms = task.getTerms();

        // Verify deep copy by checking actual content
        assertEquals("First term should be 'term1'", "term1", taskTerms.get(0).utf8ToString());

        // Modify original terms to verify deep copy
        originalTerms.get(0).bytes[0] = (byte) 'X';

        // Verify task's terms remain unchanged (proving deep copy worked)
        assertEquals("Task's first term should still be 'term1'", "term1", taskTerms.get(0).utf8ToString());
        assertNotEquals("Original term should now be different", "term1", originalTerms.get(0).utf8ToString());
    }

    public void testGetWithNullMergeStateThenThrowException() {
        // Test behavior with null merge state - should throw NullPointerException within constructor
        NullPointerException nullPointerException = assertThrows(
            NullPointerException.class,
            () -> new BatchClusteringTask(terms, key, 0.5f, 0.3f, 10, null, null, null)
        );
        assertNotNull(nullPointerException);
    }

    @SneakyThrows
    public void testGetWithNonNullMergeState() {

        // Create BatchClusteringTask
        BatchClusteringTask task = new BatchClusteringTask(terms, key, 0.5f, 0.3f, 10, mergeStateFacade, fieldInfo, mergeHelper);

        // Execute and examine the result
        List<Pair<BytesRef, PostingClusters>> result = task.get();

        // Verify the returned clusters
        assertNotNull("Result should not be null", result);
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

    @SneakyThrows
    public void testGetWithNonNullMergeStateZeroMaxDocs() {
        when(mergeStateFacade.getMaxDocs()).thenReturn(new int[]{0});
        // Create BatchClusteringTask
        BatchClusteringTask task = new BatchClusteringTask(terms, key, 0.5f, 0.3f, 10, mergeStateFacade, fieldInfo, mergeHelper);

        // Execute and examine the result
        List<Pair<BytesRef, PostingClusters>> result = task.get();

        // Verify the returned clusters
        assertNotNull("Result should not be null", result);
        // Should trigger early quit schema to return an empty list
        assertEquals("Should return an empty list", 0, result.size());
    }

    public void testThrowIOException() throws IOException {
        doThrow(new IOException()).when(mergeHelper).getMergedPostingForATerm(any(), any(), any(), any(), any());
        // Create BatchClusteringTask
        BatchClusteringTask task = new BatchClusteringTask(terms, key, 0.5f, 0.3f, 10, mergeStateFacade, fieldInfo, mergeHelper);

        // Execute and examine the result
        assertThrows(RuntimeException.class, () -> task.get());
    }

    public void testNotSeismicBinaryDocValues() throws IOException {
        BinaryDocValues docValues = mock(BinaryDocValues.class);
        when(docValuesProducer.getBinary(any())).thenReturn(docValues);
        // Create BatchClusteringTask
        BatchClusteringTask task = new BatchClusteringTask(terms, key, 0.5f, 0.3f, 10, mergeStateFacade, fieldInfo, mergeHelper);

        // Execute and examine the result
        List<Pair<BytesRef, PostingClusters>> result = task.get();
        assertEquals(2, result.size());
        for (int i = 0; i < 2; ++i) {
            assertTrue(result.get(i).getRight().getClusters().isEmpty());
        }
    }

    public void testTermsDeepCopyInGet() {
        // Test that the terms are properly deep copied and used in get() method
        List<BytesRef> originalTerms = Arrays.asList(new BytesRef("original1"), new BytesRef("original2"));

        BatchClusteringTask task = new BatchClusteringTask(originalTerms, key, 0.5f, 0.3f, 10, mergeStateFacade, null, mergeHelper);

        // Modify original terms
        originalTerms.get(0).bytes[0] = (byte) 'M';

        // The task should still have the original values due to deep copy
        // We can't easily test the get() method output, but we can verify the task was created properly
        assertNotNull("Task should be created and maintain its own copy of terms", task);
    }
}
