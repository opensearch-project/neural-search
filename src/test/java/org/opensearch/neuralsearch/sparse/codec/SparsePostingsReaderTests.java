/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.sparse.codec;

import lombok.SneakyThrows;
import org.apache.lucene.codecs.FieldsProducer;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.FieldInfos;
import org.apache.lucene.index.SegmentInfo;
import org.apache.lucene.util.BytesRef;
import org.junit.Before;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.opensearch.neuralsearch.sparse.AbstractSparseTestBase;
import org.opensearch.neuralsearch.sparse.algorithm.ClusterTrainingExecutor;
import org.opensearch.neuralsearch.sparse.common.MergeStateFacade;
import org.opensearch.threadpool.ThreadPool;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class SparsePostingsReaderTests extends AbstractSparseTestBase {

    @Mock
    private ThreadPool mockThreadPool;
    @Mock
    private ExecutorService mockExecutor;
    @Mock
    private FieldsProducer mockFieldsProducer;
    @Mock
    private SparseTermsLuceneWriter mockSparseTermsWriter;
    @Mock
    private ClusteredPostingTermsWriter mockClusteredWriter;
    @Mock
    private MergeHelper mergeHelper;
    @Mock
    private MergeStateFacade mockMergeState;
    @Mock
    private FieldInfo mockSparseFieldInfo;
    @Mock
    private FieldInfo mockNonSparseFieldInfo;
    @Mock
    private FieldInfos mockFieldInfos;
    @Mock
    private SegmentInfo mockSegmentInfo;

    private SparsePostingsReader reader;
    private static final Set<BytesRef> TERMS = Set.of(new BytesRef("term"));

    @Before
    @Override
    @SneakyThrows
    public void setUp() {
        super.setUp();
        MockitoAnnotations.openMocks(this);

        // configure executor service for cluster training running
        doAnswer(invocation -> {
            Runnable runnable = invocation.getArgument(0);
            runnable.run();
            return null;
        }).when(mockExecutor).execute(any(Runnable.class));

        when(mockThreadPool.executor(anyString())).thenReturn(mockExecutor);
        ClusterTrainingExecutor.getInstance().initialize(mockThreadPool);

        // configure merge state
        Map<String, String> attributes = prepareAttributes(true, 10, 0.1f, 200, 0.4f);
        when(mockSparseFieldInfo.attributes()).thenReturn(attributes);
        attributes = prepareAttributes(false, 10, 0.1f, -1, 0.4f);
        when(mockNonSparseFieldInfo.attributes()).thenReturn(attributes);
        when(mockMergeState.getFieldsProducers()).thenReturn(new FieldsProducer[] { mockFieldsProducer });
        when(mockFieldInfos.iterator()).thenReturn(List.of(mockSparseFieldInfo, mockNonSparseFieldInfo).iterator());
        when(mockMergeState.getMergeFieldInfos()).thenReturn(mockFieldInfos);
        when(mockMergeState.getMaxDocs()).thenReturn(new int[] { 5, 5 });
        when(mockMergeState.getSegmentInfo()).thenReturn(mockSegmentInfo);
        when(mockSegmentInfo.maxDoc()).thenReturn(10);
        when(mergeHelper.getAllTerms(any(), any())).thenReturn(TERMS);
        reader = new SparsePostingsReader(mockMergeState, mergeHelper);
    }

    public void testConstructor() {
        assertNotNull(reader);
    }

    @SneakyThrows
    public void testMerge_success() {
        reader.merge(mockSparseTermsWriter, mockClusteredWriter);

        verify(mockSparseTermsWriter, times(1)).writeFieldCount(1);
        verify(mockSparseTermsWriter, times(1)).writeFieldNumber(anyInt());
        verify(mockSparseTermsWriter, times(1)).writeTermsSize(1L);
        verify(mockExecutor, times(1)).execute(any(Runnable.class));
    }

    @SneakyThrows
    public void testMerge_withDefaultNPosting() {
        Map<String, String> attributes = prepareAttributes(true, 10, 0.1f, -1, 0.4f);
        when(mockSparseFieldInfo.attributes()).thenReturn(attributes);

        SparsePostingsReader reader = new SparsePostingsReader(mockMergeState, mergeHelper);
        reader.merge(mockSparseTermsWriter, mockClusteredWriter);

        verify(mockSparseTermsWriter, times(1)).writeFieldCount(1);
        verify(mockSparseTermsWriter, times(1)).writeFieldNumber(anyInt());
        verify(mockSparseTermsWriter, times(1)).writeTermsSize(1L);
        verify(mockExecutor, times(1)).execute(any(Runnable.class));
    }

    @SneakyThrows
    public void testMerge_withNonSparseTerm() {
        when(mockFieldInfos.iterator()).thenReturn(List.of(mockNonSparseFieldInfo).iterator());
        when(mockMergeState.getMergeFieldInfos()).thenReturn(mockFieldInfos);
        reader.merge(mockSparseTermsWriter, mockClusteredWriter);

        verify(mockSparseTermsWriter, times(1)).writeFieldCount(0);
        verify(mockSparseTermsWriter, never()).writeFieldNumber(anyInt());
        verify(mockSparseTermsWriter, never()).writeTermsSize(1L);
        verify(mockExecutor, never()).execute(any(Runnable.class));
    }

    @SneakyThrows
    public void testMerge_withIOException() {
        doThrow(new IOException("Test exception")).when(mockSparseTermsWriter).writeFieldCount(1);
        Exception exception = expectThrows(IOException.class, () -> reader.merge(mockSparseTermsWriter, mockClusteredWriter));
        assertEquals("Test exception", exception.getMessage());
        verify(mockSparseTermsWriter, times(1)).closeWithException();
        verify(mockClusteredWriter, times(1)).closeWithException();
        verify(mockExecutor, never()).execute(any(Runnable.class));
    }

    @SneakyThrows
    public void testMerge_ClusterDocCountSmallerThanThreshold() {
        Map<String, String> attributes = prepareAttributes(true, 100, 0.1f, -1, 0.4f);
        when(mockSparseFieldInfo.attributes()).thenReturn(attributes);
        reader.merge(mockSparseTermsWriter, mockClusteredWriter);

        verify(mockSparseTermsWriter, times(1)).writeFieldCount(0);
        verify(mockSparseTermsWriter, never()).writeFieldNumber(anyInt());
        verify(mockSparseTermsWriter, never()).writeTermsSize(anyLong());
        verify(mockExecutor, never()).execute(any(Runnable.class));
    }

    @SneakyThrows
    public void testMerge_ZeroClusterRatio() {
        Map<String, String> attributes = prepareAttributes(true, 10, 0f, -1, 0.4f);
        when(mockSparseFieldInfo.attributes()).thenReturn(attributes);
        reader.merge(mockSparseTermsWriter, mockClusteredWriter);

        verify(mockSparseTermsWriter, times(1)).writeFieldCount(1);
        verify(mockSparseTermsWriter, times(1)).writeFieldNumber(anyInt());
        verify(mockSparseTermsWriter, times(1)).writeTermsSize(1L);
        verify(mockExecutor, never()).execute(any(Runnable.class));
    }

}
