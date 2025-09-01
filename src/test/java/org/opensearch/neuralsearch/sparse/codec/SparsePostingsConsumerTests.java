/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.sparse.codec;

import lombok.SneakyThrows;
import org.apache.lucene.codecs.FieldsConsumer;
import org.apache.lucene.codecs.NormsProducer;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.FieldInfos;
import org.apache.lucene.index.Fields;
import org.apache.lucene.index.MergeState;
import org.apache.lucene.index.SegmentWriteState;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.util.BytesRef;
import org.junit.After;
import org.junit.Before;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.MockedConstruction;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import org.opensearch.neuralsearch.sparse.AbstractSparseTestBase;
import org.opensearch.neuralsearch.sparse.TestsPrepareUtils;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockConstruction;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.opensearch.neuralsearch.sparse.common.SparseConstants.APPROXIMATE_THRESHOLD_FIELD;
import static org.opensearch.neuralsearch.sparse.mapper.SparseTokensField.SPARSE_FIELD;

public class SparsePostingsConsumerTests extends AbstractSparseTestBase {

    private static final String TEST_SPARSE_FIELD_NAME = "sparse_field";

    @Mock
    private FieldsConsumer mockDelegate;

    @Mock
    private Directory mockDirectory;

    @Mock
    private IndexOutput mockTermsOutput;

    @Mock
    private IndexOutput mockPostingOutput;

    @Mock
    private Fields mockFields;

    @Mock
    private NormsProducer mockNormsProducer;

    @Mock
    private MergeHelper mockedMergeHelper;
    MockedConstruction<SparseTermsLuceneWriter> mockedSparseTermsWriter;
    MockedConstruction<ClusteredPostingTermsWriter> mockedClusteredWriter;

    private FieldInfos mockFieldInfos;
    private SegmentWriteState mockWriteState;
    private SparsePostingsConsumer sparsePostingsConsumer;

    @Before
    @Override
    @SneakyThrows
    public void setUp() {
        super.setUp();
        MockitoAnnotations.openMocks(this);

        // configure mocks
        when(mockDirectory.createOutput(anyString(), any())).thenReturn(mockTermsOutput, mockPostingOutput);
        mockFieldInfos = mock(FieldInfos.class);
        mockWriteState = TestsPrepareUtils.prepareSegmentWriteState(mockDirectory, mockFieldInfos);
        mockedClusteredWriter = mockConstruction(ClusteredPostingTermsWriter.class);
        mockedSparseTermsWriter = mockConstruction(SparseTermsLuceneWriter.class);
        sparsePostingsConsumer = new SparsePostingsConsumer(mockDelegate, mockWriteState, mockedMergeHelper);
    }

    @After
    @Override
    @SneakyThrows
    public void tearDown() {
        if (mockedClusteredWriter != null) {
            mockedClusteredWriter.close();
        }
        if (mockedSparseTermsWriter != null) {
            mockedSparseTermsWriter.close();
        }
        super.tearDown();
    }

    public void test_constructor_throwsNPE_whenParametersAreNull() {
        expectThrows(NullPointerException.class, () -> { new SparsePostingsConsumer(null, mockWriteState, mockedMergeHelper); });

        expectThrows(NullPointerException.class, () -> { new SparsePostingsConsumer(mockDelegate, null, mockedMergeHelper); });

        expectThrows(NullPointerException.class, () -> { new SparsePostingsConsumer(mockDelegate, mockWriteState, null); });
    }

    /**
     * Test constructor with default version
     */
    public void test_constructor_withDefaultVersion() throws IOException {
        SparsePostingsConsumer consumer = new SparsePostingsConsumer(mockDelegate, mockWriteState, mockedMergeHelper);
        assertNotNull("Consumer should be created", consumer);
    }

    /**
     * Test constructor with specific version
     */
    public void test_constructor_withSpecificVersion() throws IOException {
        int version = 1;
        SparsePostingsConsumer consumer = new SparsePostingsConsumer(mockDelegate, mockWriteState, mockedMergeHelper, version);
        assertNotNull("Consumer should be created", consumer);
    }

    /**
     * Test constructor handles IOException properly
     */
    public void test_constructor_withIOException() throws IOException {
        when(mockDirectory.createOutput(anyString(), any())).thenThrow(new IOException("Test exception"));

        IOException exception = expectThrows(IOException.class, () -> {
            new SparsePostingsConsumer(mockDelegate, mockWriteState, mockedMergeHelper);
        });

        assertEquals("Test exception", exception.getMessage());
    }

    /**
     * Test write method with no sparse fields
     */
    public void test_write_withNonSparseFields() throws IOException {
        // Get the constructed mock instances
        List<SparseTermsLuceneWriter> sparseTermsWriters = mockedSparseTermsWriter.constructed();
        SparseTermsLuceneWriter sparseTermsWriter = sparseTermsWriters.get(0);

        // Setup field info with non-sparse fields
        List<String> fieldNames = List.of("non_sparse_field1", "non_sparse_field2");
        when(mockFields.iterator()).thenReturn(fieldNames.iterator());
        FieldInfo mockFieldInfo = mock(FieldInfo.class);
        when(mockFieldInfos.fieldInfo(anyString())).thenReturn(mockFieldInfo);

        // Set up the delegate to capture the Fields argument
        ArgumentCaptor<Fields> fieldsCaptor = ArgumentCaptor.forClass(Fields.class);

        // Call write
        sparsePostingsConsumer.write(mockFields, mockNormsProducer);

        // Verify delegate was called and sparse writer was not used
        verify(mockDelegate).write(fieldsCaptor.capture(), eq(mockNormsProducer));
        verify(sparseTermsWriter, never()).writeFieldCount(any(Integer.class));

        // Verify the masked fields have the correct iterator
        Fields capturedFields = fieldsCaptor.getValue();
        Iterator<String> iterator = capturedFields.iterator();
        assertTrue(iterator.hasNext());
        assertEquals("non_sparse_field1", iterator.next());
        assertTrue(iterator.hasNext());
        assertEquals("non_sparse_field2", iterator.next());
        assertFalse(iterator.hasNext());
    }

    /**
     * Test write method with sparse fields and terms
     */
    @SneakyThrows
    public void test_write_withSparseFields_andTerms() {
        // Get the constructed mock instances
        List<SparseTermsLuceneWriter> sparseTermsWriters = mockedSparseTermsWriter.constructed();
        SparseTermsLuceneWriter sparseTermsWriter = sparseTermsWriters.get(0);

        // Setup field info with sparse fields
        FieldInfo mockFieldInfo = mock(FieldInfo.class);
        when(mockFieldInfo.getName()).thenReturn(SPARSE_FIELD);
        Map<String, String> sparseAttributes = new HashMap<>();
        sparseAttributes.put(SPARSE_FIELD, String.valueOf(true));
        sparseAttributes.put(APPROXIMATE_THRESHOLD_FIELD, String.valueOf(1));
        when(mockFieldInfo.attributes()).thenReturn(sparseAttributes);

        // Setup fieldInfos mock
        when(mockFieldInfos.fieldInfo(anyString())).thenReturn(mockFieldInfo);

        // Setup fields mock
        List<String> fieldNames = List.of(TEST_SPARSE_FIELD_NAME);
        when(mockFields.iterator()).thenReturn(fieldNames.iterator());

        // Setup terms mock
        Terms mockedTerms = mock(Terms.class);
        when(mockFields.terms(TEST_SPARSE_FIELD_NAME)).thenReturn(mockedTerms);
        TermsEnum mockedTermsEnum = mock(TermsEnum.class);
        when(mockedTerms.iterator()).thenReturn(mockedTermsEnum);
        BytesRef term1 = new BytesRef("term1");
        BytesRef term2 = new BytesRef("term2");
        when(mockedTermsEnum.next()).thenReturn(term1).thenReturn(term2).thenReturn(null);

        // Call write
        sparsePostingsConsumer.write(mockFields, mockNormsProducer);

        verify(sparseTermsWriter, times(1)).writeFieldCount(1);
        verify(sparseTermsWriter, times(1)).writeTermsSize(2);
    }

    /**
     * Test write method with sparse fields and empty terms
     */
    @SneakyThrows
    public void test_write_withSparseFields_andEmptyTerms() {
        // Get the constructed mock instances
        List<SparseTermsLuceneWriter> sparseTermsWriters = mockedSparseTermsWriter.constructed();
        SparseTermsLuceneWriter sparseTermsWriter = sparseTermsWriters.get(0);

        // Setup field info with sparse fields
        FieldInfo mockFieldInfo = mock(FieldInfo.class);
        when(mockFieldInfo.getName()).thenReturn(SPARSE_FIELD);
        Map<String, String> sparseAttributes = new HashMap<>();
        sparseAttributes.put(SPARSE_FIELD, String.valueOf(true));
        sparseAttributes.put(APPROXIMATE_THRESHOLD_FIELD, String.valueOf(1));
        when(mockFieldInfo.attributes()).thenReturn(sparseAttributes);

        // Setup fieldInfos mock
        when(mockFieldInfos.fieldInfo(anyString())).thenReturn(mockFieldInfo);

        // Setup fields mock
        List<String> fieldNames = List.of(TEST_SPARSE_FIELD_NAME);
        when(mockFields.iterator()).thenReturn(fieldNames.iterator());
        when(mockFields.terms(TEST_SPARSE_FIELD_NAME)).thenReturn(null);

        // Call write
        sparsePostingsConsumer.write(mockFields, mockNormsProducer);

        verify(sparseTermsWriter, times(1)).writeFieldCount(1);
        verify(sparseTermsWriter, times(1)).writeTermsSize(0);
    }

    /**
     * Test merge method
     */
    @SneakyThrows
    public void test_merge() {
        MergeState mockMergeState = TestsPrepareUtils.prepareMergeState(true);

        sparsePostingsConsumer.merge(mockMergeState, mockNormsProducer);

        verify(mockedMergeHelper, times(1)).clearCacheData(any(), any(), any());
    }

    /**
     * Test merge method handles exceptions
     */
    @SneakyThrows
    public void test_merge_withExceptions() {
        // mock exception thrown from SparsePostingsReader merge
        MergeState mockMergeState = TestsPrepareUtils.prepareMergeState(true);
        MockedConstruction<SparsePostingsReader> mockedReader = Mockito.mockConstruction(SparsePostingsReader.class, (mock, context) -> {
            doThrow(new IOException("Test exception")).when(mock).merge(any(), any());
        });

        sparsePostingsConsumer.merge(mockMergeState, mockNormsProducer);

        verify(mockedMergeHelper, never()).clearCacheData(any(), any(), any());
        mockedReader.close();
    }

    /**
     * Test close method with successful close
     */
    @SneakyThrows
    public void test_close() {
        // Get the constructed mock instances
        List<ClusteredPostingTermsWriter> clusteredWriters = mockedClusteredWriter.constructed();
        List<SparseTermsLuceneWriter> sparseTermsWriters = mockedSparseTermsWriter.constructed();
        SparseTermsLuceneWriter sparseTermsWriter = sparseTermsWriters.get(0);
        ClusteredPostingTermsWriter clusteredWriter = clusteredWriters.get(0);

        // Call the close method
        sparsePostingsConsumer.close();

        // Verify delegate, writers and outputs call the close method
        verify(mockDelegate).close();
        verify(sparseTermsWriter).close(anyLong());
        verify(clusteredWriter).close(anyLong());
        verify(mockTermsOutput).close();
        verify(mockPostingOutput).close();
    }

    /**
     * Test close method handles exceptions from writers
     */
    @SneakyThrows
    public void test_close_withExceptions() {
        // Mock exception thrown from sparseTermsLuceneWriter close
        List<SparseTermsLuceneWriter> sparseTermsWriters = mockedSparseTermsWriter.constructed();
        SparseTermsLuceneWriter sparseTermsWriter = sparseTermsWriters.get(0);
        doThrow(new IOException("Test exception")).when(sparseTermsWriter).close(anyLong());

        // Verify exception message
        IOException exception = expectThrows(IOException.class, () -> { sparsePostingsConsumer.close(); });
        assertEquals("Test exception", exception.getMessage());

        // Verify delegate and outputs call the close method
        verify(mockDelegate).close();
        verify(mockTermsOutput).close();
        verify(mockPostingOutput).close();
    }
}
