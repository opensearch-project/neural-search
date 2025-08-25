/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.sparse.codec;

import lombok.SneakyThrows;
import org.apache.lucene.codecs.CodecUtil;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.FieldInfos;
import org.apache.lucene.index.SegmentInfo;
import org.apache.lucene.index.SegmentReadState;
import org.apache.lucene.store.DataInput;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.util.BytesRef;
import org.junit.After;
import org.junit.Before;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import org.opensearch.neuralsearch.sparse.AbstractSparseTestBase;
import org.opensearch.neuralsearch.sparse.TestsPrepareUtils;
import org.opensearch.neuralsearch.sparse.data.PostingClusters;

import java.io.IOException;
import java.util.Iterator;
import java.util.Set;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class SparseTermsLuceneReaderTests extends AbstractSparseTestBase {

    private final static String termName = "test_term";
    private final static String fieldName = "test_field";

    @Mock
    private Directory mockDirectory;

    @Mock
    private FieldInfos mockFieldInfos;

    @Mock
    private IndexInput mockTermsInput;

    @Mock
    private IndexInput mockPostingInput;

    private SegmentReadState segmentReadState;

    private MockedStatic<CodecUtil> codecUtilMock;

    @Before
    @SneakyThrows
    public void setUp() {
        super.setUp();
        MockitoAnnotations.openMocks(this);
        setupMockInputsForSuccessfulConstruction();

        SegmentInfo mockSegmentInfo = TestsPrepareUtils.prepareSegmentInfo();
        FieldInfo mockFieldInfo = TestsPrepareUtils.prepareKeyFieldInfo();
        when(mockFieldInfos.fieldInfo(0)).thenReturn(mockFieldInfo);
        when(mockDirectory.openInput(anyString(), any(IOContext.class))).thenReturn(mockTermsInput).thenReturn(mockPostingInput);
        codecUtilMock = Mockito.mockStatic(CodecUtil.class);
        codecUtilMock.when(
            () -> CodecUtil.checkIndexHeader(any(DataInput.class), anyString(), anyInt(), anyInt(), any(byte[].class), anyString())
        ).thenReturn(0);

        segmentReadState = new SegmentReadState(mockDirectory, mockSegmentInfo, mockFieldInfos, IOContext.DEFAULT, "test_suffix");
    }

    @After
    @Override
    public void tearDown() throws Exception {
        if (codecUtilMock != null) {
            codecUtilMock.close();
        }
        super.tearDown();
    }

    @SneakyThrows
    public void testConstructor_thenSuccess() {
        SparseTermsLuceneReader reader = new SparseTermsLuceneReader(segmentReadState);

        assertNotNull(reader);
        verify(mockTermsInput, never()).close();
        verify(mockPostingInput, never()).close();
    }

    @SneakyThrows
    public void testConstructor_withIOExceptionHandling() {
        doThrow(new IOException("Test exception")).when(mockTermsInput).readBytes(any(byte[].class), anyInt(), anyInt());

        SparseTermsLuceneReader reader = new SparseTermsLuceneReader(segmentReadState);

        assertNotNull(reader);
        verify(mockTermsInput, times(1)).close();
        verify(mockPostingInput, times(1)).close();
    }

    @SneakyThrows
    public void testIterator() {
        SparseTermsLuceneReader reader = new SparseTermsLuceneReader(segmentReadState);
        Iterator<String> iterator = reader.iterator();

        assertNotNull(iterator);
        assertTrue(iterator.hasNext());
        assertEquals(fieldName, iterator.next());
        assertFalse(iterator.hasNext());
    }

    @SneakyThrows
    public void testTerms_ThrowsUnsupportedOperationException() {
        SparseTermsLuceneReader reader = new SparseTermsLuceneReader(segmentReadState);

        Exception exception = expectThrows(UnsupportedOperationException.class, () -> reader.terms(fieldName));
        assertNull(exception.getMessage());
    }

    @SneakyThrows
    public void testGetTerms_withExistingField() {
        SparseTermsLuceneReader reader = new SparseTermsLuceneReader(segmentReadState);
        Set<BytesRef> terms = reader.getTerms(fieldName);

        assertNotNull(terms);
        assertEquals(1, terms.size());
    }

    @SneakyThrows
    public void testGetTerms_withNonExistingField() {
        SparseTermsLuceneReader reader = new SparseTermsLuceneReader(segmentReadState);
        Set<BytesRef> terms = reader.getTerms("non_existing_field");

        assertNotNull(terms);
        assertTrue(terms.isEmpty());
    }

    @SneakyThrows
    public void testRead_withExistingFieldAndTerm() {
        setupMockPostingInput();

        SparseTermsLuceneReader reader = new SparseTermsLuceneReader(segmentReadState);
        BytesRef term = new BytesRef(termName);
        PostingClusters clusters = reader.read(fieldName, term);

        assertNotNull(clusters);
    }

    @SneakyThrows
    public void testRead_withNonExistingField() {
        SparseTermsLuceneReader reader = new SparseTermsLuceneReader(segmentReadState);
        BytesRef term = new BytesRef(termName);
        PostingClusters clusters = reader.read("non_existing_field", term);

        assertNull(clusters);
    }

    @SneakyThrows
    public void testRead_withNonExistingTerm() {
        SparseTermsLuceneReader reader = new SparseTermsLuceneReader(segmentReadState);
        BytesRef term = new BytesRef("non_existing_term");
        PostingClusters clusters = reader.read(fieldName, term);

        assertNull(clusters);
    }

    @SneakyThrows
    public void testRead_withEmptyClusters() {
        setupMockPostingInputForEmptyClusters();

        SparseTermsLuceneReader reader = new SparseTermsLuceneReader(segmentReadState);
        BytesRef term = new BytesRef("test_term");
        PostingClusters clusters = reader.read(fieldName, term);

        assertNull(clusters);
    }

    @SneakyThrows
    public void testSize_thenThrowsUnsupportedOperationException() {
        SparseTermsLuceneReader reader = new SparseTermsLuceneReader(segmentReadState);

        Exception exception = expectThrows(UnsupportedOperationException.class, () -> reader.size());
        assertNull(exception.getMessage());
    }

    @SneakyThrows
    public void testClose() {
        SparseTermsLuceneReader reader = new SparseTermsLuceneReader(segmentReadState);
        reader.close();
    }

    @SneakyThrows
    public void testCheckIntegrity() {
        SparseTermsLuceneReader reader = new SparseTermsLuceneReader(segmentReadState);
        reader.checkIntegrity();
    }

    @SneakyThrows
    private void setupMockInputsForSuccessfulConstruction() {
        BytesRef bytesRef = new BytesRef(termName);
        // numberOfFields, fieldId and byteLength
        when(mockTermsInput.readVInt()).thenReturn(1).thenReturn(0).thenReturn(bytesRef.length);
        // numberOfTerms and fileOffset
        when(mockTermsInput.readVLong()).thenReturn(1L).thenReturn(50L);
        // dirOffset
        when(mockTermsInput.readLong()).thenReturn(42L);
        // read bytes
        doAnswer(invocation -> {
            byte[] bytes = invocation.getArgument(0);
            int offset = invocation.getArgument(1);
            int length = invocation.getArgument(2);

            // Copy the actual term bytes to the destination array
            System.arraycopy(bytesRef.bytes, 0, bytes, offset, length);

            return null;
        }).when(mockTermsInput).readBytes(any(byte[].class), anyInt(), anyInt());
    }

    @SneakyThrows
    private void setupMockPostingInput() {
        // clusterSize, docSize, summaryVectorSize
        when(mockPostingInput.readVLong())
            .thenReturn(1L)
            .thenReturn(1L)
            .thenReturn(1L);
        // doc id, sparse vector item index
        when(mockPostingInput.readVInt())
            .thenReturn(1)
            .thenReturn(1);
        // doc weight, shouldNotSkip, sparse vector item weight
        when(mockPostingInput.readByte())
            .thenReturn((byte) 1)
            .thenReturn((byte) 1)
            .thenReturn((byte) 1);
    }

    @SneakyThrows
    private void setupMockPostingInputForEmptyClusters() {
        when(mockPostingInput.readVLong()).thenReturn(0L);
    }
}
