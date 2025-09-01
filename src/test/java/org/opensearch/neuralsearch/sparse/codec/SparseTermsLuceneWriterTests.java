/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.sparse.codec;

import lombok.SneakyThrows;
import org.apache.lucene.codecs.BlockTermState;
import org.apache.lucene.codecs.CodecUtil;
import org.apache.lucene.index.SegmentWriteState;
import org.apache.lucene.store.DataOutput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.util.BytesRef;
import org.junit.After;
import org.junit.Before;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import org.opensearch.common.util.io.IOUtils;
import org.opensearch.neuralsearch.sparse.AbstractSparseTestBase;
import org.opensearch.neuralsearch.sparse.TestsPrepareUtils;

import java.io.IOException;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

public class SparseTermsLuceneWriterTests extends AbstractSparseTestBase {

    private static final String CODEC_NAME = "TestCodec";
    private static final int VERSION = 1;
    private static final long START_FP = 100L;
    private static final int FIELD_COUNT = 5;
    private static final int FIELD_NUMBER = 10;
    private static final long TERMS_SIZE = 1000L;

    @Mock
    private IndexOutput mockIndexOutput;
    @Mock
    private BlockTermState mockBlockTermState;

    private MockedStatic<CodecUtil> codecUtilMock;
    private MockedStatic<IOUtils> ioUtilsMock;
    private SparseTermsLuceneWriter writer;
    private SegmentWriteState mockSegmentWriteState;

    @Before
    @Override
    public void setUp() {
        super.setUp();
        MockitoAnnotations.openMocks(this);

        ioUtilsMock = Mockito.mockStatic(IOUtils.class);
        codecUtilMock = Mockito.mockStatic(CodecUtil.class);

        mockSegmentWriteState = TestsPrepareUtils.prepareSegmentWriteState();
        writer = new SparseTermsLuceneWriter(CODEC_NAME, VERSION);
    }

    @After
    @Override
    public void tearDown() throws Exception {
        codecUtilMock.close();
        ioUtilsMock.close();
        super.tearDown();
    }

    @SneakyThrows
    public void testConstructor() {
        SparseTermsLuceneWriter testWriter = new SparseTermsLuceneWriter(CODEC_NAME, VERSION);
        assertNotNull(testWriter);

        // verify constructor correctly sets codec name and version
        testWriter.init(mockIndexOutput, mockSegmentWriteState);
        codecUtilMock.verify(
            () -> CodecUtil.writeIndexHeader(
                eq(mockIndexOutput),
                eq(CODEC_NAME),
                eq(VERSION),
                any(byte[].class),
                eq(mockSegmentWriteState.segmentSuffix)
            ),
            times(1)
        );
    }

    @SneakyThrows
    public void testInit_success() {
        writer.init(mockIndexOutput, mockSegmentWriteState);

        codecUtilMock.verify(
            () -> CodecUtil.writeIndexHeader(
                eq(mockIndexOutput),
                eq(CODEC_NAME),
                eq(VERSION),
                any(byte[].class),
                eq(mockSegmentWriteState.segmentSuffix)
            ),
            times(1)
        );
    }

    @SneakyThrows
    public void testInit_throwsIOException() {
        IOException expectedException = new IOException("Test exception");
        codecUtilMock.when(() -> CodecUtil.writeIndexHeader(any(DataOutput.class), anyString(), anyInt(), any(byte[].class), anyString()))
            .thenThrow(expectedException);

        IOException exception = expectThrows(IOException.class, () -> writer.init(mockIndexOutput, mockSegmentWriteState));
        assertEquals("Test exception", exception.getMessage());
    }

    @SneakyThrows
    public void testClose_success() {
        writer.init(mockIndexOutput, mockSegmentWriteState);
        writer.close(START_FP);

        verify(mockIndexOutput, times(1)).writeLong(START_FP);
        codecUtilMock.verify(() -> CodecUtil.writeFooter(mockIndexOutput), times(1));
    }

    @SneakyThrows
    public void testClose_throwsIOException() {
        writer.init(mockIndexOutput, mockSegmentWriteState);
        IOException expectedException = new IOException("Test exception");
        doThrow(expectedException).when(mockIndexOutput).writeLong(anyLong());

        IOException exception = expectThrows(IOException.class, () -> writer.close(START_FP));
        assertEquals("Test exception", exception.getMessage());
    }

    @SneakyThrows
    public void testWriteFieldCount_success() {
        writer.init(mockIndexOutput, mockSegmentWriteState);
        writer.writeFieldCount(FIELD_COUNT);

        verify(mockIndexOutput, times(1)).writeVInt(FIELD_COUNT);
    }

    @SneakyThrows
    public void testWriteFieldCount_throwsIOException() {
        writer.init(mockIndexOutput, mockSegmentWriteState);
        IOException expectedException = new IOException("Test exception");
        doThrow(expectedException).when(mockIndexOutput).writeVInt(anyInt());

        IOException exception = expectThrows(IOException.class, () -> writer.writeFieldCount(FIELD_COUNT));
        assertEquals("Test exception", exception.getMessage());
    }

    @SneakyThrows
    public void testWriteFieldNumber_success() {
        writer.init(mockIndexOutput, mockSegmentWriteState);
        writer.writeFieldNumber(FIELD_NUMBER);

        verify(mockIndexOutput, times(1)).writeVInt(FIELD_NUMBER);
    }

    @SneakyThrows
    public void testWriteFieldNumber_throwsIOException() {
        writer.init(mockIndexOutput, mockSegmentWriteState);
        IOException expectedException = new IOException("Test exception");
        doThrow(expectedException).when(mockIndexOutput).writeVInt(anyInt());

        IOException exception = expectThrows(IOException.class, () -> writer.writeFieldNumber(FIELD_NUMBER));
        assertEquals("Test exception", exception.getMessage());
    }

    @SneakyThrows
    public void testWriteTermsSize_success() {
        writer.init(mockIndexOutput, mockSegmentWriteState);
        writer.writeTermsSize(TERMS_SIZE);

        verify(mockIndexOutput, times(1)).writeVLong(TERMS_SIZE);
    }

    @SneakyThrows
    public void testWriteTermsSize_throwsIOException() {
        writer.init(mockIndexOutput, mockSegmentWriteState);
        IOException expectedException = new IOException("Test exception");
        doThrow(expectedException).when(mockIndexOutput).writeVLong(anyLong());

        IOException exception = expectThrows(IOException.class, () -> writer.writeTermsSize(TERMS_SIZE));
        assertEquals("Test exception", exception.getMessage());
    }

    @SneakyThrows
    public void testWriteTerm_success() {
        writer.init(mockIndexOutput, mockSegmentWriteState);
        BytesRef term = new BytesRef("test_term");

        writer.writeTerm(term, mockBlockTermState);

        verify(mockIndexOutput, times(1)).writeVInt(term.length);
        verify(mockIndexOutput, times(1)).writeBytes(term.bytes, term.offset, term.length);
        verify(mockIndexOutput, times(1)).writeVLong(mockBlockTermState.blockFilePointer);
    }

    @SneakyThrows
    public void testWriteTerm_throwsIOException() {
        writer.init(mockIndexOutput, mockSegmentWriteState);
        BytesRef term = new BytesRef("test_term");
        IOException expectedException = new IOException("Test exception");
        doThrow(expectedException).when(mockIndexOutput).writeVInt(anyInt());

        IOException exception = expectThrows(IOException.class, () -> writer.writeTerm(term, mockBlockTermState));
        assertEquals("Test exception", exception.getMessage());
    }

    @SneakyThrows
    public void testCloseWithException() {
        writer.init(mockIndexOutput, mockSegmentWriteState);
        writer.closeWithException();

        ioUtilsMock.verify(() -> IOUtils.closeWhileHandlingException(mockIndexOutput), times(1));
    }
}
