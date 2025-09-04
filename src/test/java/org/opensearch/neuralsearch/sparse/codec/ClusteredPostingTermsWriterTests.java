/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.sparse.codec;

import lombok.SneakyThrows;
import org.apache.lucene.codecs.BlockTermState;
import org.apache.lucene.codecs.Codec;
import org.apache.lucene.codecs.DocValuesFormat;
import org.apache.lucene.codecs.DocValuesProducer;
import org.apache.lucene.codecs.NormsProducer;
import org.apache.lucene.codecs.lucene101.Lucene101PostingsFormat;
import org.apache.lucene.index.BinaryDocValues;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.index.SegmentInfo;
import org.apache.lucene.index.SegmentReadState;
import org.apache.lucene.index.SegmentWriteState;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.FixedBitSet;
import org.junit.Before;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.opensearch.neuralsearch.sparse.AbstractSparseTestBase;
import org.opensearch.neuralsearch.sparse.TestsPrepareUtils;
import org.opensearch.neuralsearch.sparse.data.DocWeight;
import org.opensearch.neuralsearch.sparse.data.DocumentCluster;
import org.opensearch.neuralsearch.sparse.data.PostingClusters;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class ClusteredPostingTermsWriterTests extends AbstractSparseTestBase {

    private static final int VERSION = 1;
    private static final String CODEC_NAME = "test_codec";

    private SegmentWriteState mockWriteState;
    private ClusteredPostingTermsWriter clusteredPostingTermsWriter;

    @Mock
    private Codec mockCodec;

    @Mock
    private DocValuesFormat mockDocValuesFormat;

    @Mock
    private DocValuesProducer mockDocValuesProducer;

    @Mock
    private BinaryDocValues mockBinaryDocValues;

    @Mock
    private Directory mockDirectory;

    @Mock
    private IndexOutput mockIndexOutput;

    @Mock
    private SegmentInfo mockSegmentInfo;

    @Mock
    private IndexOptions mockIndexOptions;

    @Mock
    private FieldInfo mockFieldInfo;

    @Mock
    private CodecUtilWrapper mockCodecUtilWrapper;

    @Before
    @SneakyThrows
    public void setUp() {
        super.setUp();
        MockitoAnnotations.openMocks(this);

        // configure mocks
        mockWriteState = TestsPrepareUtils.prepareSegmentWriteState(mockSegmentInfo);
        when(mockFieldInfo.attributes()).thenReturn(prepareAttributes(true, 10, 0.1f, -1, 0.4f));

        when(mockSegmentInfo.getCodec()).thenReturn(mockCodec);
        when(mockCodec.docValuesFormat()).thenReturn(mockDocValuesFormat);
        when(mockDocValuesFormat.fieldsProducer(any(SegmentReadState.class))).thenReturn(mockDocValuesProducer);
        when(mockDocValuesProducer.getBinary(any(FieldInfo.class))).thenReturn(mockBinaryDocValues);
        when(mockDirectory.createOutput(any(String.class), any())).thenReturn(mockIndexOutput);
        when(mockFieldInfo.getIndexOptions()).thenReturn(mockIndexOptions);
        clusteredPostingTermsWriter = new ClusteredPostingTermsWriter(CODEC_NAME, VERSION, mockCodecUtilWrapper);
        clusteredPostingTermsWriter.init(mockIndexOutput, mockWriteState);
    }

    @SneakyThrows
    public void test_write_withTermsEnum() {
        clusteredPostingTermsWriter = spy(clusteredPostingTermsWriter);

        TermsEnum mockTermsEnum = mock(TermsEnum.class);
        NormsProducer mockNormsProducer = mock(NormsProducer.class);

        // Mock the super.writeTerm() to return our expected state
        BytesRef testText = new BytesRef("test");
        BlockTermState expectedState = new Lucene101PostingsFormat.IntBlockTermState();
        doReturn(expectedState).when(clusteredPostingTermsWriter)
            .writeTerm(eq(testText), eq(mockTermsEnum), any(FixedBitSet.class), eq(mockNormsProducer));

        BlockTermState result = clusteredPostingTermsWriter.write(testText, mockTermsEnum, mockNormsProducer);

        assertSame("Should return the BlockTermState from writeTerm", expectedState, result);
        verify(clusteredPostingTermsWriter).writeTerm(eq(testText), eq(mockTermsEnum), any(FixedBitSet.class), eq(mockNormsProducer));
    }

    @SneakyThrows
    public void test_write_withPostingClusters() {
        BytesRef text = new BytesRef("test_term");
        PostingClusters postingClusters = mock(PostingClusters.class);
        BlockTermState result = clusteredPostingTermsWriter.write(text, postingClusters);

        // Verify that the result is non-null and writePostingClusters gets called
        assertNotNull("BlockTermState should not be null", result);
        verify(mockIndexOutput, atLeastOnce()).writeVLong(anyLong());
    }

    @SneakyThrows
    public void test_setFieldAndMaxDoc_withoutMerge() {
        clusteredPostingTermsWriter = spy(this.clusteredPostingTermsWriter);

        clusteredPostingTermsWriter.setFieldAndMaxDoc(mockFieldInfo, 100, false);

        verify(clusteredPostingTermsWriter).setField(mockFieldInfo);
        verify(mockDocValuesFormat, times(1)).fieldsProducer(any(SegmentReadState.class));
    }

    @SneakyThrows
    public void test_setFieldAndMaxDoc_withoutMerge_andNPostingValue() {
        when(mockFieldInfo.attributes()).thenReturn(prepareAttributes(true, 10, 1.0f, 160, 1.0f));

        clusteredPostingTermsWriter.setFieldAndMaxDoc(mockFieldInfo, 100, false);

        verify(mockFieldInfo).getIndexOptions();
        verify(mockDocValuesFormat, times(1)).fieldsProducer(any(SegmentReadState.class));
    }

    @SneakyThrows
    public void test_setFieldAndMaxDoc_withoutMerge_andFieldsProducerThrowException() {
        clusteredPostingTermsWriter = spy(this.clusteredPostingTermsWriter);

        // Mock the fields producer throw exception
        when(mockDocValuesFormat.fieldsProducer(any(SegmentReadState.class))).thenThrow(new IOException("mock_exception"));
        clusteredPostingTermsWriter.setFieldAndMaxDoc(mockFieldInfo, 100, false);

        verify(clusteredPostingTermsWriter).setField(mockFieldInfo);
        verify(mockDocValuesFormat, times(1)).fieldsProducer(any(SegmentReadState.class));
    }

    @SneakyThrows
    public void test_setFieldAndMaxDoc_withMerge() {
        clusteredPostingTermsWriter = spy(clusteredPostingTermsWriter);

        // Mock the superclass setField method
        doNothing().when(clusteredPostingTermsWriter).setField(any(FieldInfo.class));
        clusteredPostingTermsWriter.setFieldAndMaxDoc(mockFieldInfo, 100, true);

        verify(clusteredPostingTermsWriter).setField(mockFieldInfo);
        verify(mockDocValuesFormat, never()).fieldsProducer(any(SegmentReadState.class));
    }

    @SneakyThrows
    public void test_newTermState() {
        assertNotNull(clusteredPostingTermsWriter.newTermState());
    }

    @SneakyThrows
    public void test_finishTerm_writesPostingClusters() {
        // set up postingClusters containing a cluster with empty summary
        List<DocumentCluster> documentClusterList = prepareClusterList();
        List<DocWeight> docWeights = new ArrayList<>();
        docWeights.add(new DocWeight(3, (byte) 4));
        documentClusterList.add(new DocumentCluster(null, docWeights, false));
        PostingClusters postingClusters = new PostingClusters(documentClusterList);

        // set up for finish term
        clusteredPostingTermsWriter.setFieldAndMaxDoc(mockFieldInfo, 100, false);
        clusteredPostingTermsWriter.write(new BytesRef("test_term"), postingClusters);

        // reset mockIndexOutput because write function also write posting clusters
        reset(mockIndexOutput);

        BlockTermState state = clusteredPostingTermsWriter.newTermState();
        clusteredPostingTermsWriter.finishTerm(state);

        // Verify the output was written
        verify(mockIndexOutput, atLeastOnce()).writeVLong(anyLong());
    }

    @SneakyThrows
    public void test_startDoc_withValidDocId() {
        clusteredPostingTermsWriter.startDoc(1, 10);
        // No assertion needed, we're just verifying it doesn't throw an exception
    }

    public void test_startDoc_withInvalidDocID() {
        expectThrows(IllegalStateException.class, () -> { clusteredPostingTermsWriter.startDoc(-1, 10); });
    }

    public void test_addPosition_thenThrownUnsupportedOperation() {
        assertThrows(UnsupportedOperationException.class, () -> clusteredPostingTermsWriter.addPosition(0, new BytesRef(), 0, 0));
    }

    @SneakyThrows
    public void test_finishDoc() {
        clusteredPostingTermsWriter.finishDoc();
    }

    @SneakyThrows
    public void test_init() {
        verify(mockCodecUtilWrapper).writeIndexHeader(
            eq(mockIndexOutput),
            eq(CODEC_NAME),
            eq(VERSION),
            any(),
            eq(mockWriteState.segmentSuffix)
        );
    }

    public void test_encodeTerm_thenThrowUnsupportedOperationException() {
        assertThrows(UnsupportedOperationException.class, () -> clusteredPostingTermsWriter.encodeTerm(null, null, null, false));
    }

    @SneakyThrows
    public void test_close_whenDocValuesProducerNull() {
        clusteredPostingTermsWriter.close();

        verify(mockDocValuesProducer, never()).close();
    }

    public void test_close_whenDocValuesProducerNonNull() throws IOException {
        clusteredPostingTermsWriter.setFieldAndMaxDoc(mockFieldInfo, 100, false);
        clusteredPostingTermsWriter.close();

        verify(mockDocValuesProducer, times(1)).close();
        verify(mockCodecUtilWrapper).writeFooter(any());
    }

    @SneakyThrows
    public void test_closeWithException_whenDocValuesProducerNull() {
        clusteredPostingTermsWriter.closeWithException();

        // Verify that IOUtils.closeWhileHandlingException calls only postingOut
        verify(mockIndexOutput, times(1)).close();
        verify(mockDocValuesProducer, never()).close();
    }

    @SneakyThrows
    public void test_closeWithException_whenDocValuesProducerNonNull() {
        clusteredPostingTermsWriter.setFieldAndMaxDoc(mockFieldInfo, 100, false);
        clusteredPostingTermsWriter.closeWithException();

        // Verify that IOUtils.closeWhileHandlingException calls with both postingOut and docValuesProducer
        verify(mockIndexOutput, times(1)).close();
        verify(mockDocValuesProducer, times(1)).close();
    }

    @SneakyThrows
    public void test_close_withStartFp() {
        clusteredPostingTermsWriter = spy(clusteredPostingTermsWriter);
        clusteredPostingTermsWriter.close(100L);

        // Verify startFp was written
        verify(mockIndexOutput).writeLong(eq(100L));

        // Verify calls this.close()
        verify(clusteredPostingTermsWriter, times(1)).close();
    }
}
