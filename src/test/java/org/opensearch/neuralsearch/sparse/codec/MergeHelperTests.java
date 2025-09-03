/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.sparse.codec;

import lombok.SneakyThrows;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.lucene.codecs.DocValuesProducer;
import org.apache.lucene.codecs.FieldsProducer;
import org.apache.lucene.index.BinaryDocValues;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.FieldInfos;
import org.apache.lucene.index.MergeState;
import org.apache.lucene.index.PostingsEnum;
import org.apache.lucene.index.SegmentInfo;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.util.BytesRef;
import org.junit.Before;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.opensearch.neuralsearch.sparse.AbstractSparseTestBase;
import org.opensearch.neuralsearch.sparse.cache.CacheGatedPostingsReader;
import org.opensearch.neuralsearch.sparse.cache.CacheKey;
import org.opensearch.neuralsearch.sparse.common.MergeStateFacade;
import org.opensearch.neuralsearch.sparse.data.DocWeight;
import org.opensearch.neuralsearch.sparse.data.PostingClusters;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Consumer;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.opensearch.neuralsearch.sparse.mapper.SparseTokensField.SPARSE_FIELD;

public class MergeHelperTests extends AbstractSparseTestBase {
    @Mock
    private MergeStateFacade mergeStateFacade;
    @Mock
    private FieldInfo sparseFieldInfo;
    @Mock
    private FieldInfo nonSparseFieldInfo;
    @Mock
    private SparseBinaryDocValuesPassThrough binaryDocValuePassThrough;
    @Mock
    private DocValuesProducer docValuesProducer;
    @Mock
    private SegmentInfo segmentInfo;
    @Mock
    private Terms mockTerms;
    @Mock
    private TermsEnum mockTermsEnum;
    @Mock
    private SparseTerms mockSparseTerms;
    @Mock
    private CacheGatedPostingsReader mockCacheGatedPostingsReader;
    @Mock
    private FieldsProducer mockFieldsProducer;
    @Mock
    private SparsePostingsEnum mockSparsePostingsEnum;
    @Mock
    private PostingsEnum mockPostingsEnum;
    @Mock
    private PostingClusters mockPostingClusters;
    @Mock
    private MergeHelper mergeHelper;
    @Mock
    private MergeState.DocMap mockDocMap;
    @Mock
    private FieldInfo mockFieldInfo;
    private static final BytesRef term = new BytesRef("term");
    private static final Set<BytesRef> terms = Set.of(term);

    @SneakyThrows
    @Before
    @Override
    public void setUp() {
        super.setUp();
        MockitoAnnotations.openMocks(this);

        mergeHelper = new MergeHelper();

        when(mergeStateFacade.getFieldsProducers()).thenReturn(new FieldsProducer[] { mockFieldsProducer });
        when(mergeStateFacade.getDocValuesProducers()).thenReturn(new DocValuesProducer[] { docValuesProducer });
        when(docValuesProducer.getBinary(any())).thenReturn(binaryDocValuePassThrough);

        when(mockFieldsProducer.terms(anyString())).thenReturn(mockTerms);
        when(mockTerms.iterator()).thenReturn(mockTermsEnum);
        when(mockTermsEnum.seekExact(any())).thenReturn(true);
        when(mockTermsEnum.postings(isNull())).thenReturn(mockSparsePostingsEnum);
        when(mockSparsePostingsEnum.nextDoc()).thenReturn(1).thenReturn(2).thenReturn(PostingsEnum.NO_MORE_DOCS);
        when(mockSparsePostingsEnum.freq()).thenReturn(1).thenReturn(2);
        when(mergeStateFacade.getDocMaps()).thenReturn(new MergeState.DocMap[] { mockDocMap });
        when(mockDocMap.get(eq(1))).thenReturn(1);
        when(mockDocMap.get(eq(2))).thenReturn(2);
        when(mockFieldInfo.getName()).thenReturn("field_name");

        // Setup sparse field
        Map<String, String> sparseAttributes = new HashMap<>();
        sparseAttributes.put(SPARSE_FIELD, "true");
        when(sparseFieldInfo.attributes()).thenReturn(sparseAttributes);

        // Setup non-sparse field
        when(nonSparseFieldInfo.attributes()).thenReturn(new HashMap<>());

        List<FieldInfo> fields = Arrays.asList(sparseFieldInfo, nonSparseFieldInfo);
        FieldInfos fieldInfos = mock(FieldInfos.class);
        when(fieldInfos.iterator()).thenReturn(fields.iterator());
        when(mergeStateFacade.getMergeFieldInfos()).thenReturn(fieldInfos);

        // configure sparse term
        when(mockSparseTerms.getReader()).thenReturn(mockCacheGatedPostingsReader);
        when(mockCacheGatedPostingsReader.read(any(BytesRef.class))).thenReturn(mockPostingClusters);
        when(mockCacheGatedPostingsReader.getTerms()).thenReturn(terms);
    }

    public void testClearCacheData_withValidSparseField_callsConsumer() throws IOException {
        SparseBinaryDocValuesPassThrough mockBinaryDocValues = mock(SparseBinaryDocValuesPassThrough.class);
        when(mockBinaryDocValues.getSegmentInfo()).thenReturn(segmentInfo);
        when(docValuesProducer.getBinary(sparseFieldInfo)).thenReturn(mockBinaryDocValues);

        List<CacheKey> capturedKeys = new ArrayList<>();
        Consumer<CacheKey> consumer = capturedKeys::add;

        mergeHelper.clearCacheData(mergeStateFacade, sparseFieldInfo, consumer);

        assertEquals("Consumer should be called when fieldInfo matches", 1, capturedKeys.size());
    }

    public void testClearInMemoryData_withNullFieldInfo_processesAllSparseFields() throws IOException {
        SparseBinaryDocValuesPassThrough mockBinaryDocValues = mock(SparseBinaryDocValuesPassThrough.class);
        when(mockBinaryDocValues.getSegmentInfo()).thenReturn(segmentInfo);
        when(docValuesProducer.getBinary(sparseFieldInfo)).thenReturn(mockBinaryDocValues);

        List<CacheKey> capturedKeys = new ArrayList<>();
        Consumer<CacheKey> consumer = capturedKeys::add;

        mergeHelper.clearCacheData(mergeStateFacade, null, consumer);

        assertEquals("Consumer should be called for sparse field", 1, capturedKeys.size());
    }

    public void testClearInMemoryData_withNonSparseFieldInfo_processesAllSparseFields() throws IOException {
        SparseBinaryDocValuesPassThrough mockBinaryDocValues = mock(SparseBinaryDocValuesPassThrough.class);
        when(mockBinaryDocValues.getSegmentInfo()).thenReturn(segmentInfo);
        when(docValuesProducer.getBinary(sparseFieldInfo)).thenReturn(mockBinaryDocValues);

        List<CacheKey> capturedKeys = new ArrayList<>();
        Consumer<CacheKey> consumer = capturedKeys::add;

        mergeHelper.clearCacheData(mergeStateFacade, nonSparseFieldInfo, consumer);

        assertEquals("Consumer should NOT be called for sparse field when fieldInfo doesn't match", 0, capturedKeys.size());
    }

    public void testClearInMemoryData_withNonSparseBinaryDocValues_skipsField() throws IOException {
        BinaryDocValues mockBinaryDocValues = mock(BinaryDocValues.class);
        when(docValuesProducer.getBinary(sparseFieldInfo)).thenReturn(mockBinaryDocValues);

        List<CacheKey> capturedKeys = new ArrayList<>();
        Consumer<CacheKey> consumer = capturedKeys::add;

        mergeHelper.clearCacheData(mergeStateFacade, nonSparseFieldInfo, consumer);

        assertEquals("Consumer should NOT be called for non-SparseBinaryDocValuesPassThrough", 0, capturedKeys.size());
    }

    public void testClearInMemoryData_withEmptyMergeState_doesNotCallConsumer() throws IOException {
        when(mergeStateFacade.getDocValuesProducers()).thenReturn(new DocValuesProducer[]{});

        List<CacheKey> capturedKeys = new ArrayList<>();
        Consumer<CacheKey> consumer = capturedKeys::add;

        mergeHelper.clearCacheData(mergeStateFacade, sparseFieldInfo, consumer);

        assertEquals("Consumer should NOT be called with empty producers", 0, capturedKeys.size());
    }

    public void test_getMergedPostingForATerm_unexpectedType() throws IOException {
        BinaryDocValues binaryDocValues = mock(BinaryDocValues.class);
        when(docValuesProducer.getBinary(any())).thenReturn(binaryDocValues);

        List<DocWeight> result = mergeHelper.getMergedPostingForATerm(mergeStateFacade, term, mockFieldInfo, new int[2], new int[2]);
        verify(docValuesProducer).getBinary(any());
        assertTrue(CollectionUtils.isEmpty(result));
    }

    public void test_getMergedPostingForATerm_nullTerm() throws IOException {
        when(mockFieldsProducer.terms(anyString())).thenReturn(null);
        List<DocWeight> result = mergeHelper.getMergedPostingForATerm(mergeStateFacade, term, mockFieldInfo, new int[2], new int[2]);
        verify(mockFieldsProducer).terms(anyString());
        assertTrue(CollectionUtils.isEmpty(result));
    }

    public void test_getMergedPostingForATerm_nullTermsEnum() throws IOException {
        when(mockTerms.iterator()).thenReturn(null);
        List<DocWeight> result = mergeHelper.getMergedPostingForATerm(mergeStateFacade, term, mockFieldInfo, new int[2], new int[2]);
        verify(mockTerms).iterator();
        assertTrue(CollectionUtils.isEmpty(result));
    }

    public void test_getMergedPostingForATerm_seekExactFalse() throws IOException {
        when(mockTermsEnum.seekExact(any())).thenReturn(false);
        List<DocWeight> result = mergeHelper.getMergedPostingForATerm(mergeStateFacade, term, mockFieldInfo, new int[2], new int[2]);
        verify(mockTermsEnum).seekExact(any());
        assertTrue(CollectionUtils.isEmpty(result));
    }

    public void test_getMergedPostingForATerm_postingIsNull() throws IOException {
        when(mockTermsEnum.postings(isNull())).thenReturn(null);
        List<DocWeight> result = mergeHelper.getMergedPostingForATerm(mergeStateFacade, term, mockFieldInfo, new int[2], new int[2]);
        verify(mockTermsEnum).postings(isNull());
        assertTrue(CollectionUtils.isEmpty(result));
    }

    public void test_getMergedPostingForATerm_postingNextDocNoMoreDocs() throws IOException {
        when(mockSparsePostingsEnum.nextDoc()).thenReturn(PostingsEnum.NO_MORE_DOCS);
        List<DocWeight> result = mergeHelper.getMergedPostingForATerm(mergeStateFacade, term, mockFieldInfo, new int[2], new int[2]);
        assertTrue(CollectionUtils.isEmpty(result));
    }

    public void test_getMergedPostingForATerm_postingNextDocIsMinus1() throws IOException {
        when(mockSparsePostingsEnum.nextDoc()).thenReturn(-1).thenReturn(PostingsEnum.NO_MORE_DOCS);
        List<DocWeight> result = mergeHelper.getMergedPostingForATerm(mergeStateFacade, term, mockFieldInfo, new int[2], new int[2]);
        assertTrue(CollectionUtils.isEmpty(result));
    }

    public void test_getMergedPostingForATerm_postingNewDocIsMinus1() throws IOException {
        when(mockSparsePostingsEnum.nextDoc()).thenReturn(1).thenReturn(PostingsEnum.NO_MORE_DOCS);
        when(mockDocMap.get(eq(1))).thenReturn(-1);
        List<DocWeight> result = mergeHelper.getMergedPostingForATerm(mergeStateFacade, term, mockFieldInfo, new int[2], new int[2]);
        assertTrue(CollectionUtils.isEmpty(result));
    }

    public void test_getMergedPostingForATerm_postingNewDocOutOfBound() throws IOException {
        when(mockSparsePostingsEnum.nextDoc()).thenReturn(1).thenReturn(PostingsEnum.NO_MORE_DOCS);
        when(mockDocMap.get(eq(1))).thenReturn(10000);
        expectThrows(IndexOutOfBoundsException.class, () -> mergeHelper.getMergedPostingForATerm(mergeStateFacade, term, mockFieldInfo, new int[2], new int[2]));
    }

    public void test_getMergedPostingForATerm_happyCase_expectedType() throws IOException {
        List<DocWeight> result = mergeHelper.getMergedPostingForATerm(mergeStateFacade, term, mockFieldInfo, new int[3], new int[3]);
        assertEquals(2, result.size());
        assertEquals(1, result.get(0).getDocID());
        assertEquals(1, result.get(0).getIntWeight());
        assertEquals(2, result.get(1).getDocID());
        assertEquals(2, result.get(1).getIntWeight());
    }

    public void test_getMergedPostingForATerm_happyCase_unexpectedType() throws IOException {
        when(mockPostingsEnum.nextDoc()).thenReturn(1).thenReturn(2).thenReturn(PostingsEnum.NO_MORE_DOCS);
        when(mockPostingsEnum.freq()).thenReturn(32512).thenReturn(32768);
        when(mockTermsEnum.postings(isNull())).thenReturn(mockPostingsEnum);
        List<DocWeight> result = mergeHelper.getMergedPostingForATerm(mergeStateFacade, term, mockFieldInfo, new int[3], new int[3]);
        assertEquals(2, result.size());
        assertEquals(1, result.get(0).getDocID());
        assertEquals(85, result.get(0).getIntWeight());
        assertEquals(2, result.get(1).getDocID());
        assertEquals(170, result.get(1).getIntWeight());
    }

    public void test_getAllTerms_emptyFieldProducer() throws IOException {
        when(mergeStateFacade.getFieldsProducers()).thenReturn(new FieldsProducer[0]);
        assertTrue(CollectionUtils.isEmpty(mergeHelper.getAllTerms(mergeStateFacade, mockFieldInfo)));
    }

    public void test_getAllTerms_unexpectedBinaryDocValueType() throws IOException {
        BinaryDocValues binaryDocValues = mock(BinaryDocValues.class);
        when(docValuesProducer.getBinary(any())).thenReturn(binaryDocValues);
        Set<BytesRef> allTerms = mergeHelper.getAllTerms(mergeStateFacade, mockFieldInfo);
        assertTrue(CollectionUtils.isEmpty(allTerms));
        verify(docValuesProducer).getBinary(any());
    }

    public void test_getAllTerms_isNotSparseTerm() throws IOException {
        when(mockTermsEnum.next()).thenReturn(term).thenReturn(null);
        Set<BytesRef> allTerms = mergeHelper.getAllTerms(mergeStateFacade, mockFieldInfo);
        assertEquals(terms, allTerms);
    }

    public void test_getAllTerms_isSparseTerm() throws IOException {
        SparseTerms sparseTerms = mock(SparseTerms.class);
        when(mockFieldsProducer.terms(anyString())).thenReturn(sparseTerms);
        when(sparseTerms.iterator()).thenReturn(mockTermsEnum);
        when(sparseTerms.getReader()).thenReturn(mockCacheGatedPostingsReader);
        when(mockCacheGatedPostingsReader.getTerms()).thenReturn(terms);
        Set<BytesRef> allTerms = mergeHelper.getAllTerms(mergeStateFacade, mockFieldInfo);
        assertEquals(terms, allTerms);
    }

    public void test_convertToMergeStateFacade() {
        MergeState mergeState = mock(MergeState.class);
        assertNotNull(mergeHelper.convertToMergeStateFacade(mergeState));
    }
}
