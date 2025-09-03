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
import org.apache.lucene.index.SegmentReadState;
import org.apache.lucene.index.Terms;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.junit.After;
import org.junit.Before;
import org.opensearch.neuralsearch.sparse.AbstractSparseTestBase;
import org.opensearch.neuralsearch.sparse.cache.CacheKey;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.function.Supplier;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.opensearch.neuralsearch.sparse.common.SparseConstants.APPROXIMATE_THRESHOLD_FIELD;
import static org.opensearch.neuralsearch.sparse.mapper.SparseTokensField.SPARSE_FIELD;

public class SparsePostingsProducerTests extends AbstractSparseTestBase {

    private FieldsProducer mockDelegate;
    private SegmentReadState segmentReadState;
    private SparsePostingsProducer producer;
    private FieldInfo sparseFieldInfo;
    private SegmentInfo segmentInfo;
    private FieldInfos fieldInfos;
    private SparseTermsLuceneReader mockReader;
    private Supplier<SparseTermsLuceneReader> readerSupplier = () -> mockReader;

    @Before
    public void setUp() {
        super.setUp();
        mockDelegate = mock(FieldsProducer.class);

        // Setup segment info
        segmentInfo = mock(SegmentInfo.class);
        when(segmentInfo.maxDoc()).thenReturn(10);

        // Setup field infos using real FieldInfo objects
        sparseFieldInfo = mock(FieldInfo.class);
        when(sparseFieldInfo.getName()).thenReturn(SPARSE_FIELD);
        Map<String, String> sparseAttributes = new HashMap<>();
        sparseAttributes.put(SPARSE_FIELD, "true");
        sparseAttributes.put(APPROXIMATE_THRESHOLD_FIELD, "10");
        when(sparseFieldInfo.attributes()).thenReturn(sparseAttributes);

        // Setup field infos
        fieldInfos = mock(FieldInfos.class);
        when(fieldInfos.fieldInfo(sparseFieldInfo.getName())).thenReturn(sparseFieldInfo);

        // Setup segment read state
        Directory mockDir = mock(Directory.class);
        segmentReadState = new SegmentReadState(mockDir, segmentInfo, fieldInfos, IOContext.DEFAULT);
        mockReader = mock(SparseTermsLuceneReader.class);

        producer = new SparsePostingsProducer(mockDelegate, segmentReadState, readerSupplier);
    }

    @After
    @Override
    public void tearDown() throws Exception {
        if (producer != null) {
            producer.close();
        }
        super.tearDown();
    }

    @SneakyThrows
    public void testConstructor() {
        SparsePostingsProducer localProducer = new SparsePostingsProducer(mockDelegate, segmentReadState, readerSupplier);
        assertNotNull(localProducer);
        assertEquals(mockDelegate, localProducer.getDelegate());
        assertEquals(segmentReadState, localProducer.getState());
        expectThrows(NullPointerException.class, () -> new SparsePostingsProducer(mockDelegate, segmentReadState, null));
    }

    @SneakyThrows
    public void testClose_WithDelegate() {
        producer.close();

        verify(mockDelegate, times(1)).close();
    }

    @SneakyThrows
    public void testClose_WithNullDelegate() {
        SparsePostingsProducer producerWithNullDelegate = new SparsePostingsProducer(null, segmentReadState, readerSupplier);

        // Should not throw exception
        producerWithNullDelegate.close();
    }

    @SneakyThrows
    public void testClose_WithReader() {
        // First call terms() to initialize reader
        Terms mockTerms = mock(Terms.class);
        when(mockDelegate.terms(sparseFieldInfo.getName())).thenReturn(mockTerms);

        producer.terms(sparseFieldInfo.getName());
        assertNotNull(producer.getReader());

        producer.close();

        verify(mockDelegate, times(1)).close();
    }

    @SneakyThrows
    public void testCheckIntegrity() {
        producer.checkIntegrity();

        verify(mockDelegate, times(1)).checkIntegrity();
    }

    @SneakyThrows
    public void testIterator() {
        Iterator<String> mockIterator = Arrays.asList("field1", "field2").iterator();
        when(mockDelegate.iterator()).thenReturn(mockIterator);

        Iterator<String> result = producer.iterator();

        assertEquals(mockIterator, result);
        verify(mockDelegate, times(1)).iterator();
    }

    @SneakyThrows
    public void testTerms_SparseFieldBelowThreshold() {
        // Create segment info with low maxDoc
        SegmentInfo lowThresholdSegmentInfo = mock(SegmentInfo.class);
        when(lowThresholdSegmentInfo.maxDoc()).thenReturn(3);

        SegmentReadState lowThresholdState = new SegmentReadState(
            segmentReadState.directory,
            lowThresholdSegmentInfo,
            fieldInfos,
            segmentReadState.context
        );

        SparsePostingsProducer lowThresholdProducer = new SparsePostingsProducer(mockDelegate, lowThresholdState, readerSupplier);

        Terms mockTerms = mock(Terms.class);
        when(mockDelegate.terms(sparseFieldInfo.getName())).thenReturn(mockTerms);

        Terms result = lowThresholdProducer.terms(sparseFieldInfo.getName());

        assertEquals(mockTerms, result);
        verify(mockDelegate, times(1)).terms(sparseFieldInfo.getName());
        assertNull(lowThresholdProducer.getReader());
        lowThresholdProducer.close();
    }

    @SneakyThrows
    public void testTerms_SparseFieldAboveThreshold() {
        Terms result = producer.terms(sparseFieldInfo.getName());

        assertNotNull(result);
        assertTrue(result instanceof SparseTerms);
        assertNotNull(producer.getReader());

        SparseTerms sparseTerms = (SparseTerms) result;
        CacheKey expectedKey = new CacheKey(segmentInfo, sparseFieldInfo);
        assertEquals(expectedKey, sparseTerms.getCacheKey());
    }

    @SneakyThrows
    public void testTerms_nullSupplier() {
        producer = new SparsePostingsProducer(mockDelegate, segmentReadState, () -> null);
        expectThrows(NullPointerException.class, () -> producer.terms(sparseFieldInfo.getName()));
    }

    @SneakyThrows
    public void testTerms_NullFieldInfo() {
        when(fieldInfos.fieldInfo("unknown_field")).thenReturn(null);

        Terms mockTerms = mock(Terms.class);
        when(mockDelegate.terms("unknown_field")).thenReturn(mockTerms);

        Terms result = producer.terms("unknown_field");

        assertEquals(mockTerms, result);
        verify(mockDelegate, times(1)).terms("unknown_field");
    }

    @SneakyThrows
    public void testTerms_SparseFieldWithNullAttributes() {
        when(fieldInfos.fieldInfo("unknown_field")).thenReturn(null);

        Terms mockTerms = mock(Terms.class);
        when(mockDelegate.terms("unknown_field")).thenReturn(mockTerms);

        Terms result = producer.terms("unknown_field");

        assertEquals(mockTerms, result);
        verify(mockDelegate, times(1)).terms("unknown_field");
    }

    @SneakyThrows
    public void testSize() {
        int result = producer.size();
        assertEquals(0, result);
    }

    @SneakyThrows
    public void testGetters() {
        assertEquals(mockDelegate, producer.getDelegate());
        assertEquals(segmentReadState, producer.getState());
    }
}
